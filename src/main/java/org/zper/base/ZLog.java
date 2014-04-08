/*  =========================================================================
    ZLog - ZeroMQ message writer 

    -------------------------------------------------------------------------
    Copyright (c) 2012 InfiniLoop Corporation
    Copyright other contributors as noted in the AUTHORS file.

    This file is part of ZPER, the ZeroMQ Persistence Broker:
    
    This is free software; you can redistribute it and/or modify it under
    the terms of the GNU Lesser General Public License as published by
    the Free Software Foundation; either version 3 of the License, or (at
    your option) any later version.
        
    This software is distributed in the hope that it will be useful, but
    WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
    Lesser General Public License for more details.
        
    You should have received a copy of the GNU Lesser General Public
    License along with this program. If not, see
    <http://www.gnu.org/licenses/>.
    =========================================================================
*/
package org.zper.base;

import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;

import zmq.Msg;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zper.MsgIterator;


public class ZLog
{

    static final Logger LOG = LoggerFactory.getLogger(ZLog.class);

    private final static String SUFFIX = ".dat";

    private final String topic;

    private final ZLogManager.ZLogConfig conf;
    private File path;
    private long start;
    private long pendingMessages;
    private long capacity;
    private long lastFlush;
    private boolean flushed;

    private final TreeMap<Long, Segment> segments;
    private Segment current;
    private static final Pattern pattern = Pattern.compile("\\d{20}\\" + SUFFIX);

    public static final long LATEST = -1L;
    public static final long EARLIEST = -2L;

    public ZLog(ZLogManager.ZLogConfig conf, String topic)
    {

        this.topic = topic;
        this.conf = conf;

        segments = new TreeMap<Long, Segment>();
        reset();
        if (conf.recover)
            recover();
        flushed = false;
    }

    protected void reset()
    {
        close();

        start = 0L;
        pendingMessages = 0L;
        lastFlush = System.currentTimeMillis();

        path = new File(conf.base_path, topic);

        if (!path.exists())
            if (!path.mkdirs()) {
                throw new RuntimeException("Cannot make directory " + path.getAbsolutePath());
            }

        File[] files = path.listFiles(
                new FilenameFilter()
                {
                    @Override
                    public boolean accept(File dir, String name)
                    {
                        return pattern.matcher(name).matches();
                    }
                });
        Arrays.sort(files,
                new Comparator<File>()
                {
                    @Override
                    public int compare(File arg0, File arg1)
                    {
                        return arg0.compareTo(arg1);
                    }
                });
        segments.clear();
        for (File f : files) {
            long offset = Long.valueOf(f.getName().replace(SUFFIX, ""));
            segments.put(offset, new Segment(this, offset));
        }

        if (!segments.isEmpty()) {
            start = segments.firstKey();
            current = segments.lastEntry().getValue();
        } else {
            current = new Segment(this, 0L);
            segments.put(0L, current);
        }

        capacity = conf.segment_size - current.size();
    }

    public File path()
    {
        return path;
    }

    public long segmentSize()
    {
        return conf.segment_size;
    }

    public int count()
    {
        return segments.size();
    }

    public long start()
    {
        return start;
    }

    public long offset()
    {
        return current == null ? 0L : current.offset();
    }

    /**
     * last element is always safely flushed offset
     *
     * @return array of segment start offsets
     */
    public long[] offsets()
    {
        long[] offsets = new long[segments.size() + 1];
        int i = 0;
        while (true) {
            // fail first instead of using a lock
            try {
                for (Long key : segments.keySet()) {
                    offsets[i] = key;
                    i++;
                }
                break;
            } catch (ConcurrentModificationException e) {
                // retry
                offsets = new long[segments.size() + 1];
            }
        }
        offsets[i] = current == null ? 0L : current.size();
        return offsets;
    }

    /**
     * For -1, it always returns start and last safely flushed offset
     *
     * @param modifiedBefore timestamp in millis. -2 for first, -1 for latest
     * @param maxEntry       maximum entries
     * @return array of segment start offsets which where modified since
     */
    public long[] offsets(long modifiedBefore, int maxEntry)
    {

        if (segments.isEmpty())
            return new long[0];

        if (modifiedBefore == EARLIEST) // first
            return new long[] {segments.firstKey()};
        if (modifiedBefore == LATEST) {
            Map.Entry<Long, Segment> last = segments.lastEntry();
            return new long[] {last.getKey(), last.getKey() + last.getValue().size()};
        }

        Segment[] values;

        while (true) {
            // fail first instead of using a lock
            try {
                values = segments.values().toArray(new Segment[0]);
                break;
            } catch (ConcurrentModificationException e) {
                //
            }
        }
        int idx = values.length / 2;
        int top = values.length;
        int bottom = -1;
        while (idx > bottom && idx < top) {
            Segment v = values[idx];
            long lastMod = v.lastModified();

            if (lastMod < modifiedBefore) {
                bottom = idx;
            } else if (lastMod > modifiedBefore) {
                top = idx;
            } else {
                break;
            }
            idx = (top + bottom) / 2;
        }
        if (bottom == -1) { // no matches
            return new long[0];
        }
        int start = 0;
        if (maxEntry > 0 && maxEntry < (idx + 1)) {
            start = idx - maxEntry + 1;
        }
        long[] offsets = new long[idx - start + 1 + (top == values.length ? 1 : 0)];
        for (int i = start; i <= idx; i++) {
            offsets[i] = values[i].start();
            i++;
        }
        if (top == values.length)
            offsets[offsets.length - 1] = current.flushedOffset();
        return offsets;
    }

    /**
     * This operation is not thread-safe.
     * This should be called by a single thread or must be synchronized by caller
     *
     * @param msg Msg instance including one or more Msg instances
     * @return last absolute position
     * @throws IOException
     * @count message count
     */
    public long appendBulk(int count, Msg msg) throws IOException
    {

        long size = msg.size();

        capacity -= size;
        if (capacity < 0) {
            rotate();
            capacity -= size;
        }

        pendingMessages = pendingMessages + count;
        current.write(msg.buf());

        tryFlush();
        return current.offset();

    }

    /**
     * This operation is not thread-safe.
     * This should be called by a single thread or must be synchronized by caller
     *
     * @param msg
     * @return last absolute position
     * @throws IOException
     */
    public long append(Msg msg) throws IOException
    {
        ByteBuffer header = getMsgHeader(msg);
        long size = msg.size() + header.capacity();

        capacity -= size;
        if (capacity < 0 && !msg.hasMore()) {
            rotate();
            capacity -= size;
        }

        pendingMessages++;

        current.writes(header, msg.buf());

        if (!msg.hasMore()) {
            tryFlush();
        }
        return current.offset();
    }

    private ByteBuffer getMsgHeader(Msg msg)
    {
        int size = msg.size();
        int flags = msg.flags();
        ByteBuffer header;
        if (size < 255) {
            header = ByteBuffer.allocate(2);
            header.put((byte) ((flags & Msg.MORE) > 0 ? 0x01 : 0x00));
            header.put((byte) size);
        } else {
            header = ByteBuffer.allocate(9);

            header.put((byte) ((flags & Msg.MORE) > 0 ? 0x03 : 0x02));
            header.putLong((long) size);
        }
        header.flip();
        return header;
    }

    private void rotate()
    {
        current.close();
        capacity = conf.segment_size;
        long offset = current.offset();
        current = new Segment(this, offset);
        segments.put(offset, current);
        cleanup();
    }

    public List<Msg> readMsg(long start, long max)
            throws InvalidOffsetException, IOException
    {

        Map.Entry<Long, Segment> entry = segments.floorEntry(start);
        List<Msg> results = new ArrayList<Msg>();
        MappedByteBuffer buf;
        Msg msg;

        if (entry == null) {
            return results;
        }
        buf = entry.getValue().getBuffer(false);
        buf.position((int) (start - entry.getKey()));

        MsgIterator it = new MsgIterator(buf, conf.allow_empty_message);

        while (it.hasNext()) {
            msg = it.next();
            if (msg == null)
                break;
            max = max - msg.size();
            if (max <= 0)
                break;
            results.add(msg);
        }

        return results;
    }

    public int countMsg(long start, long max)
            throws InvalidOffsetException, IOException
    {

        int count = 0;
        Map.Entry<Long, Segment> entry = segments.floorEntry(start);
        MappedByteBuffer buf;
        Msg msg;

        if (entry == null) {
            return count;
        }
        buf = entry.getValue().getBuffer(false);
        buf.position((int) (start - entry.getKey()));

        MsgIterator it = new MsgIterator(buf, conf.allow_empty_message);

        while (it.hasNext()) {
            msg = it.next();
            if (msg == null)
                break;
            max = max - msg.size();
            if (max <= 0)
                break;
            count++;
        }

        return count;
    }

    public int read(long start, ByteBuffer dst) throws IOException
    {
        Map.Entry<Long, Segment> entry = segments.floorEntry(start);
        FileChannel ch;
        ch = entry.getValue().getChannel(false);
        ch.position(start - entry.getKey());
        return ch.read(dst);
    }

    /**
     * By using memory mapped file, returned file channel might not be fully filled.
     *
     * @param start absolute file offset
     * @return FileChannel
     * @throws IOException
     */
    public FileChannel open(long start) throws IOException
    {
        Map.Entry<Long, Segment> entry = segments.floorEntry(start);
        FileChannel ch;
        ch = entry.getValue().getChannel(false);
        ch.position(start - entry.getKey());

        return ch;
    }

    /**
     * @param offset absolute file offset
     * @return SegmentInfo
     */
    public SegmentInfo segmentInfo(long offset)
    {
        Map.Entry<Long, Segment> entry = segments.floorEntry(offset);
        if (entry == null)
            return null;
        return new SegmentInfo(entry.getKey(), entry.getValue());
    }

    /**
     * @return An array of SegmentInfo
     */
    public SegmentInfo[] segments()
    {
        SegmentInfo[] infos = new SegmentInfo[segments.size()];

        int c = 0;
        for (Map.Entry<Long, Segment> entry : segments.entrySet()) {
            infos[c++] = new SegmentInfo(entry.getKey(), entry.getValue());
        }

        return infos;
    }

    /**
     * This operation is not thread-safe.
     * This should be called by a single thread or must be synchronized by caller
     */
    public void flush()
    {
        current.flush();
        pendingMessages = 0;
        lastFlush = System.currentTimeMillis();
    }

    private void tryFlush()
    {
        flushed = false;
        if (pendingMessages >= conf.flush_messages) {
            flushed = true;
        }
        if (!flushed && System.currentTimeMillis() - lastFlush >= conf.flush_interval) {
            flushed = true;
        }

        if (flushed)
            flush();
    }


    public boolean flushed()
    {
        return flushed;
    }

    private void recover()
    {
        try {
            current.recover(conf.allow_empty_message);
            capacity = conf.segment_size - current.size();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void cleanup()
    {
        long expire = System.currentTimeMillis() - 3600L * conf.retain_hours;

        Iterator<Map.Entry<Long, Segment>> it = segments.entrySet().iterator();
        while (it.hasNext()) {
            Segment seg = it.next().getValue();
            if (seg.lastModified() < expire && seg != current) {
                seg.delete();
                it.remove();
            } else {
                break;
            }
        }
    }

    /**
     * This operation is not thread-safe.
     * This should be called by a single thread or must be synchronized by caller
     */
    public void close()
    {
        if (current == null)
            return;
        current.close();
        current = null;
    }

    @Override
    public String toString()
    {
        if (current == null) {
            return super.toString() + "[" + topic + "]";
        } else {
            return super.toString() + "[" + topic + "," + current.toString() + "]";
        }
    }

    public static class SegmentInfo
    {

        private long start;
        private Segment segment;

        protected SegmentInfo(long start, Segment segment)
        {
            this.start = start;
            this.segment = segment;
        }

        public String path()
        {
            return segment.path.getAbsolutePath();
        }

        public long start()
        {
            return start;
        }

        public long offset()
        {
            return segment.offset();
        }

        public long flushedOffset()
        {
            return segment.flushedOffset();
        }

    }

    private static class Segment
    {

        private final ZLog zlog;
        private long size;
        private long flushed_size;
        private long start;
        private FileChannel channel;
        private MappedByteBuffer buffer;
        private final File path;

        protected Segment(ZLog zlog, long offset)
        {

            this.zlog = zlog;
            this.start = offset;
            this.size = 0;
            this.flushed_size = 0;
            this.path = new File(zlog.path(), getName(offset));
            if (path.exists())
                flushed_size = size = path.length();
        }


        private static String getName(long offset)
        {
            NumberFormat nf = NumberFormat.getInstance();
            nf.setMinimumIntegerDigits(20);
            nf.setMaximumFractionDigits(0);
            nf.setGroupingUsed(false);
            return nf.format(offset) + SUFFIX;
        }


        @SuppressWarnings("resource")
        protected FileChannel getChannel(boolean writable) throws IOException
        {
            if (writable) {
                if (channel == null) {
                    channel = new RandomAccessFile(path, "rw").getChannel();
                    channel.position(channel.size());
                }
                return channel;
            } else {
                return new FileInputStream(path).getChannel();
            }
        }

        protected MappedByteBuffer getBuffer(boolean writable) throws IOException
        {
            if (writable && buffer != null)
                return buffer;

            FileChannel ch = getChannel(writable);

            if (writable) {
                buffer = ch.map(MapMode.READ_WRITE, 0, zlog.segmentSize());
                buffer.position((int) size);
                return buffer;
            } else {
                MappedByteBuffer rbuf = ch.map(MapMode.READ_ONLY, 0, ch.size());
                ch.close();
                return rbuf;
            }

        }

        protected long write(ByteBuffer buffer) throws IOException
        {
            long written = getChannel(true).write(buffer);
            size += written;
            return written;
        }

        protected long writes(ByteBuffer... buffers) throws IOException
        {
            long written = getChannel(true).write(buffers);
            size += written;
            return written;
        }

        protected final long offset()
        {
            return start + size;
        }

        protected final long size()
        {
            return size;
        }

        protected final long flushedOffset()
        {
            return start + flushed_size;
        }

        protected final long start()
        {
            return start;
        }

        protected void flush()
        {
            if (channel != null) {
                try {
                    channel.force(false);
                    flushed_size = size;
                    if (LOG.isDebugEnabled())
                        LOG.debug("Channel {} Flush {}", path, size);
                } catch (IOException e) {
                    LOG.error("Flush Error", e);
                }
            } else if (buffer != null) {
                buffer.force();
                size = buffer.position();
                flushed_size = size;
            }
        }

        @SuppressWarnings("resource")
        protected void recover(boolean allowEmpty) throws IOException
        {
            FileChannel ch = new RandomAccessFile(path, "rw").getChannel();
            FileLock lock = null;

            while (true) {
                try {
                    lock = ch.lock();
                    break;
                } catch (OverlappingFileLockException e) {
                }
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    break;
                }
            }
            try {
                MappedByteBuffer buf = ch.map(MapMode.READ_ONLY, 0, ch.size());
                int pos = 0;
                MsgIterator it = new MsgIterator(buf, allowEmpty);
                while (it.hasNext()) {
                    Msg msg = it.next();
                    if (msg == null)
                        break;
                    pos = buf.position();
                }

                if (pos < ch.size()) {
                    ch.truncate(pos);
                    flushed_size = size = pos;
                    LOG.info("Segment " + path + " is Truncated at " + pos);
                }
            } finally {
                lock.release();
                ch.close();
            }

            LOG.info("Segment: " + path + " size: " + size);
        }

        protected void close()
        {
            if (channel == null)
                return;

            LOG.info("Closing Segment {}({})", path, size);
            flush();

            try {
                channel.truncate(size);
                channel.close();
            } catch (IOException e) {
            }
            LOG.info("Closed Segment {}({})", path, size);

            channel = null;
            buffer = null;
        }

        protected long lastModified()
        {
            return path.lastModified();
        }

        protected void delete()
        {
            path.delete();
        }

        @Override
        public String toString()
        {
            return path.getAbsolutePath() + "(" + offset() + ")";
        }
    }

    public static class InvalidOffsetException extends Exception
    {

        private static final long serialVersionUID = -1696298215013570232L;

        public InvalidOffsetException(Throwable e)
        {
            super(e);
        }

        public InvalidOffsetException()
        {
            super();
        }

    }

}
