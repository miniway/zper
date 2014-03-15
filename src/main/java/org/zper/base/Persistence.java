/*  =========================================================================
    Persistence - ZPER Persistence Encoder / Decoder 

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

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zmq.DecoderBase;
import zmq.EncoderBase;
import zmq.IMsgSink;
import zmq.IMsgSource;
import zmq.Msg;
import zmq.V1Protocol;
import zmq.ZError;

public class Persistence
{
    static final Logger LOG = LoggerFactory.getLogger(Persistence.class);

    public static final int MESSAGE_RESPONSE = 1;
    public static final int MESSAGE_FILE = 2;
    public static final int MESSAGE_ERROR = 3;

    public static final int STATUS_INTERNAL_ERROR = -1;

    public static class BufferLike
    {
        private final ByteBuffer base;

        private long position;
        private long limit;
        private long cursor;

        private final int size;

        public BufferLike(ByteBuffer base)
        {
            this.base = base;

            size = base.capacity();
            cursor = -size;
            limit = 0;
        }

        public void written(int size)
        {
            limit += size;
            cursor += size;
        }

        public long cursor()
        {
            return cursor;
        }

        public long remaining()
        {
            return limit - position;
        }

        public boolean hasRemaining()
        {
            return limit > position;
        }

        public int get()
        {
            int pos = (int) (position % size);
            ByteBuffer buffer = base.duplicate();
            buffer.limit(pos + 1);
            buffer.position(pos);

            position++;
            return buffer.get();
        }

        public long getLong()
        {
            int pos = (int) (position % size);
            ByteBuffer buffer = base.duplicate();
            position += 8;

            if (pos + 8 > size) {
                buffer.limit(size);
                buffer.position(pos);

                ByteBuffer result = ByteBuffer.allocate(8);
                result.put(buffer);

                buffer.limit(8 - result.position());
                buffer.position(0);
                result.put(buffer);
                result.flip();
                return result.getLong();
            }

            buffer.position(pos + 8);
            buffer.position(pos);

            return buffer.getLong();
        }

        public void get(byte[] data)
        {
            int pos = (int) (position % size);
            ByteBuffer buffer = base.duplicate();
            buffer.position(pos);

            position += data.length;

            try {
                buffer.get(data);
            } catch (BufferUnderflowException e)
            {
                int remaining = buffer.remaining();
                buffer.get(data, 0, remaining);
                buffer.get(data, remaining, data.length - remaining);
            }
        }

        public long position()
        {
            return position;
        }

        public void advance(int offset)
        {
            position += offset;
        }
    }

    public static class PersistDecoder extends DecoderBase
    {
        private static final int one_byte_size_ready = 0;
        private static final int eight_byte_size_ready = 1;
        private static final int flags_ready = 2;
        private static final int message_ready = 3;

        private IMsgSink msg_sink;
        private final long maxmsgsize;
        private int msg_flags;
        private int msg_size;
        private final int version;
        private BufferLike buffer;              //  Current operating buffer
        private ByteBuffer active;              //  Active buffer
        private ByteBuffer dummy;               //  Dummy buffer
        private boolean identity_received;
        private final int bufsize;
        private long start;
        private long end;
        private int count;
        private long total;

        private final AtomicLong threshold;

        public PersistDecoder(int bufsize, long maxmsgsize, IMsgSink session, int version_)
        {
            super(0);

            version = version_;
            msg_sink = session;
            identity_received = false;

            if (maxmsgsize < 0)
                this.bufsize = bufsize << 6;
            else
                this.bufsize = (int) maxmsgsize << 3;

            this.maxmsgsize = maxmsgsize;

            active = ByteBuffer.allocateDirect(this.bufsize);
            dummy = ByteBuffer.allocate(0);
            buffer = new BufferLike(active);
            start = end = count = 0;
            threshold = new AtomicLong(0);
            next_step(null, 1, flags_ready);
        }

        @Override
        public void set_msg_sink(IMsgSink msg_sink)
        {
            this.msg_sink = msg_sink;
        }

        @Override
        protected boolean next()
        {
            if (version == V1Protocol.VERSION) {
                switch (state()) {
                case one_byte_size_ready:
                    return one_byte_size_ready();
                case eight_byte_size_ready:
                    return eight_byte_size_ready();
                case flags_ready:
                    return flags_ready();
                case message_ready:
                    return message_ready();
                default:
                    return false;
                }
            }
            return false;
        }

        /**
         * This is called when there's no more data to process at the buffer
         *
         * @return ByteBuffer, socket read can fill the returned buffer at maximum to its limit
         */
        @Override
        public ByteBuffer get_buffer()
        {
            if (!active.hasRemaining()) {

                long limit = threshold.get();
                if (limit == buffer.cursor()) {
                    LockSupport.parkNanos(100);
                    return dummy; // busy waiting
                }
                assert limit > buffer.cursor();

                if (active.capacity() == active.position()) {
                    active.position(0);
                }
                if (active.position() < (limit % bufsize))
                    active.limit((int) (limit % bufsize));

                else
                    active.limit(active.capacity());
            }
            return active.duplicate();
        }

        /**
         * @param buf buffer that returned from get_buffer and flipped
         * @param size_ bytes that are received from socket
         * @return processed bytes. buf must advance as many as the returned value
         */
        @Override
        public int process_buffer(ByteBuffer buf, int size_)
        {
            //if (!buf.hasRemaining()) // busy wait
            //    return 0;

            buffer.written(size_);

            //  Check if we had an error in previous attempt.
            if (state() < 0)
                return -1;

            while (true) {

                //  Try to get more space in the message to fill in.
                //  If none is available, return.
                while (to_read == 0) {
                    if (!next()) {
                        if (state() < 0) {
                            return -1;
                        }

                        return buf.position();
                    }
                }

                //  If there are no more data in the buffer, return.
                if (to_read > buffer.remaining()) {

                    if (push_msg() != 0) { // error
                        return -1;
                    }

                    active.position(active.position() + size_);

                    return size_;
                }

                to_read = 0;
            }
        }

        private boolean one_byte_size_ready()
        {

            msg_size = buffer.get();
            if (msg_size < 0) {
                msg_size = (0xff) & msg_size;
            }

            //  Message size must not exceed the maximum allowed size.
            if (maxmsgsize >= 0)
                if (msg_size > maxmsgsize) {
                    LOG.error("Message size too large " + msg_size);
                    decoding_error();
                    return false;
                }

            next_step(null, msg_size, message_ready);

            return true;
        }

        private boolean eight_byte_size_ready()
        {
            //  The payload size is encoded as 64-bit unsigned integer.
            //  The most significant byte comes first.
            final long size = buffer.getLong();

            //  Message size must not exceed the maximum allowed size.
            if (maxmsgsize >= 0 && msg_size > maxmsgsize) {
                LOG.error("Message size too large " + msg_size);
                decoding_error();
                return false;
            }

            //  Message size must fit within range of size_t data type.
            if (msg_size > Integer.MAX_VALUE) {
                LOG.error("Message size too large " + msg_size);
                decoding_error();
                return false;
            }

            msg_size = (int) size;
            next_step(null, msg_size, message_ready);

            return true;
        }

        private boolean flags_ready()
        {

            //  Store the flags from the wire into the message structure.
            msg_flags = 0;
            int first = buffer.get();
            if ((first & V1Protocol.MORE_FLAG) > 0)
                msg_flags |= Msg.MORE;

            if (first > 3) {
                LOG.error("Invalid Flag " + first);
                decoding_error();
                return false;
            }

            //  The payload length is either one or eight bytes,
            //  depending on whether the 'large' bit is set.
            if ((first & V1Protocol.LARGE_FLAG) > 0)
                next_step(null, 8, eight_byte_size_ready);
            else
                next_step(null, 1, one_byte_size_ready);

            return true;

        }

        private int push_msg()
        {
            if (count == 0)
                return 0;

            Msg msg = new Msg(4);
            msg.setFlags(Msg.MORE);
            ByteBuffer.wrap(msg.data()).putInt(count);

            int rc = msg_sink.push_msg(msg);
            if (rc != 0) {
                if (rc != ZError.EAGAIN) {
                    decoding_error();
                    return rc;
                }
                return 0;
            }

            msg = new SharedMsg(threshold, active, start, end, count);

            rc = msg_sink.push_msg(msg);
            if (rc != 0) {

                if (rc != ZError.EAGAIN) {
                    decoding_error();
                    return rc;
                }

                assert (false);
            }

            start = end;
            count = 0;
            return 0;
        }

        private boolean message_ready()
        {
            if (!identity_received) {
                if (msg_sink == null)
                    return false;

                Msg msg = new Msg(msg_size);
                assert (msg_flags == 0);
                msg.setFlags(msg_flags);

                buffer.get(msg.data());
                int rc = msg_sink.push_msg(msg);
                if (rc != 0) {
                    if (rc != ZError.EAGAIN)
                        decoding_error();

                    buffer.advance(msg_size);
                    return false;
                }
                identity_received = true;
                end = start = buffer.position();
            } else {
                buffer.advance(msg_size);
                if ((msg_flags & V1Protocol.MORE_FLAG) == 0) {
                    end = buffer.position();
                    count++;
                    total++;
                }
            }
            next_step(null, 1, flags_ready);

            return true;
        }

        @Override
        public boolean stalled()
        {
            if (!buffer.hasRemaining() && count == 0)
                return false;

            return true;
        }
    }

    public static class PersistEncoder extends EncoderBase
    {
        private static final int identity_ready = 0;
        private static final int size_ready = 1;
        private static final int type_ready = 2;
        private static final int status_ready = 3;
        private static final int message_ready = 4;

        private Msg in_progress;
        private final byte[] tmpbuf;
        private IMsgSource msg_source;
        private final int version;
        private FileChannel channel;
        private long channel_position;
        private long channel_count;
        private int type;
        private int status;
        private boolean channel_ready;

        public PersistEncoder(int bufsize_)
        {
            this(bufsize_, null, 0);
        }

        public PersistEncoder(int bufsize_, IMsgSource session, int version)
        {
            super(bufsize_);
            this.version = version;
            tmpbuf = new byte[10];
            msg_source = session;

            //  Write 0 bytes to the batch and go to file_ready state.
            next_step((byte[]) null, 0, identity_ready, true);
        }

        @Override
        public void set_msg_source(IMsgSource msg_source_)
        {
            msg_source = msg_source_;
        }

        @Override
        protected boolean next()
        {
            switch (state()) {
            case identity_ready:
                return identity_ready();
            case size_ready:
                return size_ready();
            case type_ready:
                return type_ready();
            case status_ready:
                return status_ready();
            case message_ready:
                return message_ready();
            default:
                return false;
            }
        }

        private final boolean size_ready()
        {
            //  Write message body into the buffer.
            if (channel_ready)
                next_step(channel, channel_position, channel_count, type_ready, false);
            else {
                boolean more = in_progress.hasMore();
                next_step(in_progress.data(), in_progress.size(),
                        more ? message_ready : type_ready, !more);
            }
            return true;
        }

        private final boolean identity_ready()
        {
            if (msg_source == null)
                return false;

            if (!require_msg())
                return false;

            return encode_message();
        }

        private final boolean type_ready()
        {
            //  Read new message. If there is none, return false.
            //  The first frame of the persistence codec is message type
            //  if type == MESSAGE_ERROR then close connection
            //  if type == MESSAGE_RESPONSE then transfer them all
            //  if type == MESSAGE_FILE then transfer file channel

            channel_ready = false;
            if (!require_msg())
                return false;

            type = in_progress.data()[0];

            next_step((byte[]) null, 0, status_ready, true);
            return true;
        }

        private final boolean status_ready()
        {
            if (!require_msg())
                return false;

            status = in_progress.data()[0];
            if (type == MESSAGE_FILE) {
                process_file();

                in_progress = new Msg(1);
                in_progress.setFlags(Msg.MORE);
                in_progress.put((byte) status);
            }

            return encode_message();
        }

        private final boolean message_ready()
        {
            if (type == MESSAGE_ERROR) {
                encoding_error();
                return false;
            }

            if (type == MESSAGE_FILE) {
                return encode_file();
            }

            if (!require_msg())
                return false;

            return encode_message();
        }

        private final void process_file()
        {
            // The second file frame is path
            boolean rc = require_msg();
            assert (rc);
            String path = new String(in_progress.data());

            // The third file frame is position
            rc = require_msg();
            assert (rc);
            channel_position = in_progress.buf().getLong();

            // The fourth file frame is sending count
            rc = require_msg();
            assert (rc);
            channel_count = in_progress.buf().getLong();

            try {
                channel = new FileInputStream(path).getChannel();
            } catch (IOException e) {
                e.printStackTrace();
                status = STATUS_INTERNAL_ERROR;
                type = MESSAGE_ERROR;
            }
        }

        private final boolean encode_file()
        {
            channel_ready = true;
            return encode_size((int) channel_count, false);
        }

        private final boolean encode_message()
        {
            if (version == V1Protocol.VERSION) {
                return v1_encode_message();
            } else {
                return v0_encode_message();
            }
        }

        private final boolean encode_size(int size, boolean more)
        {
            if (version == V1Protocol.VERSION) {
                return v1_encode_size(size, more);
            } else {
                return v0_encode_size(size, more);
            }
        }

        private boolean v1_encode_size(int size, boolean more)
        {
            int protocol_flags = 0;
            if (more)
                protocol_flags |= V1Protocol.MORE_FLAG;
            if (size > 255)
                protocol_flags |= V1Protocol.LARGE_FLAG;
            tmpbuf[0] = (byte) protocol_flags;

            //  Encode the message length. For messages less then 256 bytes,
            //  the length is encoded as 8-bit unsigned integer. For larger
            //  messages, 64-bit unsigned integer in network byte order is used.
            if (size > 255) {
                ByteBuffer b = ByteBuffer.wrap(tmpbuf);
                b.position(1);
                b.putLong(size);
                next_step(tmpbuf, 9, size_ready, false);
            } else {
                tmpbuf[1] = (byte) (size);
                next_step(tmpbuf, 2, size_ready, false);
            }
            return true;
        }

        private boolean v1_encode_message()
        {
            final int size = in_progress.size();

            v1_encode_size(size, in_progress.hasMore());
            return true;
        }

        private boolean v0_encode_size(int size, boolean more)
        {
            // Not implemented yet
            encoding_error();
            return false;
        }

        private boolean v0_encode_message()
        {
            // Not implemented yet
            encoding_error();
            return false;
        }

        private boolean require_msg()
        {

            in_progress = msg_source.pull_msg();
            if (in_progress == null) {
                return false;
            }
            return true;
        }

    }
}
