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
package org.zeromq.zper.base;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.LockSupport;

import zmq.DecoderBase;
import zmq.EncoderBase;
import zmq.IMsgSink;
import zmq.IMsgSource;
import zmq.Msg;
import zmq.V1Protocol;
import zmq.ZError;

public class Persistence {

    public static final int MESSAGE_RESPONSE = 1;
    public static final int MESSAGE_FILE = 2;
    public static final int MESSAGE_ERROR = 3;
    
    public static final int STATUS_INTERNAL_ERROR = -1;
    
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
        private int read_pos;
        private final int version;
        private ByteBuffer buffer;              //  Current operating buffer
        private ByteBuffer active;              //  Active buffer
        private ByteBuffer standby;             //  Standby buffer
        private ByteBuffer threshold;           
        private ByteBuffer standby_threshold;
        private ByteBuffer dummy;               //  Dummy buffer
        private boolean identity_received;
        private final int bufsize;
        private int start;
        private int end;
        private int count;
        
        public PersistDecoder (int bufsize_, long maxmsgsize_, IMsgSink session, int version_)
        {
            super (0);

            maxmsgsize = maxmsgsize_;
            version = version_;
            msg_sink = session;
            identity_received = false;
            
            if (maxmsgsize_ < 0)
                bufsize = bufsize_ * 10;
            else
                bufsize = (int) maxmsgsize_;
            
            active = ByteBuffer.allocateDirect (bufsize);
            standby = ByteBuffer.allocateDirect (bufsize);
            dummy = ByteBuffer.allocate (0);
            threshold = standby_threshold = null;
            start = end = count = 0;
            read_pos = 0;
            next_step (null, 1, flags_ready);
        }

        @Override
        public void set_msg_sink (IMsgSink msg_sink)
        {
            this.msg_sink = msg_sink;
        }

        @Override
        protected boolean next() 
        {
            if (version == V1Protocol.VERSION) {
                switch(state()) {
                case one_byte_size_ready:
                    return one_byte_size_ready ();
                case eight_byte_size_ready:
                    return eight_byte_size_ready ();
                case flags_ready:
                    return flags_ready ();
                case message_ready:
                    return message_ready ();
                default:
                    return false;
                }
            }
            return false;
        }

        @Override
        public ByteBuffer get_buffer () 
        {
            if (active.remaining () < to_read) {
                
                if (threshold != null && threshold.hasRemaining ()) {
                    LockSupport.parkNanos (1);
                    return dummy; // busy waiting
                }
                
                active.limit (active.position ());
                active.position (end);
    
                start = end = 0;
                standby.clear ();
                standby.put (active.slice ());
                
                ByteBuffer temp = active;
                active = standby;
                standby = temp;
                threshold = standby_threshold;
            }
            buffer = active.slice ();
            read_pos = 0;
            return buffer;
        }

        @Override
        public int process_buffer (ByteBuffer buf, int size_) 
        {
            //  Check if we had an error in previous attempt.
            if (state() < 0)
                return -1;
            

            while (true) {

                //  Try to get more space in the message to fill in.
                //  If none is available, return.
                while (to_read == 0) {
                    if (!next ()) {
                        if (state() < 0) {
                            return -1;
                        }

                        return buf.position ();
                    }
                }

                //  If there are no more data in the buffer, return.
                if (to_read > buf.remaining ()) {

                    if (!push_msg ()) {
                        return buf.position (); // block
                    }
                    
                    to_read -= buf.remaining ();
                    active.position (active.position () + size_);
                    
                    return size_;
                }
                
                //  Copy the data from buffer to the message.
                read_pos += to_read;
                buf.position (read_pos);
                to_read = 0;
            }
        }

        private boolean one_byte_size_ready () {
            
            buffer.position (read_pos - 1);
            msg_size = buffer.get ();
            if (msg_size < 0)
                msg_size = (0xff) & msg_size;

            //  Message size must not exceed the maximum allowed size.
            if (maxmsgsize >= 0)
                if (msg_size > maxmsgsize) {
                    decoding_error ();
                    return false;
                }

            next_step (null, msg_size, message_ready);
            
            return true;
        }
        
        private boolean eight_byte_size_ready() {
            
            //  The payload size is encoded as 64-bit unsigned integer.
            //  The most significant byte comes first.
            buffer.position (read_pos - 8);
            final long size = buffer.getLong();

            //  Message size must not exceed the maximum allowed size.
            if (maxmsgsize >= 0)
                if (msg_size > maxmsgsize) {
                    decoding_error ();
                    return false;
                }

            //  Message size must fit within range of size_t data type.
            if (msg_size > Integer.MAX_VALUE) {
                decoding_error ();
                return false;
            }
            
            msg_size = (int) size;
            next_step (null, msg_size, message_ready);

            return true;
        }
        
        private boolean flags_ready() {

            //  Store the flags from the wire into the message structure.
            msg_flags = 0;
            buffer.position (read_pos - 1);
            int first = buffer.get ();
            if ((first & V1Protocol.MORE_FLAG) > 0)
                msg_flags |= Msg.more;
            
            //  The payload length is either one or eight bytes,
            //  depending on whether the 'large' bit is set.
            if ((first & V1Protocol.LARGE_FLAG) > 0)
                next_step (null, 8, eight_byte_size_ready);
            else
                next_step (null, 1, one_byte_size_ready);
            
            return true;

        }
        
        private boolean push_msg () 
        {
            if (count == 0)
                return true;
            
            Msg msg = new Msg (4);
            msg.set_flags (Msg.more);
            ByteBuffer.wrap (msg.data ()).putInt (count);
            
            boolean rc = msg_sink.push_msg (msg);
            if (!rc) {
                if (!ZError.is (ZError.EAGAIN))
                    decoding_error ();
                
                return false;
            }
            
            int pos = active.position ();
            active.position (start);
            active.limit (end);
            
            msg = new Msg (active.slice ());
            
            active.limit (bufsize);
            active.position (pos);

            
            rc = msg_sink.push_msg (msg);
            while (!rc) {

                if (!ZError.is (ZError.EAGAIN)) {
                    decoding_error ();
                    return false;
                }

                rc = msg_sink.push_msg (msg);
            }
            
            start = end;
            count = 0;
            standby_threshold = msg.buf ();
            return true;
        }
        
        private boolean message_ready () 
        {
            if (!identity_received) {
                if (msg_sink == null)
                    return false;
                
                Msg msg = new Msg (msg_size);
                assert (msg_flags == 0);
                msg.set_flags (msg_flags);

                buffer.position (read_pos - msg_size);
                buffer.get (msg.data ());
                boolean rc = msg_sink.push_msg (msg);
                if (!rc) {
                    if (!ZError.is (ZError.EAGAIN))
                        decoding_error ();

                    buffer.position (buffer.position () - msg_size);
                    return false;
                }
                identity_received = true;
                start = buffer.position ();
            } else {
                if ((msg_flags & V1Protocol.MORE_FLAG) == 0) {
                    end = active.position () + buffer.position ();
                    count ++;
                }
            }
            next_step (null, 1, flags_ready);
            
            return true;
        }
        
        @Override
        public boolean stalled () 
        {
            if (!buffer.hasRemaining ())
                return false;
            
            return super.stalled ();
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
        private final byte [] tmpbuf;
        private IMsgSource msg_source;
        private final int version;
        private FileChannel channel;
        private long channel_position;
        private long channel_count;
        private int type;
        private int status;
        private boolean channel_ready;
        
        public PersistEncoder (int bufsize_)
        {
            this (bufsize_, null, 0);
        }
        
        public PersistEncoder (int bufsize_, IMsgSource session, int version)
        {
            super (bufsize_);
            this.version = version;
            tmpbuf = new byte [10];
            msg_source = session;
            
            //  Write 0 bytes to the batch and go to file_ready state.
            next_step ((byte []) null, 0, identity_ready, true);
        }

        @Override
        public void set_msg_source (IMsgSource msg_source_)
        {
            msg_source = msg_source_;
        }

        @Override
        protected boolean next () 
        {
            switch (state ()) {
            case identity_ready:
                return identity_ready ();
            case size_ready:
                return size_ready ();
            case type_ready:
                return type_ready ();
            case status_ready:
                return status_ready ();
            case message_ready:
                return message_ready ();
            default:
                return false;
            }
        }

        private final boolean size_ready ()
        {
            //  Write message body into the buffer.
            if (channel_ready)
                next_step (channel, channel_position, channel_count, type_ready, false);
            else {
                boolean more = in_progress.has_more();
                next_step (in_progress.data (), in_progress.size (),
                        more ? message_ready : type_ready, !more);
            }
            return true;
        }

        private final boolean identity_ready ()
        {
            if (msg_source == null)
                return false;
            
            if (!require_msg ())
                return false;
            
            return encode_message ();
        }
        
        private final boolean type_ready ()
        {
            //  Read new message. If there is none, return false.
            //  The first frame of the persistence codec is message type
            //  if type == MESSAGE_ERROR then close connection
            //  if type == MESSAGE_RESPONSE then transfer them all
            //  if type == MESSAGE_FILE then transfer file channel
            
            channel_ready = false;
            if (!require_msg ())
                return false;
            
            type = in_progress.data () [0];
            
            next_step ((byte []) null, 0, status_ready, true);
            return true;
        }
        
        private final boolean status_ready ()
        {
            if (!require_msg ())
                return false;
            
            status = in_progress.data () [0];
            if (type == MESSAGE_FILE) {
                process_file ();
                
                in_progress = new Msg (1);
                in_progress.set_flags (Msg.more);
                in_progress.put ((byte) status);
            }
            
            return encode_message ();
        }
        
        private final boolean message_ready ()
        {
            if (type == MESSAGE_ERROR) {
                encoding_error ();
                return false;
            }
                
            if (type == MESSAGE_FILE) {
                return encode_file ();
            } 
            
            if (!require_msg ())
                return false;

            return encode_message ();
        }

        private final void process_file () 
        {
            // The second file frame is path
            boolean rc = require_msg ();
            assert (rc);
            String path = new String (in_progress.data ());

            // The third file frame is position
            rc = require_msg ();
            assert (rc);
            channel_position = in_progress.buf ().getLong ();

            // The fourth file frame is sending count
            rc = require_msg ();
            assert (rc);
            channel_count = in_progress.buf ().getLong ();

            try {
                channel = new FileInputStream(path).getChannel ();
            } catch (IOException e) {
                e.printStackTrace();
                status = STATUS_INTERNAL_ERROR;
                type = MESSAGE_ERROR;
            }
        }
        
        private final boolean encode_file () 
        {
            channel_ready = true;
            return encode_size ((int) channel_count, false);
        }
        
        private final boolean encode_message ()
        {
            if (version == V1Protocol.VERSION) {
                return v1_encode_message ();
            } else {
                return v0_encode_message ();
            }
        }

        private final boolean encode_size (int size, boolean more)
        {
            if (version == V1Protocol.VERSION) {
                return v1_encode_size (size, more);
            } else {
                return v0_encode_size (size, more);
            }
        }

        private boolean v1_encode_size (int size, boolean more)
        {
            int protocol_flags = 0;
            if (more)
                protocol_flags |= V1Protocol.MORE_FLAG;
            if (size > 255)
                protocol_flags |= V1Protocol.LARGE_FLAG;
            tmpbuf [0] = (byte) protocol_flags;
            
            //  Encode the message length. For messages less then 256 bytes,
            //  the length is encoded as 8-bit unsigned integer. For larger
            //  messages, 64-bit unsigned integer in network byte order is used.
            if (size > 255) {
                ByteBuffer b = ByteBuffer.wrap (tmpbuf);
                b.position (1);
                b.putLong (size);
                next_step (tmpbuf, 9, size_ready, false);
            }
            else {
                tmpbuf [1] = (byte) (size);
                next_step (tmpbuf, 2, size_ready, false);
            }
            return true;
        }
        private boolean v1_encode_message ()
        {
            final int size = in_progress.size ();
            
            v1_encode_size (size, in_progress.has_more ());
            return true;
        }
        
        private boolean v0_encode_size (int size, boolean more)
        {
            // Not implemented yet
            encoding_error ();
            return false;
        }
        
        private boolean v0_encode_message ()
        {
            // Not implemented yet
            encoding_error ();
            return false;
        }
        
        private boolean require_msg () {
            
            in_progress = msg_source.pull_msg ();
            if (in_progress == null) {
                return false;
            }
            return true;
        }

    }
}
