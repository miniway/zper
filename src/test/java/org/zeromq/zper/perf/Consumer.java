package org.zeromq.zper.perf;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.jeromq.ZContext;
import org.jeromq.ZMQ;
import org.jeromq.ZMQ.Msg;
import org.jeromq.ZMQ.Socket;
import org.zeromq.zper.ZPUtils;
import org.zeromq.zper.base.MsgIterator;

public class Consumer
{
    private ZContext ctx;
    private Socket sock;

    public Consumer (String addr, String topic, int bufsize)
    {
        ctx = new ZContext ();
        ctx.setLinger (-1);
        sock = ctx.createSocket (ZMQ.DEALER);
        sock.setReceiveBufferSize (bufsize);
        sock.setIdentity (ZPUtils.genTopicIdentity (topic, 0));
        sock.connect ("tcp://" + addr);
    }

    public long[] getOffsetsBefore (String topic, int ts, int entry)
    {
        sock.sendMore ("OFFSET");
        sock.sendMore (ByteBuffer.allocate (8).putLong (ts).array ());
        sock.send (ByteBuffer.allocate (4).putInt (entry).array ());
        
        sock.recv (); // status
        
        long [] offsets = new long [entry];
        int idx = 0;
        while (sock.hasReceiveMore ()) {
            if (idx < entry)
                offsets [idx] = ((ByteBuffer)ByteBuffer.allocate (8).put (sock.recv ()).flip ()).getLong ();
            idx ++;
        }
        
        return offsets;
    }

    public List<Message> fetch (long offset, long fetchSize)
    {
        
        sock.sendMore ("FETCH");
        sock.sendMore (ByteBuffer.allocate (8).putLong (offset).array ());
        sock.send (ByteBuffer.allocate (8).putLong (fetchSize).array ());

        byte [] status = sock.recv (); // status
        
        ArrayList <Message> result = new ArrayList <Message> ();
        
        if (status[0] != 100) {
            System.out.println ("no more");
            return result;
        }
        ByteBuffer buf = ByteBuffer.wrap (sock.recv ());

        MsgIterator it = new MsgIterator (buf);
        
        
        while (it.hasNext ()) {
            Msg header = it.next ();
            if (it.hasNext ()) {
                Msg data = it.next ();
                result.add (new Message (header.data (), data.data ()));
            } else
                break;
        }
        
        return result;
    }

}
