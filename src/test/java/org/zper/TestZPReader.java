package org.zper;

import static org.junit.Assert.*;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.zper.base.ZLog;
import org.zper.base.ZLogManager;
import org.zper.base.ZLog.SegmentInfo;
import org.jeromq.ZMQ;
import org.jeromq.ZMQ.Context;
import org.jeromq.ZMQ.Msg;
import org.jeromq.ZMQ.Socket;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestZPReader
{
    private static ZPer server;
    private static String bind = "tcp://*:5556";
    private static String topic = "test";

    @BeforeClass
    public static void start () throws Exception {
        
        Properties conf = new Properties ();
        conf.setProperty ("reader.bind", bind);
        conf.setProperty ("base_dir", "/tmp/zlogs/");
        
        server = new ZPer (conf);
        server.start();
        
    }
    
    @AfterClass
    public static void tearDown () throws Exception {
        server.shutdown ();
    }
    
    @Test
    public void testOffset () throws Exception {
        Context ctx = ZMQ.context (1);
        Socket sock = ctx.socket (ZMQ.DEALER);
        
        ZLog zlog = ZLogManager.instance().get (topic);
        SegmentInfo [] infos = zlog.segments ();

        sock.setIdentity (ZPUtils.genTopicIdentity (topic, 0));
        sock.setLinger (100);

        boolean ret = sock.connect ("tcp://127.0.0.1:5556");
        assertTrue (ret);

        //  Earliest offset
        sock.sendMore ("OFFSET");
        sock.send (ByteBuffer.allocate (8).putLong (-2).array ());

        Msg status = sock.recvMsg ();
        assertTrue (status.hasMore ());
        assertEquals (100, status.data ()[0]);

        Msg oldest = sock.recvMsg ();
        assertFalse (oldest.hasMore ());
        assertEquals (infos[0].start (), oldest.buf ().getLong ());
        
        //  Latest offset
        sock.sendMore ("OFFSET");
        sock.send (ByteBuffer.allocate (8).putLong (-1).array ());

        status = sock.recvMsg ();
        assertTrue (status.hasMore ());
        assertEquals (100, status.data ()[0]);

        Msg latest = sock.recvMsg ();
        assertTrue (latest.hasMore ());
        assertEquals (infos[infos.length -1].start (), latest.buf ().getLong ());

        Msg last = sock.recvMsg ();
        assertFalse (last.hasMore ());
        assertEquals (infos[infos.length -1].offset (), last.buf ().getLong ());

        //  Modified before
        sock.sendMore ("OFFSET");
        sock.send (ByteBuffer.allocate (8).putLong (System.currentTimeMillis ()).array ());

        status = sock.recvMsg ();
        assertTrue (status.hasMore ());
        assertEquals (100, status.data ()[0]);

        List <Long> offsetList = new ArrayList <Long> ();
        
        while (true) {
            Msg msg = sock.recvMsg ();
            offsetList.add (msg.buf ().getLong ());
            if (!msg.hasMore ())
                break;
        }
        assertEquals (infos.length + 1, offsetList.size ());
        for (int i = 0; i < infos.length; i++) {
            assertEquals (infos [i].start (), (long) offsetList.get (i)); 
        }
        assertEquals (infos [infos.length -1].offset (), (long) offsetList.get (infos.length));


        sock.close ();
        ctx.term ();
    }
    
    @Test
    public void testFetch () throws Exception {
        Context ctx = ZMQ.context (1);
        Socket sock = ctx.socket (ZMQ.DEALER);
        
        ZLog zlog = ZLogManager.instance().get(topic);
        SegmentInfo [] infos = zlog.segments ();
        SegmentInfo last = infos [infos.length - 1];
        
        sock.setIdentity (ZPUtils.genTopicIdentity (topic, 0));

        sock.connect ("tcp://127.0.0.1:5556");

        //  Latest offset
        sock.sendMore ("FETCH");
        sock.sendMore (ByteBuffer.allocate (8).putLong (last.start ()).array ());
        sock.send (ByteBuffer.allocate (8).putLong (Integer.MAX_VALUE).array ());

        Msg status = sock.recvMsg ();
        assertEquals (100, status.data ()[0]);

        Msg result = sock.recvMsg ();
        assertEquals ((int) (last.offset () - last.start ()), result.size ());
        
        Iterator <Msg> it = new MsgIterator (result.buf ());
        while (it.hasNext ()) {
            it.next ();
        }
        
        sock.close ();
        ctx.term ();
    }

}
