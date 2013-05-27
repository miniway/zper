package org.zper;

import static org.junit.Assert.*;

import java.util.Properties;

import org.zper.base.ZLog;
import org.zper.base.ZLogManager;
import org.jeromq.ZMQ;
import org.jeromq.ZMQ.Context;
import org.jeromq.ZMQ.Socket;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestZPWriter
{
    private static ZPer server;
    private static String bind = "tcp://*:5557";
    private static String topic = "test";

    @BeforeClass
    public static void start () throws Exception 
    {
        Properties conf = new Properties ();
        conf.setProperty ("writer.bind", bind);
        conf.setProperty ("base_dir", "/tmp/zlogs/");
        
        server = new ZPer (conf);
        server.start();
    }
    
    @AfterClass
    public static void tearDown () throws Exception 
    {
        server.shutdown ();
    }
    
    @Test
    public void testSend () throws Exception 
    {
        Context ctx = ZMQ.context (1);
        Socket sock = ctx.socket (ZMQ.DEALER);
        
        ZLog zlog = ZLogManager.instance().get(topic);
        long offset = zlog.offset();
        String data = "hello";
        
        System.out.println("previous offset " + zlog.path ().getAbsolutePath () + ":" + offset);

        sock.setIdentity (ZPUtils.genTopicIdentity (topic, 0));
        sock.setLinger (100);

        boolean ret = sock.connect ("tcp://127.0.0.1:5557");
        assertTrue (ret);


        ret = sock.send (data);
        assertTrue (ret);

        // wait until flush
        Thread.sleep (1000);

        assertEquals (zlog.offset(), offset + data.length () + 2);
        
        sock.close ();
        ctx.term ();
    }
    
    @Test
    public void testSendPush () throws Exception {
        Context ctx = ZMQ.context (1);
        Socket sock = ctx.socket (ZMQ.PUSH);
        
        ZLog zlog = ZLogManager.instance().get(topic);
        long offset = zlog.offset();
        String data = "hello";
        
        System.out.println("previous offset " + zlog.path ().getAbsolutePath () + ":" + offset);

        sock.setIdentity (ZPUtils.genTopicIdentity (topic, 0));
        sock.setLinger (100);

        boolean ret = sock.connect ("tcp://127.0.0.1:5557");
        assertTrue (ret);


        ret = sock.send (data);
        assertTrue (ret);

        // wait until flush
        Thread.sleep (1000);

        assertEquals (zlog.offset(), offset + data.length () + 2);
        
        sock.close ();
        ctx.term ();
    }

}
