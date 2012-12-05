package org.zeromq.zper;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.zeromq.zper.base.Persistence;
import org.zeromq.zper.base.ZLog;
import org.zeromq.zper.base.ZLog.SegmentInfo;
import org.zeromq.zper.base.ZLogManager;
import org.jeromq.ZMQ;
import org.jeromq.ZMQException;
import org.jeromq.ZMQ.Context;
import org.jeromq.ZMQ.Msg;
import org.jeromq.ZMQ.Socket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZPReaderWorker extends Thread
{
    private static final Logger LOG = LoggerFactory.getLogger (ZPWriterWorker.class);
    
    // states
    private static final int START = 0;
    private static final int TOPIC = 1;
    private static final int COMMAND = 2;
    private static final int ARGUMENT = 3;
    
    private final Context context ;
    private final String bindAddr;
    private final String identity;
    private final ZLogManager logMgr;
    private Socket worker ;

    public ZPReaderWorker (Context context, String bindAddr, String identity)
    {
        this.context = context;
        this.bindAddr = bindAddr;
        this.identity = identity;

        logMgr = ZLogManager.instance();
    }

    @Override
    public void run ()
    {
        LOG.info ("Started Worker " + identity);
        worker = context.socket (ZMQ.DEALER);
        worker.setIdentity (identity);
        worker.connect (bindAddr);
        try {
            loop();
        } catch (ZMQException.CtxTerminated e) {
        }

        LOG.info("Ended Worker " + identity);
        worker.close();
    }
    
    public void loop() {

        int state = START;

        String topic = null;
        boolean more = false;
        boolean stop = false;
        ZLog zlog = null;
        Msg msg;
        int command = 0;
        List <Msg> args = new ArrayList <Msg> ();

        while (!Thread.currentThread ().isInterrupted ()
                && !stop) {

            msg = worker.recvMsg (0);
            if (msg == null)
                break;
            more = worker.hasReceiveMore ();

            switch (state) {
            case START:
                byte [] id = msg.data ();
                if (id == null)
                    break;
                int tsize = id [1];
                topic = new String (id, 2, tsize);
                state = TOPIC;
                zlog = logMgr.get (topic);
                
                worker.sendMore (id);
                args.clear ();
                break;

            case TOPIC:

                if (msg.size () == 0 && more) { // bottom
                    state = COMMAND;
                } else {
                    worker.sendMore (msg);
                }
                break;

            case COMMAND:
                
                command = msg.data () [1];
                state = ARGUMENT;
                break;
                
            case ARGUMENT:
                
                args.add (msg);
                if (!more) {
                    processCommand (zlog, command, args);
                    state = START;
                }
                break;
            }
        }
    }

    private void processCommand (ZLog zlog, int command, List<Msg> args)
    {
        switch (command) 
        {
        case ZPConstants.COMMAND_FETCH:
            processFetch (zlog, args);
            break;
        }
    }

    private void processFetch (ZLog zlog, List<Msg> args)
    {
        if (args.size () != 2) {
            error ();
            return;
        }
        long offset = args.get (0).buf ().getLong ();
        long size = args.get (1).buf ().getLong ();

        SegmentInfo info = zlog.segmentInfo (offset);
        
        if (info == null || info.offset () < offset) {
            code (ZPConstants.TYPE_RESPONSE, ZPConstants.STATUS_INVALID_OFFSET, false);
            return;
        }
        
        assert (info.start () <= offset);
        
        code (ZPConstants.TYPE_FILE, ZPConstants.STATUS_OK, true);
        worker.sendMore (info.path ());
        sendLong (offset - info.start (), true);
        if (info.offset () - offset < size)
            size = info.offset () - offset;
        sendLong (size, false);
    }
    
    private void sendLong (long value, boolean more) 
    {
        ByteBuffer buf = ByteBuffer.allocate (8).putLong (value);
        if (more)
            worker.sendMore (buf.array ());
        else
            worker.send (buf.array ());
    }
    
    private void error () 
    {
        worker.send (new byte [] {Persistence.MESSAGE_ERROR});
    }

    private void code (int type, int code, boolean more) 
    {
        worker.sendMore (new byte [] {(byte) type});
        if (more)
            worker.sendMore (new byte [] {(byte) code});
        else
            worker.send (new byte [] {(byte) code});
    }
}
