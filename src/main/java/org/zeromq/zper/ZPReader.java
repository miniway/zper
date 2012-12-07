package org.zeromq.zper;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.jeromq.ZDevice;
import org.zeromq.zper.base.ZLogManager;
import org.jeromq.ZMQ;
import org.zeromq.zper.base.Persistence.PersistEncoder;
import org.zeromq.zper.base.ZLogManager.ZLogConfig;
import org.jeromq.ZContext;
import org.jeromq.ZMQ.Socket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZPReader extends Thread
{
    static final Logger LOG = LoggerFactory.getLogger(ZPReader.class);
    
    private final ZContext context;
    private final int numWorkers;
    private final String bind;
    private final int sendBufferSize;
    
    private final List <byte []> workers;
    
    public ZPReader (ZContext context, Properties conf)
    {
        this.context = ZContext.shadow (context);
        
        sendBufferSize = Integer.parseInt (conf.getProperty ("send_buffer", "1048576"));
        numWorkers = Integer.parseInt (conf.getProperty ("reader.workers", "5"));
        bind = conf.getProperty ("reader.bind", "tcp://*:5556");

        ZLogConfig zc = ZLogManager.instance ().config ();
        zc.set ("base_dir", conf.getProperty ("base_dir"));

        LOG.info("Data is stored at " + zc.get("base_dir"));

        workers = new ArrayList <byte []> ();
        
    }
    
    @Override
    public void run ()
    {
        String workerBind = "inproc://reader-worker";

        Socket router = context.createSocket (ZMQ.ROUTER);
        Socket inrouter = context.createSocket (ZMQ.ROUTER);

        router.setSendBufferSize (sendBufferSize);
        router.setEncoder (PersistEncoder.class);
        
        inrouter.bind (workerBind);

        for (int i=0; i < numWorkers; i++) {
            String id = String.valueOf (i);
            ZPReaderWorker worker = new ZPReaderWorker (context, workerBind, id);
            worker.start ();
            workers.add (id.getBytes ());
        }

        router.bind (bind);

        LOG.info ("Reader bind on " + bind);
        
        ZDevice.loadBalanceDevice (router, inrouter, workers);
        
        ZLogManager.instance ().shutdown ();
        
        context.destroy ();
        LOG.info ("Reader Front Ended");
    }
    
}
