package org.zeromq.zper;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.jeromq.ZDevice;
import org.zeromq.zper.base.ZLogManager;
import org.jeromq.ZMQ;
import org.zeromq.zper.base.Persistence.PersistEncoder;
import org.zeromq.zper.base.ZLogManager.ZLogConfig;
import org.jeromq.ZMQ.Context;
import org.jeromq.ZMQ.Socket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZPReader extends ZPServer
{
    static final Logger LOG = LoggerFactory.getLogger(ZPReader.class);
    
    private final Context context;
    private final int numIOs;
    private final int numWorkers;
    private final String bind;
    
    private final List <byte []> workers;
    
    public ZPReader (Properties conf)
    {
        numIOs = Integer.parseInt (conf.getProperty ("reader.io_threads", "1"));
        numWorkers = Integer.parseInt (conf.getProperty ("reader.workers", "5"));
        bind = conf.getProperty ("reader.bind", "tcp://*:5556");

        ZLogConfig zc = ZLogManager.instance ().config ();
        zc.set ("base_dir", conf.getProperty ("base_dir"));

        LOG.info("Data is stored at " + zc.get("base_dir"));

        workers = new ArrayList <byte []> ();
        context = ZMQ.context(numIOs);
        
    }
    
    @Override
    public void run ()
    {
        String workerBind = "inproc://worker";

        Socket router = context.socket (ZMQ.ROUTER);
        Socket inrouter = context.socket (ZMQ.ROUTER);

        router.setEncoder (PersistEncoder.class);
        
        inrouter.bind (workerBind);

        for (int i=0; i < numWorkers; i++) {
            String id = String.valueOf (i);
            ZPReaderWorker worker = new ZPReaderWorker (context, workerBind, id);
            worker.start ();
            workers.add (id.getBytes ());
        }

        router.bind (bind);

        LOG.info ("Rear Server Started on " + bind);
        
        ZDevice.loadBalanceDevice (router, inrouter, workers);
        
        LOG.info ("Front Ended");
        ZLogManager.instance ().shutdown ();
        router.close ();
        inrouter.close ();
    }
    
    @Override
    public void shutdown ()
    {
        context.term ();
    }

    public static void main(String[] argv) {
        try {
            ZPUtils.setup(argv, ZPReader.class);
        } catch (Exception e) {
            LOG.error(
              "Aborting: Unexpected problem with environment." + e.getMessage(), e);
            System.exit(-1);
        }
    }

}
