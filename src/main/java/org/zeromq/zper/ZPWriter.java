/*  =========================================================================
    ZPFront - ZPER Writer application 

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
package org.zeromq.zper;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.jeromq.ZDevice;
import org.zeromq.zper.base.ZLogManager;
import org.zeromq.zper.base.Persistence.PersistDecoder;
import org.zeromq.zper.base.ZLogManager.ZLogConfig;
import org.jeromq.ZContext;
import org.jeromq.ZMQ;
import org.jeromq.ZMQ.Socket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZPWriter extends Thread
{
    static final Logger LOG = LoggerFactory.getLogger(ZPWriter.class);

    private final ZContext context;
    private final int numWorkers;
    private final String bind;
    private final List <byte []> workers;
    
    // socket parameter
    private final int recvBufferSize;
    private final long maxMessageSize;
    
    public ZPWriter (ZContext context, Properties conf)
    {
        this.context = ZContext.shadow (context);
        
        numWorkers = Integer.parseInt (conf.getProperty ("writer.workers", "5"));
        bind = conf.getProperty ("writer.bind", "tcp://*:5555");

        recvBufferSize = Integer.parseInt (conf.getProperty ("receive_buffer", "1048576"));
        maxMessageSize = Long.parseLong (conf.getProperty ("max_message", "10485760"));
        
        ZLogConfig zc = ZLogManager.instance ().config ();
        zc.set ("base_dir", conf.getProperty ("base_dir"));
        zc.set ("segment_size", Long.parseLong (conf.getProperty ("segment_size","536870912")));
        zc.set ("flush_messages", Long.parseLong (conf.getProperty ("flush_messages","10000")));
        zc.set ("flush_interval", Long.parseLong (conf.getProperty ("flush_interval","10000")));
        zc.set ("retain_hours", Integer.parseInt (conf.getProperty ("retain_hours","168")));
        zc.set ("recover", true);
        
        LOG.info("Data is stored at " + zc.get("base_dir"));

        workers = new ArrayList <byte[]> ();
    }
    
    @Override
    public void run ()
    {
        String workerBind = "inproc://writer-worker";

        Socket router = context.createSocket (ZMQ.ROUTER);
        Socket inrouter = context.createSocket (ZMQ.ROUTER);
        router.setReceiveBufferSize (recvBufferSize);
        router.setMaxMsgSize (maxMessageSize);
        
        router.setDecoder (PersistDecoder.class);
        inrouter.setRouterMandatory (true);

        inrouter.bind (workerBind);

        for (int i=0; i < numWorkers; i++) {
            String id = String.valueOf (i);
            ZPWriterWorker worker = new ZPWriterWorker (context, workerBind, id);
            worker.start ();
            workers.add (id.getBytes ());
        }

        router.bind (bind);

        LOG.info ("Front Server Started on " + bind);

        ZDevice.addressDevice (router, inrouter, workers);
        
        LOG.info("Front Ended");
        ZLogManager.instance ().shutdown ();
        
        context.destroy ();

    }

}
