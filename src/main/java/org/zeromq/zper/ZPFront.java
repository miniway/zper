/*  =========================================================================
    ZPFront - ZPER front application 

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
import java.util.Properties;

import org.jeromq.ZDevice;
import org.jeromq.ZLogManager;
import org.jeromq.ZLogManager.ZLogConfig;
import org.jeromq.ZMQ;
import org.jeromq.ZMQ.Context;
import org.jeromq.ZMQ.Socket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZPFront extends ZPServer
{
    static final Logger LOG = LoggerFactory.getLogger(ZPFront.class);

    private final Context context;
    private final int numIOs;
    private final int numWorkers;
    private final int cacheSize;
    private final String bind;
    private final ArrayList <byte []> workers;
    
    public ZPFront (Properties conf)
    {
        numIOs = Integer.parseInt (conf.getProperty ("front.io_threads", "1"));
        numWorkers = Integer.parseInt (conf.getProperty ("front.workers", "1"));
        bind = conf.getProperty ("front.bind", "tcp://*:5555");
        cacheSize = Integer.parseInt (conf.getProperty ("cache_size", "1260"));

        ZLogConfig zc = ZLogManager.instance ().config ();
        zc.set ("base_dir", conf.getProperty ("base_dir"));
        zc.set ("segment_size", Long.parseLong (conf.getProperty ("segment_size","536870912")));

        LOG.info("Data is stored at " + zc.get("base_dir"));

        workers = new ArrayList <byte[]> ();
        context = ZMQ.context(numIOs);
    
    }
    
    @Override
    public void run ()
    {
        String workerBind = "inproc://worker";

        Socket router = context.socket (ZMQ.ROUTER);
        Socket inrouter = context.socket (ZMQ.ROUTER);

        inrouter.bind (workerBind);

        for (int i=0; i < numWorkers; i++) {
            String id = String.valueOf (i);
            ZPFrontWorker worker = new ZPFrontWorker (context, workerBind, id, cacheSize);
            worker.start ();
            workers.add (id.getBytes ());
        }

        router.bind (bind);

        LOG.info ("Server Started on " + bind);

        ZDevice.addressDevice (router, inrouter, workers);
        
        LOG.info("Ended " + this.getClass ().getName ());
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
            ZPUtils.setup(argv, ZPFront.class);
        } catch (Exception e) {
            LOG.error(
              "Aborting: Unexpected problem with environment." + e.getMessage(), e);
            System.exit(-1);
        }
    }

}
