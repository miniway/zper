/*  =========================================================================
    ZPReader - ZPER Reader 

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
package org.zper.server;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.zper.base.ZDevice;
import org.zeromq.ZMQ;
import org.zper.base.ZLogManager;
import org.zper.base.Persistence.PersistEncoder;
import org.zper.base.ZLogManager.ZLogConfig;
import org.zeromq.ZContext;
import org.zeromq.ZMQ.Socket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZPReader extends Thread
{
    static final Logger LOG = LoggerFactory.getLogger(ZPReader.class);

    private final ZContext context;
    private final int numWorkers;
    private final String bind;
    private final int sendBufferSize;

    private final List<byte[]> workers;

    public ZPReader(ZContext context, Properties conf)
    {
        this.context = ZContext.shadow(context);

        sendBufferSize = Integer.parseInt(conf.getProperty("send_buffer", "1048576"));
        numWorkers = Integer.parseInt(conf.getProperty("reader.workers", "5"));
        bind = conf.getProperty("reader.bind", "tcp://*:5556");

        ZLogConfig zc = ZLogManager.instance().config();
        zc.set("base_dir", conf.getProperty("base_dir"));

        LOG.info("Data is stored at " + zc.get("base_dir"));

        workers = new ArrayList<byte[]>();

    }

    @Override
    public void run()
    {
        String workerBind = "inproc://reader-worker";

        Socket router = context.createSocket(ZMQ.ROUTER);
        Socket inrouter = context.createSocket(ZMQ.ROUTER);

        router.setSendBufferSize(sendBufferSize);
        router.setEncoder(PersistEncoder.class);

        inrouter.bind(workerBind);

        for (int i = 0; i < numWorkers; i++) {
            String id = String.valueOf(i);
            ZPReaderWorker worker = new ZPReaderWorker(context, workerBind, id);
            worker.start();
            workers.add(id.getBytes());
        }

        router.bind(bind);

        LOG.info("Reader bind on " + bind);

        ZDevice.loadBalanceDevice(router, inrouter, workers);

        ZLogManager.instance().shutdown();

        context.destroy();
        LOG.info("Reader Front Ended");
    }

}
