/*  =========================================================================
    ZPServer - ZPER abstract base server class 

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
package org.zper;

import java.util.Properties;

import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zper.server.ZPReader;
import org.zper.server.ZPWriter;

public class ZPer
{
    static final Logger LOG = LoggerFactory.getLogger(ZPer.class);

    private Properties conf;
    private ZContext context;
    private boolean destoryed = false;

    public ZPer(Properties conf)
    {
        this.conf = conf;
    }

    public void start()
    {
        context = new ZContext();
        // fix lazy creation
        context.setContext(ZMQ.context(Integer.parseInt(conf.getProperty("io_threads", "1"))));


        ZPWriter writer = new ZPWriter(context, conf);
        ZPReader reader = new ZPReader(context, conf);

        writer.start();
        reader.start();

    }

    synchronized public void shutdown()
    {
        if (!destoryed) {
            context.destroy();
            destoryed = true;
        }
    }

    public static void main(String[] argv)
    {
        Properties conf = ZPUtils.setup(argv);

        final ZPer serv = new ZPer(conf);
        serv.start();

        Runtime.getRuntime().addShutdownHook(new Thread("shutdownHook")
        {

            @Override
            public void run()
            {
                serv.shutdown();
            }

        });

    }
}
