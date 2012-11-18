/*  =========================================================================
    ZPFrontWorker - ZPER front worker 

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

import java.io.IOException;

import org.jeromq.ZLog;
import org.jeromq.ZLogManager;
import org.jeromq.ZMQ;
import org.jeromq.ZMQ.Context;
import org.jeromq.ZMQ.Msg;
import org.jeromq.ZMQ.Socket;
import org.jeromq.ZMQException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZPFrontWorker extends Thread
{
    private static final Logger LOG = LoggerFactory.getLogger (ZPFrontWorker.class);
    
    // states
    private static final int START = 0;
    private static final int TOPIC_RECEIVED = 1;
    private static final int MESSAGE_BEGINS = 2;
    
    private final Context context ;
    private final String bindAddr;
    private final String identity;
    private final ZLogManager logMgr;
    private Socket worker ;

    public ZPFrontWorker (Context context, 
            String bindAddr, String identity, int cacheSize)
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
                state = TOPIC_RECEIVED;
                zlog = logMgr.get (topic);
                LOG.debug ("topic : " + topic);
                break;

            case TOPIC_RECEIVED:

                if (msg.size () == 0 && more) { // bottom
                    state = MESSAGE_BEGINS;
                    break;
                } 

            case MESSAGE_BEGINS:
                
                if (!store (zlog, msg)) {
                    stop = true;
                }
                if (!more)
                    state = START;
                break;
            }
        }
    }

    private boolean store (ZLog zlog, Msg msg)
    {
        try {
            zlog.append (msg);
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            LOG.error ("Failed to append msg", e);
            return false;
        }
    }
}
