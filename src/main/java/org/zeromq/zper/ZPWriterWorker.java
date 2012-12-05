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
import java.nio.ByteBuffer;

import org.zeromq.zper.base.ZLog;
import org.zeromq.zper.base.ZLogManager;
import org.jeromq.ZMQ;
import org.jeromq.ZMQ.Context;
import org.jeromq.ZMQ.Msg;
import org.jeromq.ZMQ.Socket;
import org.jeromq.ZMQException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZPWriterWorker extends Thread
{
    private static final Logger LOG = LoggerFactory.getLogger (ZPWriterWorker.class);
    
    // states
    private static final int START = 0;
    private static final int TOPIC = 1;
    private static final int COUNT = 2;
    private static final int MESSAGE = 3;
    
    private final Context context ;
    private final String bindAddr;
    private final String identity;
    private final ZLogManager logMgr;
    private Socket worker ;

    public ZPWriterWorker (Context context, String bindAddr, String identity)
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
        worker.setRcvHWM (2000);
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
        int flag = 0;
        int count = 0;
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
                flag = id [1];
                int tsize = id [2];
                topic = new String (id, 3, tsize);
                state = TOPIC;
                zlog = logMgr.get (topic);
                break;

            case TOPIC:

                if (msg.size () == 0 && more) { // bottom
                    state = COUNT;
                    break;
                } 
            case COUNT:
                count = ByteBuffer.wrap (msg.data ()).getInt ();
                state = MESSAGE;
                
                break;

            case MESSAGE:
                
                if (!store (zlog, count, msg)) {
                    stop = true;
                }
                if (!more)
                    state = START;
                break;
            }
            msg = null;
        }
    }

    private boolean store (ZLog zlog, int count, Msg msg)
    {
        try {
            zlog.appendBulk (count, msg);
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            LOG.error ("Failed to append msg", e);
            return false;
        }
    }
}
