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

import org.zeromq.zper.base.MsgIterator;
import org.zeromq.zper.base.ZLog;
import org.zeromq.zper.base.ZLogManager;
import org.jeromq.ZContext;
import org.jeromq.ZMQ;
import org.jeromq.ZMQ.Msg;
import org.jeromq.ZMQ.Socket;
import org.jeromq.ZMQException;
import org.jeromq.ZMsg;
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
    
    private final ZContext context ;
    private final String bindAddr;
    private final String identity;
    private final ZLogManager logMgr;
    private Socket worker ;

    public ZPWriterWorker (ZContext context, String bindAddr, String identity)
    {
        this.context = ZContext.shadow (context);
        this.bindAddr = bindAddr;
        this.identity = identity;

        logMgr = ZLogManager.instance();
    }

    @Override
    public void run ()
    {
        LOG.info ("Started Worker " + identity);
        worker = context.createSocket (ZMQ.DEALER);
        worker.setRcvHWM (2000);
        worker.setIdentity (identity);
        worker.connect (bindAddr);
        try {
            loop();
        } catch (ZMQException.CtxTerminated e) {
        }

        LOG.info("Ended Writer Worker " + identity);
        context.destroy ();
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
        ZMsg response = null;

        while (!Thread.currentThread ().isInterrupted ()
                && !stop) {

            msg = worker.recvMsg (0);
            if (msg == null)
                break;
            more = msg.hasMore ();

            switch (state) {
            case START:
                byte [] id = msg.data ();
                if (id == null)
                    break;
                flag = id [1];
                topic = ZPUtils.getTopic (id);
                state = TOPIC;
                zlog = logMgr.get (topic);
                
                if (flag > 0) {
                    response = new ZMsg ();
                    response.add (id);
                }                
                break;

            case TOPIC:

                if (msg.size () == 0 && more) { // bottom
                    state = COUNT;
                    
                    if (flag > 0)
                        response.add (msg.data ());
                    break;
                }
                
            case COUNT:
                count = ByteBuffer.wrap (msg.data ()).getInt ();
                state = MESSAGE;
                
                break;

            case MESSAGE:
                
                if (store (zlog, count, msg)) {
                    if (flag > 0 && zlog.flushed ()) {
                        response.add (getLastFrame (msg.buf ().duplicate ()));
                        response.send (worker);
                    }
                } else 
                    stop = true;
                if (!more)
                    state = START;
                break;
            }
            msg = null;
        }
    }

    private byte [] getLastFrame (ByteBuffer buf)
    {
        buf.rewind ();
        MsgIterator it = new MsgIterator (buf);
        it.hasNext ();
        return it.next ().data ();
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
