/*  =========================================================================
    ZPReaderWorker - ZPER Reader worker 

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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import zmq.Msg;
import org.zper.ZPConstants;
import org.zper.ZPUtils;
import org.zper.base.Persistence;
import org.zper.base.ZLog;
import org.zper.base.ZLogManager;
import org.zper.base.ZLog.SegmentInfo;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQException;
import org.zeromq.ZMQ.Socket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZPReaderWorker extends Thread
{
    private static final Logger LOG = LoggerFactory.getLogger(ZPReaderWorker.class);

    // states
    private static final int START = 0;
    private static final int TOPIC = 1;
    private static final int COMMAND = 2;
    private static final int ARGUMENT = 3;

    private final ZContext context;
    private final String bindAddr;
    private final String identity;
    private final ZLogManager logMgr;
    private Socket worker;

    public ZPReaderWorker(ZContext context, String bindAddr, String identity)
    {
        this.context = ZContext.shadow(context);
        this.bindAddr = bindAddr;
        this.identity = identity;

        logMgr = ZLogManager.instance();
    }

    @Override
    public void run()
    {
        LOG.info("Started Worker " + identity);
        worker = context.createSocket(ZMQ.DEALER);
        worker.setIdentity(identity.getBytes());
        worker.connect(bindAddr);
        try {
            loop();
        } catch (ZMQException e) {
            if (e.getErrorCode() != ZMQ.Error.ETERM.getCode())
                throw e;
        }

        context.destroy();
        LOG.info("Ended Reader Worker " + identity);
    }

    public void loop()
    {
        int state = START;

        String topic = null;
        boolean more = false;
        boolean stop = false;
        ZLog zlog = null;
        Msg msg;
        String command = "";
        List<Msg> args = new ArrayList<Msg>();

        while (!Thread.currentThread().isInterrupted()
                && !stop) {

            msg = worker.base().recv(0);
            if (msg == null)
                break;
            more = msg.hasMore();

            switch (state) {
            case START:
                byte[] id = msg.data();
                if (id == null)
                    break;
                topic = ZPUtils.getTopic(id);
                state = TOPIC;
                zlog = logMgr.get(topic);

                worker.sendMore(msg.data());
                args.clear();
                break;

            case TOPIC:

                if (msg.size() == 0 && more) { // bottom
                    state = COMMAND;
                    worker.sendMore(msg.data());
                    break;
                }

            case COMMAND:

                command = new String(msg.data());
                state = ARGUMENT;
                break;

            case ARGUMENT:

                args.add(msg);
                if (!more) {
                    processCommand(zlog, command, args);
                    state = START;
                }
                break;
            }
        }
    }

    private void processCommand(ZLog zlog, String command, List<Msg> args)
    {
        if (command.equals(ZPConstants.COMMAND_FETCH)) {
            processFetch(zlog, args);
        } else if (command.equals(ZPConstants.COMMAND_OFFSET)) {
            processOffset(zlog, args);
        } else {
            code(ZPConstants.TYPE_RESPONSE, ZPConstants.STATUS_INVALID_COMMAND);
        }
    }

    private void processFetch(ZLog zlog, List<Msg> args)
    {
        if (args.size() != 2) {
            error();
            return;
        }
        long offset = args.get(0).buf().getLong();
        long size = args.get(1).buf().getLong();

        SegmentInfo info = zlog.segmentInfo(offset);

        if (info == null || info.flushedOffset() < offset) {
            LOG.info("No such segment for offset {}, or found {}",
                    offset, info == null ? -1 : info.flushedOffset());
            code(ZPConstants.TYPE_RESPONSE, ZPConstants.STATUS_INVALID_OFFSET);
            return;
        }

        assert (info.start() <= offset);

        code(ZPConstants.TYPE_FILE, ZPConstants.STATUS_OK);
        worker.sendMore(info.path());
        sendLong(offset - info.start(), true);
        if (info.flushedOffset() - offset < size)
            size = info.flushedOffset() - offset;
        sendLong(size, false);

        if (LOG.isDebugEnabled())
            LOG.debug("FETCH {} {} {}", new Object[] {offset, size, offset + size});

    }

    private void processOffset(ZLog zlog, List<Msg> args)
    {
        if (args.size() > 2) {
            error();
            return;
        }

        long timestamp = args.get(0).buf().getLong();

        long[] offsets;
        if (timestamp == ZLog.EARLIEST || timestamp == ZLog.LATEST)
            offsets = zlog.offsets(timestamp, 1);
        else {
            int maxEntries = Integer.MAX_VALUE;
            if (args.size() > 1)
                maxEntries = args.get(1).buf().getInt();
            offsets = zlog.offsets(timestamp, maxEntries);
        }

        if (offsets.length == 0) {
            code(ZPConstants.TYPE_RESPONSE, ZPConstants.STATUS_INVALID_OFFSET);
            return;
        }

        code(ZPConstants.TYPE_RESPONSE, ZPConstants.STATUS_OK);
        for (int i = 0; i < offsets.length - 1; i++) {
            sendLong(offsets[i], true);
        }
        sendLong(offsets[offsets.length - 1], false);
    }

    private void sendLong(long value, boolean more)
    {
        ByteBuffer buf = ByteBuffer.allocate(8).putLong(value);
        if (more)
            worker.sendMore(buf.array());
        else
            worker.send(buf.array());
    }

    private void error()
    {
        worker.send(new byte[] {Persistence.MESSAGE_ERROR});
    }

    private void code(int type, int code)
    {
        worker.sendMore(new byte[] {(byte) type});
        if (code == ZPConstants.STATUS_OK)
            worker.sendMore(new byte[] {(byte) code});
        else
            worker.send(new byte[] {(byte) code});
    }
}
