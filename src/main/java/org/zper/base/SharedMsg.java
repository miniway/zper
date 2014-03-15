/*
 * =========================================================================
 *     SharedMsg.java
 *
 *     -------------------------------------------------------------------------
 *     Copyright (c) 2012-2013 InfiniLoop Corporation
 *     Copyright other contributors as noted in the AUTHORS file.
 *
 *     This file is part of Zyni, an open-source message based application framework.
 *
 *     This is free software; you can redistribute it and/or modify it under
 *     the terms of the GNU Lesser General Public License as published by the
 *     Free Software Foundation; either version 3 of the License, or (at your
 *     option) any later version.
 *
 *     This software is distributed in the hope that it will be useful, but
 *     WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTA-
 *     BILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
 *     Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program. If not, see http://www.gnu.org/licenses/.
 *     =========================================================================
 */

package org.zper.base;

import org.zper.MsgIterator;
import zmq.Msg;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class SharedMsg
        extends Msg
{
    private final ByteBuffer buffer;
    private final long start;
    private final long end;

    private final int count;
    private final int size;
    private final int capacity;

    private final AtomicLong threshold;
    private static AtomicLong total = new AtomicLong(0);

    public SharedMsg(AtomicLong threshold, ByteBuffer buffer, long start, long end, int count)
    {
        this.threshold = threshold;

        this.buffer = buffer;
        this.start = start;
        this.end = end;

        this.count = count;
        this.size = (int) (end - start);
        this.capacity = buffer.capacity();

        assert (start != end);
    }

    @Override
    public int size()
    {
        return size;
    }

    @Override
    public ByteBuffer buf()
    {
        threshold.set(start);

        int position = (int) (start % capacity);
        int limit = (int) (end % capacity);

        ByteBuffer payload = buffer.duplicate();

        if (limit > position) {
            payload.limit(limit);
            payload.position(position);
            //check(payload);

            return payload;
        } else {
            ByteBuffer copied = ByteBuffer.allocate(capacity - position + limit);
            payload.limit(capacity);
            payload.position(position);
            copied.put(payload);

            payload.position(0);
            payload.limit(limit);
            copied.put(payload);

            copied.flip();

            //check(copied);

            return copied;
        }
    }

    private void check(ByteBuffer buf)
    {
        assert (size == buf.remaining());
        MsgIterator mi = new MsgIterator(buf.duplicate());

        int msgCount = 0;
        while (mi.hasNext()) {
            msgCount++;
            mi.next();
        }

        assert (count == msgCount);
        total.getAndAdd(count);
        System.out.println(total);
    }

}
