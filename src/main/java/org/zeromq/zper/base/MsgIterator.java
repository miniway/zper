/*  =========================================================================
    MsgIterator - ZPER Message Iterator 

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
package org.zeromq.zper.base;

import java.nio.ByteBuffer;
import java.util.Iterator;

import org.jeromq.ZMQ.Msg;

public class MsgIterator implements Iterator <Msg>
{
    private ByteBuffer buf;
    private int length;
    private int flag;
    
    public MsgIterator (ByteBuffer buf)
    {
        this.buf = buf;
    }

    @Override
    public boolean hasNext ()
    {
        if (buf.remaining() < 2)
            return false;

        long longLength;
        
        flag = buf.get();
        if (flag > 3)
            return false;
        
        if ((flag & 0x02) == 0) {   //  Short length  
            length = buf.get ();
            if (length < 0)
                length = (0xFF) & length;
        } else {                    //  Long length
            
            if (8 > buf.remaining())
                return false;

            longLength = buf.getLong();
            if (longLength < 255 || longLength > Integer.MAX_VALUE)
                return false;
            
            length = (int) longLength;
        }
        
        if (length == 0 && flag == 0)
            return false;
        
        if (length > buf.remaining())
            return false;

        return true;
    }

    @Override
    public Msg next ()
    {
        int limit = buf.limit ();
        buf.limit (buf.position () + length);
        Msg msg = new Msg (buf.slice ());
        if ((flag & Msg.MORE) > 0)
            msg.setFlags (Msg.MORE);
        
        buf.limit (limit);
        buf.position (buf.position () + length);
        return msg;
    }

    @Override
    public void remove ()
    {
    }

}
