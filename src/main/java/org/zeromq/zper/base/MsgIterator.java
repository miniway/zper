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
