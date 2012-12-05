package org.zeromq.zper.perf;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

public class Message
{
    private byte [] data;
    public Message (String data)
    {
        this.data = data.getBytes ();
    }

    public long payloadSize ()
    {
        return 5L + 2L + data.length + (data.length > 255 ? 9L : 2L); 
    }

    public byte [] getHeader ()
    {
        ByteBuffer buf = ByteBuffer.allocate (5);
        buf.put ((byte) 0);
        
        CRC32 crc = new CRC32 ();
        crc.update (data);
        buf.putInt ((int) (0xffffffffL & crc.getValue ()));
        
        return buf.array ();
    }

    public byte [] getPayload ()
    {
        return data;
    }

}
