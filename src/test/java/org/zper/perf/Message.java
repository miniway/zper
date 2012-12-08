package org.zper.perf;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

public class Message
{
    private byte [] header;
    private byte [] data;
    
    public Message (String data)
    {
        header = null;
        this.data = data.getBytes ();
    }
    
    public Message (byte [] header, byte [] data)
    {
        this.header = header;
        this.data = data;
    }

    public long payloadSize ()
    {
        return header.length + data.length; 
    }

    public byte [] getHeader ()
    {
        if (header == null) {
            ByteBuffer buf = ByteBuffer.allocate (5);
            buf.put ((byte) 0);
            
            CRC32 crc = new CRC32 ();
            crc.update (data);
            buf.putInt ((int) (0xffffffffL & crc.getValue ()));
            
            header = buf.array ();
        }
        return header;
    }

    public byte [] getPayload ()
    {
        return data;
    }

    public long validBytes ()
    {
        return header.length + 2L + data.length + (data.length > 255 ? 9L : 2L); 

    }

}
