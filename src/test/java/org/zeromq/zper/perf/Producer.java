package org.zeromq.zper.perf;

import java.util.Properties;

import org.jeromq.ZContext;
import org.jeromq.ZMQ;
import org.jeromq.ZMQ.Socket;
import org.zeromq.zper.ZPUtils;

public class Producer
{
    private ZContext ctx;
    private Socket sock;
    
    public Producer (String topic, Properties props)
    {
        ctx = new ZContext ();
        ctx.setLinger (-1);
        sock = ctx.createSocket (ZMQ.DEALER);
        sock.setIdentity (ZPUtils.genTopicIdentity (topic, 0));
        for (String addr: props.getProperty ("writer.list").split (",")) {
            sock.connect ("tcp://" + addr);
        }
    }

    public void send (Message message)
    {
        boolean rc;
        rc = sock.sendMore (message.getHeader ());
        if (!rc)
            assert (false);
        rc = sock.send (message.getPayload ());
        if (!rc)
            assert (false);

    }

    public void close ()
    {
        ctx.destroy ();
    }

}
