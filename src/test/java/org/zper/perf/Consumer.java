package org.zper.perf;

import java.util.ArrayList;
import java.util.List;

import zmq.Msg;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;
import org.zper.MsgIterator;
import org.zper.ZPUtils;

public class Consumer
{
    private ZContext ctx;
    private Socket sock;

    public Consumer(String addr, String topic, int bufsize)
    {
        ctx = new ZContext();
        ctx.setLinger(-1);
        sock = ctx.createSocket(ZMQ.DEALER);
        sock.setReceiveBufferSize(bufsize);
        sock.setIdentity(ZPUtils.genTopicIdentity(topic, 0));
        sock.connect("tcp://" + addr);
    }

    public long[] getOffsetsBefore(String topic, int ts, int entry)
    {
        sock.sendMore("OFFSET");
        sock.sendMore(ZPUtils.getBytes(ts));
        sock.send(ZPUtils.getBytes(entry));

        sock.recv(); // status

        long[] offsets = new long[entry];
        int idx = 0;
        while (sock.hasReceiveMore()) {
            if (idx < entry)
                offsets[idx] = ZPUtils.getLong(sock.recv());
            idx++;
        }

        return offsets;
    }

    public List<Message> fetch(long offset, long fetchSize)
    {

        sock.sendMore("FETCH");
        sock.sendMore(ZPUtils.getBytes(offset));
        sock.send(ZPUtils.getBytes(fetchSize));

        int status = ZPUtils.getInt(sock.recv()); // status

        ArrayList<Message> result = new ArrayList<Message>();

        if (status != 100) {
            // no more data
            return result;
        }

        MsgIterator it = ZPUtils.iterator(sock.base().recv(0));

        while (it.hasNext()) {
            Msg header = it.next();
            if (it.hasNext()) {
                Msg data = it.next();
                result.add(new Message(header.data(), data.data()));
            } else
                break;
        }

        return result;
    }

}
