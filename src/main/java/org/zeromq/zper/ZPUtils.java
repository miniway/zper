/*  =========================================================================
    ZPUtils - ZPER utility class 

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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Properties;
import java.util.UUID;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.jeromq.ZMQ.Msg;
import org.zeromq.zper.base.MsgIterator;
import org.zeromq.zper.base.ZLog;
import org.zeromq.zper.base.ZLog.SegmentInfo;
import org.zeromq.zper.base.ZLogManager;
import org.zeromq.zper.base.ZLogManager.ZLogConfig;

public class ZPUtils
{
    public static Properties setup (String[] args)
    {
        CommandLine cmd = null;
        Properties conf = new Properties ();

        Options options = new Options ();
        options.addOption ("h", false, "Print help information");
        try {
            CommandLineParser parser = new PosixParser ();
            cmd = parser.parse (options, args);
        } catch (ParseException e) {
            help ("java class config_file", options);
            System.exit (-1);
        }

        if (args.length < 1 || (cmd != null && cmd.hasOption ("h"))) {
            help ("java class config_file", options);
            System.exit (-1);
        }
        
        try {
            BufferedReader reader = 
                    new BufferedReader (new FileReader (args [0]));
            conf.load(reader);
        } catch (IOException e) {
            e.printStackTrace ();
            System.exit (1);
        }
        
        return conf;
        
    }
    
    
    public static void help (String name, Options options) 
    {
        HelpFormatter fmt = new HelpFormatter ();
        fmt.printHelp (name, options, true);
    }

    synchronized public static byte [] genTopicIdentity (String topic, int flag) 
    {
        byte [] identity = new byte [1 + 2 + topic.length () + 16];
        ByteBuffer buf = ByteBuffer.wrap (identity);
        UUID uuid = UUID.randomUUID ();
        
        buf.put ((byte) topic.hashCode ());
        buf.put ((byte) flag);
        buf.put ((byte) topic.length ());
        buf.put (topic.getBytes ());
        buf.putLong (uuid.getMostSignificantBits ());
        buf.putLong (uuid.getLeastSignificantBits ());
        
        return identity;
    }
    
    public static String getTopic (byte [] identity)
    {
        int tsize = identity [2];
        return new String (identity, 3, tsize);
    }

    public static void main (String [] argv) throws Exception
    {
        String cmd = argv [0];
        
        if (cmd.equals ("count"))
            countMsg (argv);
    }
    
    private static void countMsg (String [] argv) throws Exception
    {
        ZLogConfig zc = ZLogManager.instance ().config ();
        zc.set ("base_dir", argv [1]);
        
        ZLog log = ZLogManager.instance ().get (argv [2]);
        SegmentInfo [] segments = log.segments ();
        long total = 0;
        
        for (SegmentInfo seg: segments) {
            int count = log.countMsg (seg.start (), seg.offset () - seg.start ());
            System.out.println (String.format ("%020d : %d", seg.start (), count));
            total += count;
        }
        System.out.println ("Total : " + total);
    }


    public static Iterator <Msg> iterator (Msg msg)
    {
        return new MsgIterator (msg.buf ());
    }
}
