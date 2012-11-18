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
import java.util.Properties;
import java.util.UUID;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

public class ZPUtils
{
    public static void setup (String[] args, Class<? extends ZPServer> serverCls)
    {
        CommandLine cmd = null;
        Properties conf = new Properties ();

        Options options = new Options ();
        options.addOption ("c", true, "Load initial config from cmdline arg");
        options.addOption ("h", false, "Print help information");
        try {
            CommandLineParser parser = new PosixParser ();
            cmd = parser.parse (options, args);
        } catch (ParseException e) {
            help (serverCls.getName (), options);
            return;
        }

        if (cmd != null && cmd.hasOption ("h")) {
            help (serverCls.getName (), options);
            return;
        }
        
        if (cmd != null && cmd.hasOption ("c")) {
            try {
                BufferedReader reader = 
                        new BufferedReader (new FileReader (cmd.getOptionValue ("c")));
                conf.load(reader);
            } catch (IOException e) {
                e.printStackTrace ();
                System.exit (1);
            }
        }
        
        try {
            final ZPServer server = serverCls.getConstructor (conf.getClass ()).newInstance (conf);
            
            Runtime.getRuntime().addShutdownHook (new Thread ("shutdownHook") {

                @Override
                public void run () {
                  server.shutdown ();
                }

              });

            server.start ();

        } catch (Exception e) {
            e.printStackTrace ();
        }
    }
    
    
    private static void help (String name, Options options) 
    {
        HelpFormatter fmt = new HelpFormatter ();
          fmt.printHelp (name, options, true);
    }

    public static byte [] genTopicIdentity (String topic) 
    {
        byte [] identity = new byte [1 + 1 + topic.length () + 16];
        ByteBuffer buf = ByteBuffer.wrap (identity);
        UUID uuid = UUID.randomUUID ();
        
        buf.put ((byte) topic.hashCode ());
        buf.put ((byte) topic.length ());
        buf.put (topic.getBytes ());
        buf.putLong (uuid.getMostSignificantBits ());
        buf.putLong (uuid.getLeastSignificantBits ());
        
        return identity;
    }

}
