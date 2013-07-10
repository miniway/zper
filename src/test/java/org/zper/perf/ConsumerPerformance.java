package org.zper.perf;

import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.zper.ZPUtils;

public class ConsumerPerformance
{
    public class MessageAndMetadata
    {

        public Message message;

    }

    public class Stream<T> implements Iterable<MessageAndMetadata>
    {

        @Override
        public Iterator<MessageAndMetadata> iterator()
        {
            return null;
        }

    }

    public static class ConsumerPerfConfig
    {
        private String topic;
        private Long numMessages;
        private Integer reportingInterval;
        private boolean showDetailedStats;
        private SimpleDateFormat dateFormat;
        private boolean hideHeader;
        private String readerInfo;
        private int fetchSize;
        public Properties props = new Properties();
        public boolean fromLatest;

        protected ConsumerPerfConfig(String[] args)
        {
            CommandLine cmd = null;
            Options options = new Options();
            Option opt;
            opt = new Option("t", "topic", true, "REQUIRED: The topic to consume from.");
            opt.setRequired(true);
            options.addOption(opt);

            opt = new Option("r", "readerinfo", true, "REQUIRED: ZPReader info (comma seperated host:port)");
            opt.setRequired(true);
            options.addOption(opt);

            opt = new Option("n", "messages", true, "REQUIRED: The number of messages to send or consume");
            opt.setRequired(true);
            options.addOption(opt);

            options.addOption(new Option("s", "fetch-size", true, "The size of each message.")); // 100
            options.addOption(new Option("v", "vary-message-size", false, "If set, message size will vary up to the given maximum."));
            options.addOption(new Option("c", "threads", true, "Number of sending threads."));  // 10
            options.addOption(new Option("i", "reporting-interval", true, "Interval at which to print progress info.")); // 5000
            options.addOption(new Option("x", "show-detailed-stats", false, "If set, stats are reported for each reporting "
                    + "interval as configured by reporting-interval"));
            options.addOption(new Option("h", "hide-header", false, "If set, skips printing the header for the stats"));
            options.addOption(new Option("d", "date-format", true, "The date format to use for formatting the time field. "
                    + "See java.text.SimpleDateFormat for options."));  // yyyy-MM-dd HH:mm:ss:SSS

            try {
                CommandLineParser parser = new PosixParser();
                cmd = parser.parse(options, args);
            } catch (ParseException e) {
                ZPUtils.help(ConsumerPerformance.class.getName(), options);
                System.exit(-1);
            }

            topic = cmd.getOptionValue("topic");
            numMessages = Long.valueOf(cmd.getOptionValue("messages"));
            reportingInterval = Integer.valueOf(cmd.getOptionValue("reporting-interval", "5000"));
            showDetailedStats = cmd.hasOption("show-detailed-stats");
            dateFormat = new SimpleDateFormat(cmd.getOptionValue("date-format", "yyyy-MM-dd HH:mm:ss:SSS"));
            hideHeader = cmd.hasOption("hide-header");
            readerInfo = cmd.getOptionValue("readerinfo", "");
            fetchSize = Integer.valueOf(cmd.getOptionValue("fetch-size", "1048576"));

            props.put("reader.list", readerInfo);

            props.put("reconnect.interval", String.valueOf(Integer.MAX_VALUE));
            props.put("fetch.size", fetchSize);

        }
    }

    public static void main(String[] args) throws Exception
    {
        ConsumerPerfConfig config = new ConsumerPerfConfig(args);

        long startMs = System.currentTimeMillis();
        long totalMessagesRead = 0L;
        long totalBytesRead = 0L;

        if (!config.hideHeader) {
            if (!config.showDetailedStats)
                System.out.println("start.time, end.time, fetch.size, data.consumed.in.MB, MB.sec, data.consumed.in.nMsg, nMsg.sec");
            else
                System.out.println("time, fetch.size, data.consumed.in.MB, MB.sec, data.consumed.in.nMsg, nMsg.sec");
        }

        long messagesRead = 0L;
        long lastMessagesRead = 0L;
        long bytesRead = 0L;
        long lastBytesRead = 0L;
        long lastReportTime = startMs;

        Consumer consumer = new Consumer(config.readerInfo, config.topic, 2 * config.fetchSize);

        long offset;

        if (config.fromLatest)
            offset = consumer.getOffsetsBefore(config.topic, -1, 1)[0];
        else
            offset = consumer.getOffsetsBefore(config.topic, -2, 1)[0];
        long consumedInterval = 0L;
        boolean done = false;
        while (!done) {
            List<Message> messages = consumer.fetch(offset, config.fetchSize);
            messagesRead = 0;
            bytesRead = 0;

            for (Message message : messages) {
                messagesRead += 1;
                bytesRead += message.payloadSize();
                offset += message.validBytes();
            }

            totalMessagesRead += messagesRead;

            if (messagesRead == 0 || totalMessagesRead > config.numMessages)
                done = true;

            totalBytesRead += bytesRead;
            consumedInterval += messagesRead;

            if (consumedInterval > config.reportingInterval) {
                if (config.showDetailedStats) {
                    long reportTime = System.currentTimeMillis();
                    double elapsed = (reportTime - lastReportTime) / 1000.0;
                    double totalMBRead = ((totalBytesRead - lastBytesRead) * 1.0) / (1024 * 1024);
                    System.out.printf("%s, %d, %.4f, %.4f, %d, %.4f\n",
                            config.dateFormat.format(reportTime), config.fetchSize,
                            (totalBytesRead * 1.0) / (1024 * 1024), totalMBRead / elapsed,
                            totalMessagesRead, (totalMessagesRead - lastMessagesRead) / elapsed);
                }
                lastReportTime = System.currentTimeMillis();
                lastBytesRead = totalBytesRead;
                lastMessagesRead = totalMessagesRead;
                consumedInterval = 0;
            }
        }

        long reportTime = System.currentTimeMillis();
        long elapsedMs = (reportTime - startMs);


        if (!config.showDetailedStats) {
            double totalMBRead = (bytesRead * 1.0) / (1024 * 1024);
            System.out.printf("%s, %s, %d, %.4f, %.4f, %d, %.4f\n",
                    config.dateFormat.format(startMs),
                    config.dateFormat.format(reportTime),
                    config.fetchSize,
                    totalMBRead, 1000.0 * (totalMBRead / elapsedMs),
                    totalMessagesRead, (totalMessagesRead / elapsedMs) * 1000.0);
        }
        System.exit(0);
    }

}
