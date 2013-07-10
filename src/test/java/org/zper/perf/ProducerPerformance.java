package org.zper.perf;

import java.text.SimpleDateFormat;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zper.ZPUtils;

public class ProducerPerformance
{
    protected static class ProducerPerfConfig
    {
        private String topic;
        private Long numMessages;
        private Integer reportingInterval;
        private boolean showDetailedStats;
        private SimpleDateFormat dateFormat;
        private boolean hideHeader;
        private String writerInfo;
        private String readerInfo;
        private int messageSize;
        private boolean isFixSize;
        private int numThreads;
        private int compressionCodec;
        private int batchSize = -1;

        protected ProducerPerfConfig(String[] args)
        {
            CommandLine cmd = null;
            Options options = new Options();
            Option opt;
            opt = new Option("t", "topic", true, "REQUIRED: The topic to consume from.");
            opt.setRequired(true);
            options.addOption(opt);

            opt = new Option("w", "writerinfo", true, "REQUIRED: ZPWriter info (comma seperated host:port)");
            opt.setRequired(true);
            options.addOption(opt);

            opt = new Option("r", "readerinfo", true, "ZPReader info (comma seperated host:port)");
            //opt.setRequired (true);
            options.addOption(opt);

            opt = new Option("n", "messages", true, "REQUIRED: The number of messages to send or consume");
            opt.setRequired(true);
            options.addOption(opt);

            options.addOption(new Option("s", "message-size", true, "The size of each message.")); // 100
            options.addOption(new Option("v", "vary-message-size", false, "If set, message size will vary up to the given maximum."));
            options.addOption(new Option("c", "threads", true, "Number of sending threads."));  // 10
            options.addOption(new Option("i", "reporting-interval", true, "Interval at which to print progress info.")); // 5000
            options.addOption(new Option("x", "show-detailed-stats", false, "If set, stats are reported for each reporting "
                    + "interval as configured by reporting-interval"));
            options.addOption(new Option("h", "hide-header", false, "If set, skips printing the header for the stats"));
            options.addOption(new Option("d", "date-format", true, "The date format to use for formatting the time field. "
                    + "See java.text.SimpleDateFormat for options."));  // yyyy-MM-dd HH:mm:ss:SSS
            options.addOption(new Option("z", "compression-codec", true, "If set, messages are sent compressed"));  // 0

            try {
                CommandLineParser parser = new PosixParser();
                cmd = parser.parse(options, args);
            } catch (ParseException e) {
                ZPUtils.help(ProducerPerformance.class.getName(), options);
                System.exit(-1);
            }

            topic = cmd.getOptionValue("topic");
            numMessages = Long.valueOf(cmd.getOptionValue("messages"));
            reportingInterval = Integer.valueOf(cmd.getOptionValue("reporting-interval", "5000"));
            showDetailedStats = cmd.hasOption("show-detailed-stats");
            dateFormat = new SimpleDateFormat(cmd.getOptionValue("date-format", "yyyy-MM-dd HH:mm:ss:SSS"));
            hideHeader = cmd.hasOption("hide-header");
            writerInfo = cmd.getOptionValue("writerinfo");
            readerInfo = cmd.getOptionValue("readerinfo", "");
            messageSize = Integer.valueOf(cmd.getOptionValue("message-size", "100"));
            isFixSize = !cmd.hasOption("vary-message-size");
            numThreads = Integer.valueOf(cmd.getOptionValue("threads", "1"));
            compressionCodec = Integer.valueOf(cmd.getOptionValue("compression-codec", "0"));
        }
    }

    private static class ProducerThread implements Runnable
    {
        private ProducerPerfConfig config;
        private Random rand;
        private Producer producer;
        private CountDownLatch allDone;
        private AtomicLong totalMessagesSent;
        private AtomicLong totalBytesSent;
        private int threadId;

        protected ProducerThread(int threadId,
                                 ProducerPerfConfig config,
                                 AtomicLong totalBytesSent,
                                 AtomicLong totalMessagesSent,
                                 CountDownLatch allDone,
                                 Random rand)
        {
            this.config = config;
            this.rand = rand;
            this.allDone = allDone;
            this.totalBytesSent = totalBytesSent;
            this.totalMessagesSent = totalMessagesSent;
            this.threadId = threadId;

            Properties props = new Properties();
            props.put("writer.list", config.writerInfo);
            props.put("reader.list", config.readerInfo);

            props.put("compression.codec", config.compressionCodec);
            props.put("reconnect.interval", String.valueOf(Integer.MAX_VALUE));
            props.put("buffer.size", String.valueOf(64 * 1024));

            producer = new Producer(config.topic, props);
        }

        @Override
        public void run()
        {
            long bytesSent = 0L;
            int nSends = 0;
            int lastNSends = 0;
            long lastBytesSent = 0L;
            Message message = new Message(getStringOfLength(config.messageSize));
            long reportTime = System.currentTimeMillis();
            long lastReportTime = reportTime;
            long messagesPerThread = config.numMessages / config.numThreads;

            for (int k = 0; k < messagesPerThread; k++) {
                if (!config.isFixSize)
                    message = new Message(getStringOfLength(rand.nextInt(config.messageSize)));
                producer.send(message);
                bytesSent += message.payloadSize();
                nSends += 1;

                if (nSends % config.reportingInterval == 0) {
                    reportTime = System.currentTimeMillis();
                    double elapsed = (reportTime - lastReportTime) / 1000.0;
                    double mbBytesSent = ((bytesSent - lastBytesSent) * 1.0) / (1024 * 1024);
                    double numMessagesPerSec = (nSends - lastNSends) / elapsed;
                    double mbPerSec = mbBytesSent / elapsed;
                    String formattedReportTime = config.dateFormat.format(reportTime);
                    if (config.showDetailedStats)
                        System.out.println(String.format("%s, %d, %d, %d, %d, %.2f, %.4f, %d, %.4f",
                                formattedReportTime, config.compressionCodec,
                                threadId, config.messageSize, config.batchSize, (bytesSent * 1.0) / (1024 * 1024), mbPerSec, nSends, numMessagesPerSec));
                    lastReportTime = reportTime;
                    lastBytesSent = bytesSent;
                    lastNSends = nSends;
                }
            }
            producer.close();
            totalBytesSent.addAndGet(bytesSent);
            totalMessagesSent.addAndGet(nSends);
            allDone.countDown();
        }
    }

    public static void main(String[] args) throws Exception
    {
        Logger logger = LoggerFactory.getLogger(ProducerPerformance.class);
        ProducerPerfConfig config = new ProducerPerfConfig(args);
        if (!config.isFixSize)
            logger.info("WARN: Throughput will be slower due to changing message size per request");

        AtomicLong totalBytesSent = new AtomicLong(0);
        AtomicLong totalMessagesSent = new AtomicLong(0);
        ExecutorService executor = Executors.newFixedThreadPool(config.numThreads);
        CountDownLatch allDone = new CountDownLatch(config.numThreads);
        long startMs = System.currentTimeMillis();
        Random rand = new java.util.Random();

        if (!config.hideHeader) {
            if (!config.showDetailedStats)
                System.out.println("start.time, end.time, compression, message.size, batch.size, total.data.sent.in.MB, MB.sec, " +
                        "total.data.sent.in.nMsg, nMsg.sec");
            else
                System.out.println("time, compression, thread.id, message.size, batch.size, total.data.sent.in.MB, MB.sec, " +
                        "total.data.sent.in.nMsg, nMsg.sec");
        }

        for (int i = 0; i < config.numThreads; i++) {
            executor.execute(new ProducerThread(i, config, totalBytesSent, totalMessagesSent, allDone, rand));
        }

        allDone.await();
        long endMs = System.currentTimeMillis();
        double elapsedSecs = (endMs - startMs) / 1000.0;
        if (!config.showDetailedStats) {
            double totalMBSent = (totalBytesSent.get() * 1.0) / (1024 * 1024);
            System.out.printf("%s, %s, %d, %d, %d, %.2f, %.4f, %d, %.4f\n",
                    config.dateFormat.format(startMs),
                    config.dateFormat.format(endMs), config.compressionCodec, config.messageSize, config.batchSize,
                    totalMBSent, totalMBSent / elapsedSecs, totalMessagesSent.get(), totalMessagesSent.get() / elapsedSecs);
        }
        System.exit(0);
    }

    private static String getStringOfLength(int len)
    {
        byte[] strArray = new byte[len];
        for (int i = 0; i < len; i++)
            strArray[i] = 'x';
        return new String(strArray);
    }

}
