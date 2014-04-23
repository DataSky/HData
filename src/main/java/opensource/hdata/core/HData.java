package opensource.hdata.core;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import opensource.hdata.common.HDataConfigConstants;
import opensource.hdata.config.EngineConfig;
import opensource.hdata.config.JobConfig;
import opensource.hdata.config.PluginConfig;
import opensource.hdata.core.plugin.Reader;
import opensource.hdata.core.plugin.RecordCollector;
import opensource.hdata.core.plugin.Splitter;
import opensource.hdata.core.plugin.Writer;
import opensource.hdata.exception.HDataException;
import opensource.hdata.util.Utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

public class HData {

    private DecimalFormat df = new DecimalFormat("#0.00");
    private static final Logger LOG = LogManager.getLogger(HData.class);

    public void start(final JobConfig jobConfig) {
        final PluginConfig readerConfig = jobConfig.getReaderConfig();
        final PluginConfig writerConfig = jobConfig.getWriterConfig();

        LOG.info("Reader: {}, Writer: {}", jobConfig.getReaderName(), jobConfig.getWriterName());
        int writerParallelism = writerConfig.getParallelism();

        final JobContext context = new JobContext();
        context.setJobConfig(jobConfig);
        final Metric metric = new Metric();
        context.setMetric(metric);
        final OutputFieldsDeclarer outputFieldsDeclarer = new OutputFieldsDeclarer(context);
        context.setDeclarer(outputFieldsDeclarer);

        final EngineConfig engineConfig = EngineConfig.create();
        context.setEngineConfig(engineConfig);

        long sleepMillis = engineConfig.getLong(HDataConfigConstants.HDATA_SLEEP_MILLIS, 3000);

        List<PluginConfig> readerConfigList = null;
        Splitter spliter = jobConfig.newSplitter();
        if (spliter != null) {
            LOG.info("Executing spliter for reader.");
            readerConfigList = spliter.split(jobConfig);
            if (readerConfigList == null || readerConfigList.size() == 0) {
                LOG.info("Job Finished.");
                System.exit(0);
            }
        } else if (readerConfig.getParallelism() > 1) {
            throw new HDataException("Reader parallelism is " + readerConfig.getParallelism() + ", but can not find splitter.");
        } else {
            readerConfigList = new ArrayList<PluginConfig>();
            readerConfigList.add(readerConfig);
        }

        Reader[] readers = new Reader[readerConfigList.size()];
        for (int i = 0, len = readers.length; i < len; i++) {
            readers[i] = jobConfig.newReader();
        }

        LOG.info("Reader parallelism: {}, Writer parallelism: {}", readers.length, writerParallelism);

        final Writer[] writers = new Writer[writerParallelism];
        final RecordWorkHandler[] handlers = new RecordWorkHandler[writerParallelism];
        for (int i = 0; i < writerParallelism; i++) {
            writers[i] = jobConfig.newWriter();
            handlers[i] = new RecordWorkHandler(readers, writers[i], context, writerConfig);
        }

        int bufferSize = engineConfig.getInt(HDataConfigConstants.STORAGE_BUFFER_SIZE, 1024);
        String WaitStrategyName = engineConfig.getString(HDataConfigConstants.HDATA_STORAGE_DISRUPTOR_WAIT_STRATEGY, "BlockingWaitStrategy");

        Storage storage = createStorage(bufferSize, WaitStrategyName, readers.length, handlers);
        context.setStorage(storage);
        RecordCollector rc = new RecordCollector(storage, metric);

        LOG.info("Transfering data from reader to writer...");
        ExecutorService es = Executors.newFixedThreadPool(readers.length);
        for (int i = 0, len = readerConfigList.size(); i < len; i++) {
            es.submit(new ReaderWorker(readers[i], context, readerConfigList.get(i), rc));
        }
        es.shutdown();

        metric.setReaderStartTime(System.currentTimeMillis());
        metric.setWriterStartTime(System.currentTimeMillis());
        while (!es.isTerminated()) {
            Utils.sleep(sleepMillis);
            LOG.info("Read: {}\tWrite: {}", metric.getReadCount().get(), metric.getWriteCount().get());
        }
        metric.setReaderEndTime(System.currentTimeMillis());

        while (!storage.isEmpty()) {
            if (context.isWriterError()) {
                LOG.error("Write error.");
                break;
            }
            Utils.sleep(sleepMillis);
            LOG.info("Read Finished(total: {}), Write: {}", metric.getReadCount().get(), metric.getWriteCount().get());
        }
        storage.close();
        LOG.info("Read Finished(total: {}), Write Finished(total: {})", metric.getReadCount().get(), metric.getWriteCount().get());

        metric.setWriterEndTime(System.currentTimeMillis());
        for (Writer writer : writers) {
            writer.close();
        }

        double readSeconds = (metric.getReaderEndTime() - metric.getReaderStartTime()) / 1000d;
        double writeSeconds = (metric.getWriterEndTime() - metric.getWriterStartTime()) / 1000d;
        String readSpeed = df.format(metric.getReadCount().get() / readSeconds);
        String writeSpeed = df.format(metric.getWriteCount().get() / writeSeconds);
        LOG.info("Read spent time: {}s, Write spent time: {}s", df.format(readSeconds), df.format(writeSeconds));
        LOG.info("Read records: {}/s, Write records: {}/s", readSpeed, writeSpeed);
    }

    private Storage createStorage(int bufferSize, String WaitStrategyName, int producerCount, RecordWorkHandler[] handlers) {
        WaitStrategy waitStrategy = WaitStrategyFactory.build(WaitStrategyName);
        ExecutorService executorService = Executors.newCachedThreadPool();
        ProducerType producerType;
        if (producerCount == 1) {
            producerType = ProducerType.SINGLE;
        } else {
            producerType = ProducerType.MULTI;
        }
        Disruptor<RecordEvent> disruptor = new Disruptor<RecordEvent>(RecordEvent.FACTORY, bufferSize, executorService, producerType, waitStrategy);
        Storage storage = new Storage(disruptor, handlers);
        executorService.shutdown();
        return storage;
    }

}
