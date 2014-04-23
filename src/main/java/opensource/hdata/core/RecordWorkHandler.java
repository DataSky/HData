package opensource.hdata.core;

import opensource.hdata.config.PluginConfig;
import opensource.hdata.core.plugin.Reader;
import opensource.hdata.core.plugin.Writer;

import com.lmax.disruptor.WorkHandler;

public class RecordWorkHandler implements WorkHandler<RecordEvent> {

    private Reader[] readers;
    private Writer writer;
    private JobContext context;
    private PluginConfig writerConfig;
    private boolean writerPrepared;
    private boolean isWriterError;
    private Metric metric;

    public RecordWorkHandler(Reader[] readers, Writer writer, JobContext context, PluginConfig writerConfig) {
        this.readers = readers;
        this.writer = writer;
        this.context = context;
        this.writerConfig = writerConfig;
        this.metric = context.getMetric();
    }

    public void onEvent(RecordEvent event) {
        if (!isWriterError) {
            try {
                if (!writerPrepared) {
                    for (Reader reader : readers) {
                        if (context.getFields() == null) {
                            reader.declareOutputFields(context.getDeclarer());
                        } else {
                            break;
                        }
                    }
                    writer.prepare(context, writerConfig);
                    writerPrepared = true;

                    if (metric.getWriterStartTime() == 0) {
                        metric.setWriterStartTime(System.currentTimeMillis());
                    }
                }

                writer.execute(event.getRecord());
                metric.getWriteCount().incrementAndGet();
            } catch (Exception e) {
                this.isWriterError = true;
                context.setWriterError(true);
                e.printStackTrace();
            }
        }
    }

}
