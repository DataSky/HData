package opensource.hdata.core.plugin;

import opensource.hdata.config.PluginConfig;
import opensource.hdata.core.JobContext;
import opensource.hdata.core.OutputFieldsDeclarer;

public abstract class Reader extends AbstractPlugin {

    public void prepare(JobContext context, PluginConfig readerConfig) {
    }

    public void execute(RecordCollector recordCollector) {
    }

    public void close() {
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
}
