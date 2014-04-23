package opensource.hdata.core.plugin;

import opensource.hdata.config.PluginConfig;
import opensource.hdata.core.JobContext;

public abstract class Writer extends AbstractPlugin {

    public void prepare(JobContext context, PluginConfig writerConfig) {
    }

    public void execute(Record record) {
    }

    public void close() {
    }
}
