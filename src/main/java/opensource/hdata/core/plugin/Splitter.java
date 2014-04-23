package opensource.hdata.core.plugin;

import java.util.List;

import opensource.hdata.config.JobConfig;
import opensource.hdata.config.PluginConfig;

public abstract class Splitter extends AbstractPlugin {

    public abstract List<PluginConfig> split(JobConfig jobConfig);
}
