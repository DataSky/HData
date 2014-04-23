package opensource.hdata.core;

import opensource.hdata.config.Configuration;
import opensource.hdata.config.EngineConfig;
import opensource.hdata.config.JobConfig;

public class JobContext {

    private Fields fields;
    private EngineConfig engineConfig;
    private JobConfig jobConfig;
    private OutputFieldsDeclarer declarer;
    private Storage storage;
    private Metric metric;
    private boolean isWriterError;

    public Fields getFields() {
        return fields;
    }

    protected void setFields(Fields fields) {
        this.fields = fields;
    }

    public Configuration getEngineConfig() {
        return engineConfig;
    }

    public void setEngineConfig(EngineConfig engineConfig) {
        this.engineConfig = engineConfig;
    }

    protected OutputFieldsDeclarer getDeclarer() {
        return declarer;
    }

    protected void setDeclarer(OutputFieldsDeclarer declarer) {
        this.declarer = declarer;
    }

    public Storage getStorage() {
        return storage;
    }

    public void setStorage(Storage storage) {
        this.storage = storage;
    }

    public Metric getMetric() {
        return metric;
    }

    public void setMetric(Metric metric) {
        this.metric = metric;
    }

    public JobConfig getJobConfig() {
        return jobConfig;
    }

    public void setJobConfig(JobConfig jobConfig) {
        this.jobConfig = jobConfig;
    }

    public boolean isWriterError() {
        return isWriterError;
    }

    public void setWriterError(boolean isWriterError) {
        this.isWriterError = isWriterError;
    }

}
