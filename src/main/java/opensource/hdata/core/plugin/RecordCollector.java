package opensource.hdata.core.plugin;

import opensource.hdata.core.Metric;
import opensource.hdata.core.Storage;

public class RecordCollector {

    private Storage storage;
    private Metric metric;

    public RecordCollector(Storage storage, Metric metric) {
        this.storage = storage;
        this.metric = metric;
    }

    public void send(Record record) {
        storage.put(record);
        metric.getReadCount().incrementAndGet();
    }

    public void send(Record[] records) {
        storage.put(records);
    }
}
