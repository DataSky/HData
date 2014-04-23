package opensource.hdata.plugin.writer.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import opensource.hdata.config.PluginConfig;
import opensource.hdata.core.JobContext;
import opensource.hdata.core.plugin.Record;
import opensource.hdata.core.plugin.Writer;
import opensource.hdata.exception.HDataException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseWriter extends Writer {

    private HTable table;
    private int batchSize;
    private int rowkeyIndex = -1;
    private List<Put> putList = new ArrayList<Put>();
    private String[] columns;
    private static final String ROWKEY = ":rowkey";

    @Override
    public void prepare(JobContext context, PluginConfig writerConfig) {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", writerConfig.getString(HBaseWriterProperties.ZOOKEEPER_QUORUM));
        conf.set("hbase.zookeeper.property.clientPort", writerConfig.getString(HBaseWriterProperties.ZOOKEEPER_PROPERTY_CLIENTPORT, "2181"));
        batchSize = writerConfig.getInt(HBaseWriterProperties.BATCH_INSERT_SIZE, 10000);
        columns = writerConfig.getString(HBaseWriterProperties.COLUMNS).split(",");
        for (int i = 0, len = columns.length; i < len; i++) {
            if (ROWKEY.equalsIgnoreCase(columns[i])) {
                rowkeyIndex = i;
                break;
            }
        }

        if (rowkeyIndex == -1) {
            throw new IllegalArgumentException("Can not find :rowkey in columnsMapping of HBase Writer!");
        }

        try {
            table = new HTable(conf, writerConfig.getString(HBaseWriterProperties.TABLE));
        } catch (IOException e) {
            throw new HDataException(e);
        }
    }

    @Override
    public void execute(Record record) {
        Object rowkeyValue = record.getField(rowkeyIndex);
        Put put = new Put(Bytes.toBytes(rowkeyValue == null ? "NULL" : rowkeyValue.toString()));
        for (int i = 0, len = record.getFieldsCount(); i < len; i++) {
            if (i != rowkeyIndex) {
                String[] tokens = columns[i].split(":");
                put.add(Bytes.toBytes(tokens[0]), Bytes.toBytes(tokens[1]),
                        record.getField(i) == null ? null : Bytes.toBytes(record.getField(i).toString()));
            }
        }

        putList.add(put);
        if (putList.size() == batchSize) {
            try {
                table.put(putList);
            } catch (IOException e) {
                throw new HDataException(e);
            }
            putList.clear();
        }
    }

    @Override
    public void close() {
        if (table != null) {
            try {
                if (putList.size() > 0) {
                    table.put(putList);
                }

                table.close();
            } catch (IOException e) {
                throw new HDataException(e);
            }
            putList.clear();
        }
    }
}
