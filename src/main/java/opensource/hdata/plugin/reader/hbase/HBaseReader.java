package opensource.hdata.plugin.reader.hbase;

import java.io.IOException;

import opensource.hdata.config.PluginConfig;
import opensource.hdata.core.DefaultRecord;
import opensource.hdata.core.Fields;
import opensource.hdata.core.JobContext;
import opensource.hdata.core.OutputFieldsDeclarer;
import opensource.hdata.core.plugin.Reader;
import opensource.hdata.core.plugin.Record;
import opensource.hdata.core.plugin.RecordCollector;
import opensource.hdata.exception.HDataException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseReader extends Reader {

    private Fields fields = new Fields();
    private HTable table;
    private byte[] startRowkey;
    private byte[] endRowkey;
    private String[] columns;
    private int rowkeyIndex = -1;
    private static final String ROWKEY = ":rowkey";

    @Override
    public void prepare(JobContext context, PluginConfig readerConfig) {
        startRowkey = (byte[]) readerConfig.get(HBaseReaderProperties.START_ROWKWY);
        endRowkey = (byte[]) readerConfig.get(HBaseReaderProperties.END_ROWKWY);

        String[] schema = readerConfig.getString(HBaseReaderProperties.SCHEMA).split(",");
        for (String field : schema) {
            fields.add(field);
        }

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", readerConfig.getString(HBaseReaderProperties.ZOOKEEPER_QUORUM));
        conf.set("hbase.zookeeper.property.clientPort", readerConfig.getString(HBaseReaderProperties.ZOOKEEPER_PROPERTY_CLIENTPORT, "2181"));
        columns = readerConfig.getString(HBaseReaderProperties.COLUMNS).split("\\s*,\\s*");
        for (int i = 0, len = columns.length; i < len; i++) {
            if (ROWKEY.equalsIgnoreCase(columns[i])) {
                rowkeyIndex = i;
                break;
            }
        }

        try {
            table = new HTable(conf, readerConfig.getString(HBaseReaderProperties.TABLE));
        } catch (IOException e) {
            e.printStackTrace();
            throw new HDataException(e);
        }
    }

    @Override
    public void execute(RecordCollector recordCollector) {
        Scan scan = new Scan();
        if (startRowkey.length > 0) {
            scan.setStartRow(startRowkey);
        }
        if (endRowkey.length > 0) {
            scan.setStopRow(endRowkey);
        }

        for (int i = 0, len = columns.length; i < len; i++) {
            if (i != rowkeyIndex) {
                String[] column = columns[i].split(":");
                scan.addColumn(Bytes.toBytes(column[0]), Bytes.toBytes(column[1]));
            }
        }

        try {
            ResultScanner results = table.getScanner(scan);
            for (Result result : results) {
                Record record = new DefaultRecord(fields.size());
                for (int i = 0, len = fields.size(); i < len; i++) {
                    if (i == rowkeyIndex) {
                        record.addField(Bytes.toString(result.getRow()));
                    } else {
                        String[] column = columns[i].split(":");
                        record.addField(Bytes.toString(result.getValue(Bytes.toBytes(column[0]), Bytes.toBytes(column[1]))));
                    }
                }
                recordCollector.send(record);
            }

            if (table != null) {
                table.close();
            }
        } catch (IOException e) {
            throw new HDataException(e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(fields);
    }
}
