package opensource.hdata.plugin.reader.hive;

import java.util.List;

import opensource.hdata.config.PluginConfig;
import opensource.hdata.core.DefaultRecord;
import opensource.hdata.core.Fields;
import opensource.hdata.core.JobContext;
import opensource.hdata.core.OutputFieldsDeclarer;
import opensource.hdata.core.plugin.Reader;
import opensource.hdata.core.plugin.Record;
import opensource.hdata.core.plugin.RecordCollector;
import opensource.hdata.exception.HDataException;
import opensource.hdata.util.HiveTypeUtils;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

@SuppressWarnings("deprecation")
public class HiveReader extends Reader {

    private final Fields fields = new Fields();
    private List<String> files;
    private List<String> partitionValues;
    private Class<? extends InputFormat<Writable, Writable>> inputFormat;
    private StructObjectInspector oi;
    private List<? extends StructField> structFields;

    private Deserializer deserializer;

    @SuppressWarnings("unchecked")
    @Override
    public void prepare(JobContext context, PluginConfig readerConfig) {
        inputFormat = (Class<? extends InputFormat<Writable, Writable>>) readerConfig.get(HiveReaderProperties.INPUT_FORMAT_CLASS);
        deserializer = (Deserializer) readerConfig.get(HiveReaderProperties.DESERIALIZER);
        files = (List<String>) readerConfig.get(HiveReaderProperties.TABLE_FILES);
        partitionValues = (List<String>) readerConfig.get(HiveReaderProperties.PARTITION_VALUES);
        List<FieldSchema> columns = (List<FieldSchema>) readerConfig.get(HiveReaderProperties.TABLE_COLUMNS);

        for (FieldSchema fs : columns) {
            fields.add(fs.getName());
        }

        try {
            oi = (StructObjectInspector) deserializer.getObjectInspector();
        } catch (SerDeException e) {
            throw new HDataException(e);
        }
        structFields = oi.getAllStructFieldRefs();
    }

    @Override
    public void execute(RecordCollector recordCollector) {

        int columnsCount = fields.size();
        int partitionValueCount = partitionValues == null ? 0 : partitionValues.size();

        JobConf jobConf = new JobConf();
        for (String file : files) {
            Path path = new Path(file);
            try {
                FileSystem fs = path.getFileSystem(jobConf);
                FileInputFormat<Writable, Writable> fileInputFormat = (FileInputFormat<Writable, Writable>) inputFormat.newInstance();
                long filelen = fs.getFileStatus(path).getLen();
                FileSplit split = new FileSplit(path, 0, filelen, (String[]) null);
                RecordReader<Writable, Writable> reader = fileInputFormat.getRecordReader(split, jobConf, Reporter.NULL);
                Writable key = reader.createKey();
                Writable value = reader.createValue();
                while (reader.next(key, value)) {
                    Object row = deserializer.deserialize(value);
                    Record record = new DefaultRecord(columnsCount);
                    for (int i = 0, len = structFields.size(); i < len; i++) {
                        Object fieldData = oi.getStructFieldData(row, structFields.get(i));
                        Object standardData = ObjectInspectorUtils.copyToStandardJavaObject(fieldData, structFields.get(i).getFieldObjectInspector());
                        record.addField(HiveTypeUtils.toJavaObject(standardData));
                    }

                    for (int i = 0, len = partitionValueCount; i < len; i++) {
                        record.addField(partitionValues.get(i));
                    }
                    recordCollector.send(record);
                }
                reader.close();
            } catch (Exception e) {
                throw new HDataException(e);
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(fields);
    }

}
