package opensource.hdata.plugin.writer.hive;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtField;
import opensource.hdata.config.PluginConfig;
import opensource.hdata.core.JobContext;
import opensource.hdata.core.plugin.Record;
import opensource.hdata.core.plugin.Writer;
import opensource.hdata.exception.HDataException;
import opensource.hdata.plugin.reader.hive.HiveReaderProperties;
import opensource.hdata.util.HiveTypeUtils;
import opensource.hdata.util.LoggerUtils;
import opensource.hdata.util.TypeConvertUtils;
import opensource.hdata.util.Utils;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.serde2.Serializer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@SuppressWarnings("deprecation")
public class HiveWriter extends Writer {

    private Serializer serializer;
    private HiveOutputFormat<?, ?> outputFormat;
    private StructObjectInspector inspector;
    private FileSinkOperator.RecordWriter writer;
    private Path path = null;
    private Map<String, String> partitionSpecify = new HashMap<String, String>();
    private int partitionKeySize;
    private PluginConfig writerConfig;
    private Object hiveRecord;
    private String hdfsTmpDir;

    private static Class<?> hiveRecordWritale;
    private static List<Field> classFields = new ArrayList<Field>();
    private static List<Path> files = new ArrayList<Path>();
    private static final Pattern HDFS_MASTER = Pattern.compile("hdfs://[\\w\\.]+:\\d+");
    private static final Logger LOG = LogManager.getLogger(HiveWriter.class);

    private synchronized static void createHiveRecordClass(List<FieldSchema> columns) {
        if (hiveRecordWritale == null) {
            ClassPool pool = ClassPool.getDefault();
            try {
                CtClass ctClass = pool.get("opensource.hdata.plugin.writer.hive.HiveRecordWritable");
                for (FieldSchema fieldSchema : columns) {
                    PrimitiveCategory primitiveCategory = HiveTypeUtils.getPrimitiveCategory(fieldSchema.getType().replaceAll("\\(.*\\)", "")
                            .toUpperCase());
                    Class<?> fieldTypeClazz = PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(primitiveCategory)
                            .getJavaPrimitiveClass();
                    CtField ctField = new CtField(pool.get(fieldTypeClazz.getName()), fieldSchema.getName(), ctClass);
                    ctClass.addField(ctField);
                }
                hiveRecordWritale = ctClass.toClass();
                for (Field field : hiveRecordWritale.getDeclaredFields()) {
                    field.setAccessible(true);
                    classFields.add(field);
                }
            } catch (Exception e) {
                throw new HDataException(e);
            }
        }
    }

    @Override
    public void prepare(JobContext context, PluginConfig writerConfig) {
        hdfsTmpDir = context.getEngineConfig().getString("hdata.hive.writer.tmp.dir", "/tmp");
        this.writerConfig = writerConfig;
        String metastoreUris = writerConfig.getString(HiveWriterProperties.METASTORE_URIS);
        String dbName = writerConfig.getString(HiveWriterProperties.DATABASE, "default");
        String tableName = writerConfig.getString(HiveWriterProperties.TABLE);
        boolean isCompress = writerConfig.getBoolean(HiveWriterProperties.COMPRESS, true);

        System.setProperty("HADOOP_USER_NAME", writerConfig.getString(HiveWriterProperties.HADOOP_USER));

        HiveConf conf = new HiveConf();
        conf.set(ConfVars.METASTOREURIS.varname, metastoreUris);

        Hive hive;
        Table table;
        try {
            hive = Hive.get(conf, true);
            table = hive.getTable(dbName, tableName, false);

            partitionKeySize = table.getPartitionKeys().size();
            serializer = (Serializer) table.getDeserializer();
            outputFormat = (HiveOutputFormat<?, ?>) table.getOutputFormatClass().newInstance();
            if (writerConfig.containsKey(HiveReaderProperties.PARTITIONS)) {
                String partitions = writerConfig.getString(HiveReaderProperties.PARTITIONS);
                String[] partKVs = partitions.split("\\s*,\\s*");
                for (String kv : partKVs) {
                    String[] tokens = kv.split("=");
                    if (tokens.length == 2) {
                        partitionSpecify.put(tokens[0], tokens[1]);
                    }
                }
            } else if (partitionKeySize > 0) {
                throw new HDataException(String.format("Table %s.%s is partition table, but partition config is not given.", dbName, tableName));
            }

            createHiveRecordClass(table.getCols());
            hiveRecord = hiveRecordWritale.newInstance();

            String tableLocation = Utils.fixLocaltion(table.getDataLocation().toString(), metastoreUris);
            Matcher m = HDFS_MASTER.matcher(tableLocation);
            if (m.find()) {
                path = new Path(String.format("%s/%s/%s-%s.tmp", m.group(), hdfsTmpDir, tableName, UUID.randomUUID().toString().replaceAll("-", "")));
                files.add(path);
            }

            inspector = (StructObjectInspector) ObjectInspectorFactory.getReflectionObjectInspector(HiveRecordWritable.class,
                    ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
            JobConf jobConf = new JobConf();
            writer = outputFormat.getHiveRecordWriter(jobConf, path, HiveRecordWritable.class, isCompress, table.getMetadata(), Reporter.NULL);
        } catch (Exception e) {
            throw new HDataException(e);
        } finally {
            Hive.closeCurrent();
        }
    }

    @Override
    public void execute(Record record) {
        try {
            for (int i = 0, len = record.getFieldsCount() - partitionKeySize; i < len; i++) {
                classFields.get(i).set(hiveRecord, TypeConvertUtils.convert(record.getField(i), classFields.get(i).getType()));
            }
            writer.write(serializer.serialize(hiveRecord, inspector));
        } catch (Exception e) {
            throw new HDataException(e);
        }
    }

    private synchronized static Partition createPartition(Hive hive, Table table, Map<String, String> partSpec) {
        Partition p = null;
        try {
            p = hive.getPartition(table, partSpec, false);
            if (p == null) {
                p = hive.getPartition(table, partSpec, true);
            }
        } catch (HiveException e) {
            throw new HDataException(e);
        }
        return p;
    }

    @Override
    public void close() {
        if (writer != null) {
            try {
                writer.close(true);

                String metastoreUris = writerConfig.getString(HiveWriterProperties.METASTORE_URIS);
                String dbName = writerConfig.getString(HiveWriterProperties.DATABASE, "default");
                String tableName = writerConfig.getString(HiveWriterProperties.TABLE);
                HiveConf conf = new HiveConf();
                conf.set(ConfVars.METASTOREURIS.varname, metastoreUris);
                Path renamedPath = new Path(path.toString().replaceFirst("\\.tmp$", ""));
                FileSystem fs = renamedPath.getFileSystem(conf);
                fs.rename(path, renamedPath);

                Hive hive;
                try {
                    hive = Hive.get(conf, true);
                    if (partitionKeySize == 0) {
                        LOG.info("Loading data {} into table {}.{}", renamedPath.toString(), dbName, tableName);
                        hive.loadTable(renamedPath, dbName + "." + tableName, false, false);
                    } else {
                        Table table = hive.getTable(dbName, tableName, false);
                        Partition p = createPartition(hive, table, partitionSpecify);
                        LOG.info("Loading data {} into table {}.{} partition({})", renamedPath.toString(), dbName, tableName, p.getName());
                        hive.loadPartition(renamedPath, dbName + "." + tableName, partitionSpecify, false, false, true, false);
                    }
                } catch (Exception e) {
                    throw new HDataException(e);
                } finally {
                    Hive.closeCurrent();
                }
            } catch (IOException e) {
                LoggerUtils.error(LOG, e);
            }
        }
    }
}
