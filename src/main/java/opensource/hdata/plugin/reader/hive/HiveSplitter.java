package opensource.hdata.plugin.reader.hive;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import opensource.hdata.config.JobConfig;
import opensource.hdata.config.PluginConfig;
import opensource.hdata.core.plugin.Splitter;
import opensource.hdata.exception.HDataException;
import opensource.hdata.util.LoggerUtils;
import opensource.hdata.util.Utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class HiveSplitter extends Splitter {

    private static final Logger LOG = LogManager.getLogger(HiveSplitter.class);

    @Override
    public List<PluginConfig> split(JobConfig jobConfig) {
        List<PluginConfig> list = new ArrayList<PluginConfig>();
        PluginConfig readerConfig = jobConfig.getReaderConfig();
        String metastoreUris = readerConfig.getString(HiveReaderProperties.METASTORE_URIS);
        String dbName = readerConfig.getString(HiveReaderProperties.DATABASE, "default");
        String tableName = readerConfig.getString(HiveReaderProperties.TABLE);
        int parallelism = readerConfig.getParallelism();
        List<String> partitionValues = null;

        HiveConf conf = new HiveConf();
        conf.set(ConfVars.METASTOREURIS.varname, metastoreUris);

        Hive hive;
        Table table;
        try {
            hive = Hive.get(conf, true);
            table = hive.getTable(dbName, tableName, false);
        } catch (HiveException e) {
            throw new HDataException(e);
        }

        if (table == null) {
            throw new HDataException(String.format("Table %s.%s is not exist.", dbName, tableName));
        }

        readerConfig.put(HiveReaderProperties.TABLE_COLUMNS, table.getAllCols());
        readerConfig.put(HiveReaderProperties.INPUT_FORMAT_CLASS, table.getInputFormatClass());
        readerConfig.put(HiveReaderProperties.DESERIALIZER, table.getDeserializer());

        String tableLocation = Utils.fixLocaltion(table.getDataLocation().toString(), metastoreUris);
        if (readerConfig.containsKey(HiveReaderProperties.PARTITIONS)) {
            String partitions = readerConfig.getString(HiveReaderProperties.PARTITIONS);
            tableLocation += "/" + partitions.replaceAll("\\s*,\\s*", "/");
            partitionValues = Utils.parsePartitionValue(partitions);
            readerConfig.put(HiveReaderProperties.PARTITION_VALUES, partitionValues);
        }

        List<String> files = getTableFiles(tableLocation);
        if (files == null || files.size() < 1) {
            LOG.info("Can not find files on path {}", tableLocation);
            return null;
        }

        if (parallelism > files.size()) {
            parallelism = files.size();
            LOG.info("Reader parallelism is greater than file count, so parallelism is set to equal with file count.");
        }

        if (parallelism == 1) {
            readerConfig.put(HiveReaderProperties.TABLE_FILES, files);
            list.add(readerConfig);
        } else {
            double step = (double) files.size() / parallelism;
            for (int i = 0; i < parallelism; i++) {
                List<String> splitedFiles = new ArrayList<String>();
                for (int start = (int) Math.ceil(step * i), end = (int) Math.ceil(step * (i + 1)); start < end; start++) {
                    splitedFiles.add(files.get(start));
                }
                PluginConfig pluginConfig = (PluginConfig) readerConfig.clone();
                pluginConfig.put(HiveReaderProperties.TABLE_FILES, splitedFiles);
                list.add(pluginConfig);
            }
        }

        Hive.closeCurrent();
        return list;
    }

    private List<String> getTableFiles(String tableLocation) {
        try {
            Configuration conf = new Configuration();
            Path path = new Path(tableLocation);
            FileSystem hdfs = path.getFileSystem(conf);
            FileStatus[] fileStatus = hdfs.listStatus(path);
            List<String> files = new ArrayList<String>();
            for (FileStatus fs : fileStatus) {
                if (!fs.isDir() && !fs.getPath().getName().startsWith("_")) {
                    files.add(fs.getPath().toString());
                }
            }
            return files;
        } catch (IOException e) {
            LoggerUtils.error(LOG, e);
            return null;
        }
    }

}
