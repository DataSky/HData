package opensource.hdata.plugin.reader.hdfs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import opensource.hdata.config.JobConfig;
import opensource.hdata.config.PluginConfig;
import opensource.hdata.core.plugin.Splitter;
import opensource.hdata.exception.HDataException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSSplitter extends Splitter {

    @Override
    public List<PluginConfig> split(JobConfig jobConfig) {
        List<PluginConfig> list = new ArrayList<PluginConfig>();
        List<Path> matchedFiles = new ArrayList<Path>();
        PluginConfig readerConfig = jobConfig.getReaderConfig();
        Path dir = new Path(readerConfig.getString(HDFSReaderProperties.DIR));
        int parallelism = readerConfig.getParallelism();

        System.setProperty("HADOOP_USER_NAME", readerConfig.getString(HDFSReaderProperties.HADOOP_USER));
        Configuration conf = new Configuration();
        try {
            FileSystem fs = dir.getFileSystem(conf);
            Pattern filenamePattern = Pattern.compile(readerConfig.getString(HDFSReaderProperties.FILENAME_REGEXP));
            if (fs.exists(dir)) {
                for (FileStatus fileStatus : fs.listStatus(dir)) {
                    Matcher m = filenamePattern.matcher(fileStatus.getPath().getName());
                    if (m.matches()) {
                        matchedFiles.add(fileStatus.getPath());
                    }
                }

                if (matchedFiles.size() > 0) {
                    if (parallelism == 1) {
                        readerConfig.put(HDFSReaderProperties.FILES, matchedFiles);
                        list.add(readerConfig);
                    } else {
                        double step = (double) matchedFiles.size() / parallelism;
                        for (int i = 0; i < parallelism; i++) {
                            List<Path> splitedFiles = new ArrayList<Path>();
                            for (int start = (int) Math.ceil(step * i), end = (int) Math.ceil(step * (i + 1)); start < end; start++) {
                                splitedFiles.add(matchedFiles.get(start));
                            }
                            PluginConfig pluginConfig = (PluginConfig) readerConfig.clone();
                            pluginConfig.put(HDFSReaderProperties.FILES, splitedFiles);
                            list.add(pluginConfig);
                        }
                    }
                }

            } else {
                throw new HDataException(String.format("Path %s not found.", dir));
            }
        } catch (IOException e) {
            throw new HDataException(e);
        }

        return list;
    }
}
