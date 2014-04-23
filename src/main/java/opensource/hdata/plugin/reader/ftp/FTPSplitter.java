package opensource.hdata.plugin.reader.ftp;

import java.util.ArrayList;
import java.util.List;

import opensource.hdata.config.JobConfig;
import opensource.hdata.config.PluginConfig;
import opensource.hdata.core.plugin.Splitter;
import opensource.hdata.exception.HDataException;
import opensource.hdata.util.FTPUtils;

import org.apache.commons.net.ftp.FTPClient;

public class FTPSplitter extends Splitter {

    @Override
    public List<PluginConfig> split(JobConfig jobConfig) {
        List<PluginConfig> list = new ArrayList<PluginConfig>();
        PluginConfig readerConfig = jobConfig.getReaderConfig();
        String host = readerConfig.getString(FTPReaderProperties.HOST);
        int port = readerConfig.getInt(FTPReaderProperties.PORT, 21);
        String username = readerConfig.getString(FTPReaderProperties.USERNAME, "anonymous");
        String password = readerConfig.getString(FTPReaderProperties.PASSWORD, "");
        String dir = readerConfig.getString(FTPReaderProperties.DIR);
        String filenameRegexp = readerConfig.getString(FTPReaderProperties.FILENAME);
        boolean recursive = readerConfig.getBoolean(FTPReaderProperties.RECURSIVE, false);
        int parallelism = readerConfig.getParallelism();

        FTPClient ftpClient = null;
        try {
            ftpClient = FTPUtils.getFtpClient(host, port, username, password);
            List<String> files = new ArrayList<String>();
            FTPUtils.listFile(files, ftpClient, dir, filenameRegexp, recursive);
            if (files.size() > 0) {
                if (parallelism == 1) {
                    readerConfig.put(FTPReaderProperties.FILES, files);
                    list.add(readerConfig);
                } else {
                    double step = (double) files.size() / parallelism;
                    for (int i = 0; i < parallelism; i++) {
                        List<String> splitedFiles = new ArrayList<String>();
                        for (int start = (int) Math.ceil(step * i), end = (int) Math.ceil(step * (i + 1)); start < end; start++) {
                            splitedFiles.add(files.get(start));
                        }
                        PluginConfig pluginConfig = (PluginConfig) readerConfig.clone();
                        pluginConfig.put(FTPReaderProperties.FILES, splitedFiles);
                        list.add(pluginConfig);
                    }
                }
            }
        } catch (Exception e) {
            throw new HDataException(e);
        } finally {
            FTPUtils.closeFtpClient(ftpClient);
        }

        return list;
    }

}
