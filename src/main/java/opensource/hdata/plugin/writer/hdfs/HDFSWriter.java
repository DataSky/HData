package opensource.hdata.plugin.writer.hdfs;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import opensource.hdata.config.PluginConfig;
import opensource.hdata.core.JobContext;
import opensource.hdata.core.plugin.Record;
import opensource.hdata.core.plugin.Writer;
import opensource.hdata.exception.HDataException;
import opensource.hdata.util.EscaperUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

import com.google.common.base.Joiner;

public class HDFSWriter extends Writer {

    private String path;
    private String fieldsSeparator;
    private String lineSeparator;
    private String encoding;
    private String compressCodec;
    private String hadoopUser;
    private BufferedWriter bw;
    private String[] strArray;
    private static AtomicInteger sequence = new AtomicInteger(0);
    private static final Pattern REG_FILE_PATH_WITHOUT_EXTENSION = Pattern.compile(".*?(?=\\.\\w+$)");
    private static final Pattern REG_FILE_EXTENSION = Pattern.compile("(\\.\\w+)$");

    @Override
    public void prepare(JobContext context, PluginConfig writerConfig) {
        path = writerConfig.getString(HDFSWriterProperties.PATH);
        fieldsSeparator = EscaperUtils.parse(writerConfig.getString(HDFSWriterProperties.FIELDS_SEPARATOR, "\t"));
        lineSeparator = EscaperUtils.parse(writerConfig.getString(HDFSWriterProperties.LINE_SEPARATOR, "\n"));
        encoding = writerConfig.getString(HDFSWriterProperties.ENCODING, "UTF-8");
        compressCodec = writerConfig.getProperty(HDFSWriterProperties.COMPRESS_CODEC);
        hadoopUser = writerConfig.getString(HDFSWriterProperties.HADOOP_USER);
        System.setProperty("HADOOP_USER_NAME", hadoopUser);

        int parallelism = writerConfig.getParallelism();
        if (parallelism > 1) {
            String filePathWithoutExtension = "";
            String fileExtension = "";
            Matcher m1 = REG_FILE_PATH_WITHOUT_EXTENSION.matcher(path.trim());
            if (m1.find()) {
                filePathWithoutExtension = m1.group();
            }

            Matcher m2 = REG_FILE_EXTENSION.matcher(path.trim());
            if (m2.find()) {
                fileExtension = m2.group();
            }
            path = String.format("%s_%04d%s", filePathWithoutExtension, sequence.getAndIncrement(), fileExtension);
        }

        Path hdfsPath = new Path(path);
        Configuration conf = new Configuration();
        try {
            FileSystem fs = hdfsPath.getFileSystem(conf);
            FSDataOutputStream output = fs.create(hdfsPath);
            if (compressCodec == null) {
                bw = new BufferedWriter(new OutputStreamWriter(output, encoding));
            } else {
                CompressionCodecFactory factory = new CompressionCodecFactory(conf);
                CompressionCodec codec = factory.getCodecByClassName(compressCodec);
                bw = new BufferedWriter(new OutputStreamWriter(codec.createOutputStream(output), encoding));
            }
        } catch (IOException e) {
            throw new HDataException(e);
        }

    }

    @Override
    public void execute(Record record) {
        if (strArray == null) {
            strArray = new String[record.getFieldsCount()];
        }

        for (int i = 0, len = record.getFieldsCount(); i < len; i++) {
            Object o = record.getField(i);
            if (o == null) {
                strArray[i] = "NULL";
            } else {
                strArray[i] = o.toString();
            }
        }
        try {
            bw.write(Joiner.on(fieldsSeparator).join(strArray));
            bw.write(lineSeparator);
        } catch (IOException e) {
            throw new HDataException(e);
        }
    }

    @Override
    public void close() {
        if (bw != null) {
            try {
                bw.flush();
                bw.close();
            } catch (IOException e) {
                throw new HDataException(e);
            }
        }
    }
}
