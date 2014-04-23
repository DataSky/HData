package opensource.hdata.plugin.reader.hdfs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
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
import opensource.hdata.util.EscaperUtils;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

public class HDFSReader extends Reader {

    private Fields fields;
    private String fieldsSeparator;
    private String encoding;
    private List<Path> files = new ArrayList<Path>();

    @SuppressWarnings("unchecked")
    @Override
    public void prepare(JobContext context, PluginConfig readerConfig) {
        fieldsSeparator = EscaperUtils.parse(readerConfig.getString(HDFSReaderProperties.FIELDS_SEPARATOR, "\t"));
        files = (List<Path>) readerConfig.get(HDFSReaderProperties.FILES);
        encoding = readerConfig.getString(HDFSReaderProperties.ENCODING, "UTF-8");
        if (readerConfig.containsKey(HDFSReaderProperties.SCHEMA)) {
            fields = new Fields();
            String[] tokens = readerConfig.getString(HDFSReaderProperties.SCHEMA).split("\\s*,\\s*");
            for (String field : tokens) {
                fields.add(field);
            }
        }
    }

    @Override
    public void execute(RecordCollector recordCollector) {
        Configuration conf = new Configuration();
        CompressionCodecFactory codecFactory = new CompressionCodecFactory(conf);
        try {
            for (Path file : files) {
                FileSystem fs = file.getFileSystem(conf);
                CompressionCodec codec = codecFactory.getCodec(file);
                FSDataInputStream input = fs.open(file);
                BufferedReader br;
                String line = null;
                if (codec == null) {
                    br = new BufferedReader(new InputStreamReader(input, encoding));
                } else {
                    br = new BufferedReader(new InputStreamReader(codec.createInputStream(input), encoding));
                }
                while ((line = br.readLine()) != null) {
                    String[] tokens = StringUtils.splitByWholeSeparator(line, fieldsSeparator);
                    Record record = new DefaultRecord(tokens.length);
                    for (String field : tokens) {
                        record.addField(field);
                    }
                    recordCollector.send(record);
                }
                br.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new HDataException(e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(fields);
    }
}
