package opensource.hdata.plugin.reader.ftp;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

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
import opensource.hdata.util.FTPUtils;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.net.ftp.FTPClient;

public class FTPReader extends Reader {

    private Fields fields;
    private String host;
    private int port;
    private String username;
    private String password;
    private String fieldsSeparator;
    private String encoding;
    private int fieldsCount;
    private List<String> files = new ArrayList<String>();

    @SuppressWarnings("unchecked")
    @Override
    public void prepare(JobContext context, PluginConfig readerConfig) {
        host = readerConfig.getString(FTPReaderProperties.HOST);
        port = readerConfig.getInt(FTPReaderProperties.PORT, 21);
        username = readerConfig.getString(FTPReaderProperties.USERNAME, "anonymous");
        password = readerConfig.getString(FTPReaderProperties.PASSWORD, "");
        fieldsSeparator = EscaperUtils.parse(readerConfig.getString(FTPReaderProperties.FIELDS_SEPARATOR, "\t"));
        encoding = readerConfig.getString(FTPReaderProperties.ENCODING, "UTF-8");
        files = (List<String>) readerConfig.get(FTPReaderProperties.FILES);
        fieldsCount = readerConfig.getInt(FTPReaderProperties.FIELDS_COUNT_FILTER, 0);

        if (readerConfig.containsKey(FTPReaderProperties.SCHEMA)) {
            fields = new Fields();
            String[] tokens = readerConfig.getString(FTPReaderProperties.SCHEMA).split("\\s*,\\s*");
            for (String field : tokens) {
                fields.add(field);
            }
        }
    }

    @Override
    public void execute(RecordCollector recordCollector) {
        FTPClient ftpClient = null;
        try {
            ftpClient = FTPUtils.getFtpClient(host, port, username, password);
            for (String file : files) {
                InputStream is = ftpClient.retrieveFileStream(file);
                BufferedReader br = null;
                if (file.endsWith(".gz")) {
                    GZIPInputStream gzin = new GZIPInputStream(is);
                    br = new BufferedReader(new InputStreamReader(gzin, encoding));
                } else {
                    br = new BufferedReader(new InputStreamReader(is, encoding));
                }

                String line = null;
                while ((line = br.readLine()) != null) {
                    String[] tokens = StringUtils.splitByWholeSeparator(line, fieldsSeparator);
                    if (tokens.length >= fieldsCount) {
                        Record record = new DefaultRecord(tokens.length);
                        for (String field : tokens) {
                            record.addField(field);
                        }
                        recordCollector.send(record);
                    }
                }
                ftpClient.completePendingCommand();
                br.close();
                is.close();
            }
        } catch (Exception e) {
            throw new HDataException(e);
        } finally {
            FTPUtils.closeFtpClient(ftpClient);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(fields);
    }
}
