package opensource.hdata.plugin.reader.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import opensource.hdata.config.PluginConfig;
import opensource.hdata.core.DefaultRecord;
import opensource.hdata.core.Fields;
import opensource.hdata.core.JobContext;
import opensource.hdata.core.OutputFieldsDeclarer;
import opensource.hdata.core.plugin.Reader;
import opensource.hdata.core.plugin.Record;
import opensource.hdata.core.plugin.RecordCollector;
import opensource.hdata.exception.HDataException;
import opensource.hdata.util.JDBCUtils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class JDBCReader extends Reader {

    private Connection connection;
    private String sql;
    private Fields fields;
    private static final Logger LOG = LogManager.getLogger(JDBCReader.class);

    @Override
    public void prepare(JobContext context, PluginConfig readerConfig) {
        String driver = readerConfig.getString(JBDCReaderProperties.DRIVER);
        String url = readerConfig.getString(JBDCReaderProperties.URL);
        String username = readerConfig.getString(JBDCReaderProperties.USERNAME);
        String password = readerConfig.getString(JBDCReaderProperties.PASSWORD);
        sql = readerConfig.getString(JBDCReaderProperties.SQL);
        LOG.debug(sql);

        try {
            connection = JDBCUtils.getConnection(driver, url, username, password);
        } catch (Exception e) {
            throw new HDataException(e);
        }
    }

    @Override
    public void execute(RecordCollector recordCollector) {
        try {
            Statement statement = connection.createStatement();

            ResultSet rs = statement.executeQuery(sql);
            ResultSetMetaData metaData = rs.getMetaData();
            int ColumnCount = metaData.getColumnCount();

            if (fields == null) {
                fields = new Fields();
                for (int i = 1; i <= ColumnCount; i++) {
                    fields.add(metaData.getColumnName(i));
                }
            }

            while (rs.next()) {
                Record r = new DefaultRecord(ColumnCount);
                for (int i = 1; i <= ColumnCount; i++) {
                    r.addField(i - 1, rs.getObject(i));
                }
                recordCollector.send(r);
            }
            rs.close();
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
            JDBCUtils.closeConnection(connection);
            throw new HDataException(e);
        }
    }

    @Override
    public void close() {
        JDBCUtils.closeConnection(connection);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(fields);
    }
}
