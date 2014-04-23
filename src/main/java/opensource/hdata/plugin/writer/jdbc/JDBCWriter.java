package opensource.hdata.plugin.writer.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Map;

import opensource.hdata.common.Constants;
import opensource.hdata.config.PluginConfig;
import opensource.hdata.core.Fields;
import opensource.hdata.core.JobContext;
import opensource.hdata.core.plugin.Record;
import opensource.hdata.core.plugin.Writer;
import opensource.hdata.exception.HDataException;
import opensource.hdata.util.JDBCUtils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Joiner;

public class JDBCWriter extends Writer {

    private Connection connection = null;
    private PreparedStatement statement = null;
    private int count;
    private int batchInsertSize;
    private Fields columns;
    private String table;
    private Map<String, Integer> columnTypes;
    private final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat(Constants.DATE_FORMAT_STRING);
    private final int DEFAULT_BATCH_INSERT_SIZE = 10000;
    private static final Logger LOG = LogManager.getLogger(JDBCWriter.class);

    @Override
    public void prepare(JobContext context, PluginConfig writerConfig) {
        columns = context.getFields();
        String driver = writerConfig.getString(JBDCWriterProperties.DRIVER);
        String url = writerConfig.getString(JBDCWriterProperties.URL);
        String username = writerConfig.getString(JBDCWriterProperties.USERNAME);
        String password = writerConfig.getString(JBDCWriterProperties.PASSWORD);
        String table = writerConfig.getString(JBDCWriterProperties.TABLE);
        this.table = table;
        batchInsertSize = writerConfig.getInt(JBDCWriterProperties.BATCH_INSERT_SIZE, DEFAULT_BATCH_INSERT_SIZE);
        if (batchInsertSize < 1) {
            batchInsertSize = DEFAULT_BATCH_INSERT_SIZE;
        }

        try {
            connection = JDBCUtils.getConnection(driver, url, username, password);
            connection.setAutoCommit(false);
            columnTypes = JDBCUtils.getColumnTypes(connection, table);

            String sql = null;
            if (columns != null) {
                String[] placeholder = new String[columns.size()];
                Arrays.fill(placeholder, "?");
                sql = String.format("INSERT INTO %s(%s) VALUES(%s)", table, Joiner.on(", ").join(columns), Joiner.on(", ").join(placeholder));
                LOG.debug(sql);
                statement = connection.prepareStatement(sql);
            }
        } catch (Exception e) {
            JDBCUtils.closeConnection(connection);
            throw new HDataException("Writer prepare failed.", e);
        }
    }

    @Override
    public void execute(Record record) {
        try {
            if (statement == null) {
                String[] placeholder = new String[record.getFieldsCount()];
                Arrays.fill(placeholder, "?");
                String sql = String.format("INSERT INTO %s VALUES(%s)", table, Joiner.on(", ").join(placeholder));
                LOG.debug(sql);
                statement = connection.prepareStatement(sql);
            }

            for (int i = 0, len = record.getFieldsCount(); i < len; i++) {
                if (record.getField(i) instanceof Timestamp
                        && !Integer.valueOf(Types.TIMESTAMP).equals(columnTypes.get(columns.get(i).toLowerCase()))) {
                    statement.setObject(i + 1, DATE_FORMAT.format(record.getField(i)));
                } else {
                    statement.setObject(i + 1, record.getField(i));
                }
            }

            count++;
            statement.addBatch();

            if (count % batchInsertSize == 0) {
                count = 0;
                statement.executeBatch();
                connection.commit();
            }
        } catch (SQLException e) {
            close();
            throw new HDataException("Writer execute failed.", e);
        }
    }

    @Override
    public void close() {
        try {
            if (count > 0) {
                statement.executeBatch();
                connection.commit();
            }

            if (statement != null) {
                statement.close();
            }

        } catch (SQLException e) {
            throw new HDataException(e);
        } finally {
            JDBCUtils.closeConnection(connection);
        }
    }
}
