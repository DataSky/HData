package opensource.hdata.plugin.reader.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import opensource.hdata.common.Constants;
import opensource.hdata.config.JobConfig;
import opensource.hdata.config.PluginConfig;
import opensource.hdata.core.plugin.Splitter;
import opensource.hdata.exception.HDataException;
import opensource.hdata.util.JDBCUtils;
import opensource.hdata.util.Utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Joiner;

public class JDBCSplitter extends Splitter {

    private static final String CONDITIONS_REGEX = "\\$CONDITIONS";
    private static final Logger LOG = LogManager.getLogger(JDBCSplitter.class);

    private void checkIfContainsConditionKey(String sql, String errorMessage) {
        if (!sql.contains("$CONDITIONS")) {
            throw new HDataException(errorMessage);
        }
    }

    private List<PluginConfig> buildPluginConfigs(Connection conn, String sql, String splitColumn, PluginConfig readerConfig) {
        List<PluginConfig> list = new ArrayList<PluginConfig>();
        try {
            int parallelism = readerConfig.getParallelism();
            double[] minAndMax = JDBCUtils.querySplitColumnRange(conn, sql.replaceAll(CONDITIONS_REGEX, "(1 = 1)"), splitColumn);
            double min = minAndMax[0];
            double max = minAndMax[1] + 1;
            double step = (max - min) / parallelism;
            for (int i = 0, len = parallelism; i < len; i++) {
                PluginConfig otherReaderConfig = (PluginConfig) readerConfig.clone();
                StringBuilder sb = new StringBuilder();
                sb.append(splitColumn);
                sb.append(" >= ");
                sb.append((long) Math.ceil(min + step * i));
                sb.append(" AND ");
                sb.append(splitColumn);

                if (i == (len - 1)) {
                    sb.append(" <= ");
                } else {
                    sb.append(" < ");
                }
                sb.append((long) Math.ceil(min + step * (i + 1)));

                otherReaderConfig.setProperty(JBDCReaderProperties.SQL, sql.toString().replaceAll(CONDITIONS_REGEX, sb.toString()));
                list.add(otherReaderConfig);
            }
            return list;
        } catch (SQLException e) {
            throw new HDataException(e);
        } finally {
            JDBCUtils.closeConnection(conn);
        }
    }

    @Override
    public List<PluginConfig> split(JobConfig jobConfig) {
        PluginConfig readerConfig = jobConfig.getReaderConfig();
        String driver = readerConfig.getString(JBDCReaderProperties.DRIVER);
        String url = readerConfig.getString(JBDCReaderProperties.URL);
        String username = readerConfig.getString(JBDCReaderProperties.USERNAME);
        String password = readerConfig.getString(JBDCReaderProperties.PASSWORD);
        int parallelism = readerConfig.getParallelism();

        StringBuilder sql = new StringBuilder();
        if (readerConfig.containsKey(JBDCReaderProperties.SQL)) {
            if (parallelism > 1) {
                checkIfContainsConditionKey(readerConfig.getString(JBDCReaderProperties.SQL),
                        "Reader must contains key word \"$CONDITIONS\" in sql property when parallelism > 1.");
            }
            sql.append(readerConfig.get(JBDCReaderProperties.SQL));
        } else {
            String table = readerConfig.getString(JBDCReaderProperties.TABLE);
            sql.append("SELECT ");
            if (!readerConfig.containsKey(JBDCReaderProperties.COLUMNS) && !readerConfig.containsKey(JBDCReaderProperties.EXCLUDE_COLUMNS)) {
                sql.append("*");
            } else if (readerConfig.containsKey(JBDCReaderProperties.COLUMNS)) {
                String columns = readerConfig.getString(JBDCReaderProperties.COLUMNS);
                sql.append(columns);
            } else if (readerConfig.containsKey(JBDCReaderProperties.EXCLUDE_COLUMNS)) {
                String[] excludeColumns = readerConfig.getString(JBDCReaderProperties.EXCLUDE_COLUMNS).trim().split(Constants.COLUMNS_SPLIT_REGEX);
                Connection conn = null;
                try {
                    conn = JDBCUtils.getConnection(driver, url, username, password);
                    String selectColumns = Joiner.on(", ").join(Utils.getColumns(JDBCUtils.getColumnNames(conn, table), excludeColumns));
                    sql.append(selectColumns);
                } catch (Exception e) {
                    e.printStackTrace();
                    JDBCUtils.closeConnection(conn);
                    throw new HDataException(e);
                }

            }
            sql.append(" FROM ");
            sql.append(table);

            if (readerConfig.containsKey(JBDCReaderProperties.WHERE)) {
                String where = readerConfig.getString(JBDCReaderProperties.WHERE);
                sql.append(" WHERE ");
                sql.append(where);
                sql.append(" AND $CONDITIONS");
            } else {
                sql.append(" WHERE $CONDITIONS");
            }
        }

        if (readerConfig.containsKey(JBDCReaderProperties.SPLIT_BY)) {
            String splitColumn = readerConfig.getString(JBDCReaderProperties.SPLIT_BY);
            LOG.debug("Get split-by column: {}", splitColumn);

            Connection conn = null;
            try {
                conn = JDBCUtils.getConnection(driver, url, username, password);
                return buildPluginConfigs(conn, sql.toString(), splitColumn, readerConfig);
            } catch (Exception e) {
                throw new HDataException(e);
            } finally {
                JDBCUtils.closeConnection(conn);
            }
        } else {
            if (readerConfig.containsKey(JBDCReaderProperties.TABLE)) {
                Connection conn = null;
                try {
                    String table = readerConfig.getString(JBDCReaderProperties.TABLE);
                    LOG.info("Attemp to query digital primary key for table: {}", table);
                    conn = JDBCUtils.getConnection(driver, url, username, password);
                    String splitColumn = JDBCUtils.getDigitalPrimaryKey(conn, conn.getCatalog(), null, table);
                    if (splitColumn != null) {
                        LOG.info("Table {} find digital primary key: {}", table, splitColumn);
                        return buildPluginConfigs(conn, sql.toString(), splitColumn, readerConfig);
                    } else {
                        LOG.info("Table {} can not find digital primary key.", table);
                    }
                } catch (Exception e) {
                    throw new HDataException(e);
                } finally {
                    JDBCUtils.closeConnection(conn);
                }
            }

            if (parallelism > 1) {
                LOG.warn(
                        "Reader parallelism is set to {}, but the \"split-by\" config is not given, so reader parallelism is set to default value: 1.",
                        parallelism);
            }

            List<PluginConfig> list = new ArrayList<PluginConfig>();
            readerConfig.setProperty(JBDCReaderProperties.SQL, sql.toString().replaceAll(CONDITIONS_REGEX, "(1 = 1)"));
            list.add(readerConfig);
            return list;
        }
    }
}
