package opensource.hdata.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class JDBCUtils {

    private static final Logger LOG = LogManager.getLogger(JDBCUtils.class);

    /**
     * 获取JDBC连接
     * 
     * @param driver
     * @param url
     * @param username
     * @param password
     * @return
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    public static Connection getConnection(String driver, String url, String username, String password) throws ClassNotFoundException, SQLException {
        Class.forName(driver);
        Connection conn = DriverManager.getConnection(url, username, password);
        return conn;
    }

    /**
     * 关闭JDBC连接
     * 
     * @param conn
     */
    public static void closeConnection(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                LoggerUtils.error(LOG, e);
            }
        }
    }

    /**
     * 获取表的字段类型
     * 
     * @param connection
     * @param table
     * @return
     * @throws SQLException
     */
    public static Map<String, Integer> getColumnTypes(Connection connection, String table) throws SQLException {
        Map<String, Integer> map = new HashMap<String, Integer>();
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT * FROM ");
        sql.append(table);
        sql.append(" WHERE 1=2");

        PreparedStatement ps = connection.prepareStatement(sql.toString());
        ResultSetMetaData rsd = ps.executeQuery().getMetaData();
        for (int i = 0; i < rsd.getColumnCount(); i++) {
            map.put(rsd.getColumnName(i + 1).toLowerCase(), rsd.getColumnType(i + 1));
        }
        ps.close();
        return map;
    }

    /**
     * 获取表的字段名称
     * 
     * @param conn
     * @param table
     * @return
     * @throws SQLException
     */
    public static List<String> getColumnNames(Connection conn, String table) throws SQLException {
        List<String> columnNames = new ArrayList<String>();
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT * FROM ");
        sql.append(table);
        sql.append(" WHERE 1=2");

        PreparedStatement ps = conn.prepareStatement(sql.toString());
        ResultSet rs = ps.executeQuery();
        ResultSetMetaData rsd = rs.getMetaData();

        for (int i = 0, len = rsd.getColumnCount(); i < len; i++) {
            columnNames.add(rsd.getColumnName(i + 1));
        }
        rs.close();
        ps.close();

        return columnNames;
    }

    /**
     * 查询表中分割字段值的区域（最大值、最小值）
     * 
     * @param conn
     * @param sql
     * @param splitColumn
     * @return
     * @throws SQLException
     */
    public static double[] querySplitColumnRange(Connection conn, String sql, String splitColumn) throws SQLException {
        double[] minAndMax = new double[2];
        Pattern p = Pattern.compile("\\s+FROM\\s+.*", Pattern.CASE_INSENSITIVE);
        Matcher m = p.matcher(sql);

        if (m.find() && splitColumn != null && !splitColumn.trim().isEmpty()) {
            StringBuilder sb = new StringBuilder();
            sb.append("SELECT MIN(");
            sb.append(splitColumn);
            sb.append("), MAX(");
            sb.append(splitColumn);
            sb.append(")");
            sb.append(m.group(0));

            Statement statement = conn.createStatement();
            ResultSet rs = statement.executeQuery(sb.toString());
            while (rs.next()) {
                minAndMax[0] = rs.getDouble(1);
                minAndMax[1] = rs.getDouble(2);
            }

            rs.close();
            statement.close();
        }

        return minAndMax;
    }

    /**
     * 查询表数值类型的主键
     * 
     * @param conn
     * @param catalog
     * @param schema
     * @param table
     * @return
     * @throws SQLException
     */
    public static String getDigitalPrimaryKey(Connection conn, String catalog, String schema, String table) throws SQLException {
        List<String> primaryKeys = new ArrayList<String>();
        ResultSet rs = conn.getMetaData().getPrimaryKeys(catalog, schema, table);
        while (rs.next()) {
            primaryKeys.add(rs.getString("COLUMN_NAME"));
        }
        rs.close();

        if (primaryKeys.size() > 0) {
            Map<String, Integer> map = getColumnTypes(conn, table);
            for (String pk : primaryKeys) {
                if (isDigitalType(map.get(pk))) {
                    return pk;
                }
            }
        }

        return null;
    }

    /**
     * 判断字段类型是否为数值类型
     * 
     * @param sqlType
     * @return
     */
    public static boolean isDigitalType(int sqlType) {
        switch (sqlType) {
            case Types.NUMERIC:
            case Types.DECIMAL:
            case Types.SMALLINT:
            case Types.INTEGER:
            case Types.BIGINT:
            case Types.REAL:
            case Types.FLOAT:
            case Types.DOUBLE:
                return true;

            default:
                return false;
        }
    }

}
