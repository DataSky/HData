package opensource.hdata.tool;

import java.sql.Connection;
import java.sql.Statement;

import opensource.hdata.exception.HDataException;
import opensource.hdata.util.JDBCUtils;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SQLExecuteTool {

    private static final String JDBC_DRIVER = "jdbc-driver";
    private static final String JDBC_URL = "jdbc-url";
    private static final String JDBC_USERNAME = "jdbc-username";
    private static final String JDBC_PASSWORD = "jdbc-password";
    private static final String SQL = "sql";
    private static final Logger LOG = LogManager.getLogger(SQLExecuteTool.class);

    public Options createOptions() {
        Options options = new Options();
        options.addOption(null, JDBC_DRIVER, true, "jdbc driver class name");
        options.addOption(null, JDBC_URL, true, "jdbc url, e.g., jdbc:mysql://localhost:3306/database");
        options.addOption(null, JDBC_USERNAME, true, "jdbc username");
        options.addOption(null, JDBC_PASSWORD, true, "jdbc password");
        options.addOption(null, SQL, true, "sql");
        return options;
    }

    public void printHelp(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(" ", options);
    }

    public static void main(String[] args) {
        SQLExecuteTool tool = new SQLExecuteTool();
        Options options = tool.createOptions();
        if (args.length < 1) {
            tool.printHelp(options);
            System.exit(-1);
        }

        CommandLineParser parser = new PosixParser();
        CommandLine cmd = null;
        Connection conn = null;
        try {
            cmd = parser.parse(options, args);
            String driver = cmd.getOptionValue(JDBC_DRIVER);
            String url = cmd.getOptionValue(JDBC_URL);
            String username = cmd.getOptionValue(JDBC_USERNAME);
            String password = cmd.getOptionValue(JDBC_PASSWORD);
            String sql = cmd.getOptionValue(SQL);
            conn = JDBCUtils.getConnection(driver, url, username, password);
            Statement statement = conn.createStatement();

            LOG.info("Executing sql: {}", sql);
            statement.execute(sql);
            LOG.info("Execute successfully.");
        } catch (ParseException e) {
            tool.printHelp(options);
            System.exit(-1);
        } catch (Exception e) {
            throw new HDataException(e);
        } finally {
            JDBCUtils.closeConnection(conn);
        }
    }

}
