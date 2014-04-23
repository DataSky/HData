package opensource.hdata.util;

import org.apache.logging.log4j.Logger;

public class LoggerUtils {

    public static void error(Logger logger, Exception e) {
        logger.error(e.getMessage());
        logger.error(e.getStackTrace());
    }
}
