package opensource.hdata.util;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;

public class HiveMetaStoreUtils {

    /**
     * 获取Hive表
     * 
     * @param client
     * @param database
     * @param table
     * @return
     */
    public static Table getTable(HiveMetaStoreClient client, String database, String table) {
        try {
            return client.getTable(database, table);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * 判断是否为托管表
     * 
     * @param table
     * @return
     */
    public static boolean isManagedTable(Table table) {
        return "MANAGED_TABLE".equals(table.getTableType());
    }

    /**
     * 判断是否为分区表
     * 
     * @param table
     * @return
     */
    public static boolean isPartitionTable(Table table) {
        return table.getPartitionKeys().size() > 0 ? true : false;
    }

    /**
     * 获取Hive表的分区
     * 
     * @param client
     * @param table
     * @param partitionValues
     * @return
     */
    public static Partition getPartition(HiveMetaStoreClient client, Table table, String partitionValues) {
        try {
            return client.getPartition(table.getDbName(), table.getTableName(), partitionValues.replaceAll("\"", "").replaceAll("\\s+,\\s+", ""));
        } catch (Exception e) {
            return null;
        }
    }
}
