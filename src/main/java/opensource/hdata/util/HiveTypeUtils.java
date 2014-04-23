package opensource.hdata.util;

import org.apache.hadoop.hive.common.type.HiveBaseChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;

public class HiveTypeUtils {

    /**
     * 将Hive Writable类型转为标准Java类型
     * 
     * @param o
     * @return
     */
    public static Object toJavaObject(Object o) {
        if (o instanceof HiveBaseChar) {
            return ((HiveVarchar) o).getValue();
        } else if (o instanceof HiveDecimal) {
            return ((HiveDecimal) o).bigDecimalValue();
        }

        return o;
    }

    /**
     * 获取Hive类型的PrimitiveCategory
     * 
     * @param type
     * @return
     */
    public static PrimitiveCategory getPrimitiveCategory(String type) {
        if ("TINYINT".equals(type)) {
            return PrimitiveObjectInspector.PrimitiveCategory.BYTE;
        } else if ("SMALLINT".equals(type)) {
            return PrimitiveObjectInspector.PrimitiveCategory.SHORT;
        } else if ("BIGINT".equals(type)) {
            return PrimitiveObjectInspector.PrimitiveCategory.LONG;
        } else {
            return PrimitiveObjectInspector.PrimitiveCategory.valueOf(type);
        }
    }

}
