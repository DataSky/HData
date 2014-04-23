package opensource.hdata.util;

import java.math.BigDecimal;
import java.math.BigInteger;

public class TypeConvertUtils {

    /**
     * 数据类型转换
     * 
     * @param src
     * @param clazz
     * @return
     */
    public static Object convert(Object src, Class<?> clazz) {
        if (src == null) {
            return null;
        } else if (src instanceof String) {
            if (clazz == Integer.class) {
                return Integer.valueOf(src.toString());
            } else if (clazz == Long.class) {
                return Long.valueOf(src.toString());
            } else if (clazz == Double.class) {
                return Double.valueOf(src.toString());
            } else if (clazz == Float.class) {
                return Float.valueOf(src.toString());
            } else if (clazz == Boolean.class) {
                return Boolean.valueOf(src.toString());
            } else if (clazz == Short.class) {
                return Short.valueOf(src.toString());
            } else if (clazz == Byte.class) {
                return Byte.valueOf(src.toString());
            } else if (clazz == BigInteger.class) {
                return BigInteger.valueOf(Long.valueOf(src.toString()));
            } else if (clazz == BigDecimal.class) {
                return new BigDecimal(src.toString());
            }
        } else if (clazz == String.class) {
            return src.toString();
        }
        return src;
    }
}
