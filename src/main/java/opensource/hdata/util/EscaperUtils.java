package opensource.hdata.util;

import java.util.HashMap;
import java.util.Map;

public class EscaperUtils {
    private static Map<Character, Character> map = null;
    private static final char CHAR_SLASH = '\\';

    /**
     * 特殊字符转义
     * 
     * @param input
     * @return
     */
    public static String parse(String input) {
        int cursor = 0;
        int index = input.indexOf(CHAR_SLASH, cursor);

        if (index < 0) {
            return input;
        }

        StringBuilder sb = new StringBuilder();
        int len = input.length();
        while ((index = input.indexOf('\\', cursor)) != -1) {
            if (index < len - 1) {
                if (map.containsKey(input.charAt(index + 1))) {
                    sb.append(input.substring(cursor, index));
                    sb.append(map.get(input.charAt(index + 1)));
                } else {
                    sb.append(input.substring(cursor, index + 2));
                }
                cursor = index + 2;
            } else {
                break;
            }
        }
        sb.append(input.substring(cursor));

        return sb.toString();
    }

    static {
        map = new HashMap<Character, Character>();
        map.put('b', '\b');
        map.put('t', '\t');
        map.put('n', '\n');
        map.put('f', '\f');
        map.put('r', '\r');
        map.put('"', '\"');
        map.put('\'', '\'');
        map.put('\\', '\\');
    }
}
