package opensource.hdata.config;

import java.util.Properties;

public abstract class Configuration extends Properties {

    private static final long serialVersionUID = 8606831740240321865L;

    public String getString(String key, String defalutValue) {
        String value = getProperty(key);
        return value != null ? value : defalutValue;
    }

    public String getString(String key) {
        return getProperty(key);
    }

    public void setString(String key, String value) {
        setProperty(key, value);
    }

    public int getInt(String key, int defalutValue) {
        String value = getProperty(key);
        return value != null ? Integer.parseInt(value) : defalutValue;
    }

    public void setInt(String key, int value) {
        setString(key, Integer.toString(value));
    }

    public long getLong(String key, long defalutValue) {
        String value = getProperty(key);
        return value != null ? Long.parseLong(value) : defalutValue;
    }

    public void setLong(String key, long value) {
        setString(key, Long.toString(value));
    }

    public double getDouble(String key, double defalutValue) {
        String value = getProperty(key);
        return value != null ? Double.parseDouble(value) : defalutValue;
    }

    public void setDouble(String key, double value) {
        setString(key, Double.toString(value));
    }

    public boolean getBoolean(String key, boolean defalutValue) {
        String value = getProperty(key);
        return value != null ? Boolean.parseBoolean(value) : defalutValue;
    }

    public void setBoolean(String key, boolean value) {
        setString(key, Boolean.toString(value));
    }

    public float getFloat(String key, float defalutValue) {
        String value = getProperty(key);
        return value != null ? Float.parseFloat(value) : defalutValue;
    }

    public void setFloat(String key, float value) {
        setString(key, Float.toString(value));
    }
}
