package opensource.hdata.core.plugin;

public interface Record {

    public void addField(Object field);

    public void addField(int index, Object field);

    public Object getField(int index);

    public int getFieldsCount();
}
