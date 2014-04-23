package opensource.hdata.core.plugin;

public class ReaderPlugin extends AbstractPlugin {

    private String className;
    private String splitterClassName;

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public String getSplitterClassName() {
        return splitterClassName;
    }

    public void setSplitterClassName(String splitterClassName) {
        this.splitterClassName = splitterClassName;
    }

}
