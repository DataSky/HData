package opensource.hdata.core.plugin;

public abstract class AbstractPlugin implements Pluginable {

    private String pluginName;

    public String getPluginName() {
        return this.pluginName;
    }

    public void setPluginName(String name) {
        this.pluginName = name;
    }

}
