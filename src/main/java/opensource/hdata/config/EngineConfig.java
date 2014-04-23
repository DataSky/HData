package opensource.hdata.config;

import java.util.List;

import opensource.hdata.common.Constants;
import opensource.hdata.exception.HDataException;
import opensource.hdata.util.Utils;
import opensource.hdata.util.XMLUtils;

import org.jdom2.Element;

public class EngineConfig extends Configuration {

    private static final long serialVersionUID = -4751544524691015405L;

    private EngineConfig() {
        super();
    }

    public static EngineConfig create() {
        EngineConfig conf = new EngineConfig();
        Element root = null;
        try {
            root = XMLUtils.load(Utils.getConfigDir() + Constants.HDATA_XML);
        } catch (Exception e) {
            throw new HDataException("Init EngineConf error!", e);
        }
        List<Element> list = root.getChildren("property");

        for (Element element : list) {
            conf.setString(element.getChildText("name"), element.getChildText("value"));
        }
        return conf;
    }

}
