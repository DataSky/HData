package opensource.hdata.config;

import opensource.hdata.core.PluginLoader;
import opensource.hdata.core.plugin.Reader;
import opensource.hdata.core.plugin.Splitter;
import opensource.hdata.core.plugin.Writer;
import opensource.hdata.exception.HDataException;
import opensource.hdata.util.XMLUtils;

import org.jdom2.Element;

public class JobConfig extends Configuration {

    private Element root;
    private PluginConfig readerConfig;
    private PluginConfig writerConfig;
    private static final long serialVersionUID = -106497323171420503L;

    public JobConfig() {
        super();
    }

    public JobConfig(String jobXmlPath) {
        this();
        try {
            root = XMLUtils.load(jobXmlPath);
        } catch (Exception e) {
            throw new HDataException("Can not load job xml file: " + jobXmlPath, e);
        }
    }

    public PluginConfig getReaderConfig() {
        if (readerConfig == null) {
            readerConfig = new PluginConfig();
            for (Element e : root.getChild("reader").getChildren()) {
                if (!e.getValue().trim().isEmpty()) {
                    readerConfig.setProperty(e.getName(), e.getValue());
                }
            }
        }

        return readerConfig;
    }

    public PluginConfig getWriterConfig() {
        if (writerConfig == null) {
            writerConfig = new PluginConfig();
            for (Element e : root.getChild("writer").getChildren()) {
                if (!e.getValue().trim().isEmpty()) {
                    writerConfig.setProperty(e.getName(), e.getValue());
                }
            }
        }
        return writerConfig;
    }

    public String getReaderName() {
        return root.getChild("reader").getAttributeValue("name");
    }

    public String getReaderClassName() {
        return PluginLoader.getReaderPlugin(getReaderName()).getClassName();
    }

    public Reader newReader() {
        String readerClassName = getReaderClassName();
        if (readerClassName == null) {
            throw new HDataException("Can not find class for reader: " + getReaderName());
        }

        try {
            return (Reader) Class.forName(readerClassName).newInstance();
        } catch (Exception e) {
            throw new HDataException("Can not create new reader instance for: " + getReaderName(), e);
        }
    }

    public Splitter newSplitter() {
        String spliterClassName = PluginLoader.getReaderPlugin(getReaderName()).getSplitterClassName();

        if (spliterClassName == null) {
            return null;
        }

        try {
            return (Splitter) Class.forName(spliterClassName.trim()).newInstance();
        } catch (Exception e) {
            throw new HDataException("Can not find splitter for reader: " + getReaderName(), e);
        }
    }

    public String getWriterName() {
        return root.getChild("writer").getAttributeValue("name");
    }

    public String getWriterClassName() {
        return PluginLoader.getWriterPlugin(getWriterName()).getClassName();
    }

    public Writer newWriter() {
        String writerClassName = getWriterClassName();
        if (writerClassName == null) {
            throw new HDataException("Can not find class for writer: " + getWriterName());
        }

        try {
            return (Writer) Class.forName(getWriterClassName()).newInstance();
        } catch (Exception e) {
            throw new HDataException("Can not create new writer instance for: " + getWriterName(), e);
        }
    }
}
