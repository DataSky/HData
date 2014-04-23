package opensource.hdata;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import opensource.hdata.config.JobConfig;
import opensource.hdata.config.PluginConfig;
import opensource.hdata.core.HData;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

public class CliDriver {

    private static final String XML_FILE = "f";
    private static final String HDATA_VARS = "var";

    /**
     * 创建命令行选项
     * 
     * @return
     */
    public Options createOptions() {
        Options options = new Options();
        options.addOption(XML_FILE, null, true, "job xml path");
        OptionBuilder.withValueSeparator();
        OptionBuilder.hasArgs(2);
        OptionBuilder.withArgName("property=value");
        OptionBuilder.withLongOpt(HDATA_VARS);
        options.addOption(OptionBuilder.create());
        return options;
    }

    /**
     * 打印命令行帮助信息
     * 
     * @param options
     */
    public void printHelp(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(" ", options);
    }

    /**
     * 替换命令行变量
     * 
     * @param config
     * @param vars
     */
    public void replaceConfigVars(PluginConfig config, Map<String, String> vars) {
        for (Entry<Object, Object> confEntry : config.entrySet()) {
            if (confEntry.getKey().getClass() == String.class && confEntry.getValue().getClass() == String.class) {
                for (Entry<String, String> varEntry : vars.entrySet()) {
                    String replaceVar = "${" + varEntry.getKey() + "}";
                    if (confEntry.getValue().toString().contains(replaceVar)) {
                        config.put(confEntry.getKey(), confEntry.getValue().toString().replace(replaceVar, varEntry.getValue()));
                    }
                }
            }
        }
    }

    /**
     * 主程序入口
     * 
     * @param args
     */
    public static void main(String[] args) {
        CliDriver cliDriver = new CliDriver();
        Options options = cliDriver.createOptions();
        if (args.length < 1) {
            cliDriver.printHelp(options);
            System.exit(-1);
        }

        CommandLineParser parser = new PosixParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
            String jobXmlPath = cmd.getOptionValue(XML_FILE);
            JobConfig jobConfig = new JobConfig(jobXmlPath);
            Map<String, String> vars = new HashMap<String, String>();
            Properties properties = cmd.getOptionProperties(HDATA_VARS);
            for (String key : properties.stringPropertyNames()) {
                vars.put(key, properties.getProperty(key));
            }

            final PluginConfig readerConfig = jobConfig.getReaderConfig();
            final PluginConfig writerConfig = jobConfig.getWriterConfig();

            cliDriver.replaceConfigVars(readerConfig, vars);
            cliDriver.replaceConfigVars(writerConfig, vars);

            HData hData = new HData();
            hData.start(jobConfig);
        } catch (ParseException e) {
            cliDriver.printHelp(options);
            System.exit(-1);
        }
    }
}
