package opensource.hdata.plugin.writer.console;

import opensource.hdata.core.plugin.Record;
import opensource.hdata.core.plugin.Writer;

public class ConsoleWriter extends Writer {

    @Override
    public void execute(Record record) {
        System.out.println(record);
    }
}
