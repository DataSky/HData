package opensource.hdata.plugin.writer.hive;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class HiveRecordWritable implements Writable {

    public void write(DataOutput dataOutput) throws IOException {
        throw new UnsupportedOperationException("no write");
    }

    public void readFields(DataInput dataInput) throws IOException {
        throw new UnsupportedOperationException("no read");
    }

}
