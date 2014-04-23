package opensource.hdata.core;

import java.util.concurrent.atomic.AtomicLong;

public class Metric {

    private AtomicLong readCount = new AtomicLong(0);
    private AtomicLong writeCount = new AtomicLong(0);
    private long readerStartTime;
    private long readerEndTime;
    private long writerStartTime;
    private long writerEndTime;

    public AtomicLong getReadCount() {
        return readCount;
    }

    public void setReadCount(AtomicLong readCount) {
        this.readCount = readCount;
    }

    public AtomicLong getWriteCount() {
        return writeCount;
    }

    public void setWriteCount(AtomicLong writeCount) {
        this.writeCount = writeCount;
    }

    public long getReaderStartTime() {
        return readerStartTime;
    }

    public void setReaderStartTime(long readerStartTime) {
        this.readerStartTime = readerStartTime;
    }

    public long getReaderEndTime() {
        return readerEndTime;
    }

    public void setReaderEndTime(long readerEndTime) {
        this.readerEndTime = readerEndTime;
    }

    public long getWriterStartTime() {
        return writerStartTime;
    }

    public void setWriterStartTime(long writerStartTime) {
        this.writerStartTime = writerStartTime;
    }

    public long getWriterEndTime() {
        return writerEndTime;
    }

    public void setWriterEndTime(long writerEndTime) {
        this.writerEndTime = writerEndTime;
    }

}
