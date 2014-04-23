package opensource.hdata.core;

import opensource.hdata.core.plugin.Record;

import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;

public class Storage {

    private Disruptor<RecordEvent> disruptor;
    private RingBuffer<RecordEvent> ringBuffer;

    private static final EventTranslatorOneArg<RecordEvent, Record> TRANSLATOR = new EventTranslatorOneArg<RecordEvent, Record>() {

        public void translateTo(RecordEvent event, long sequence, Record record) {
            event.setRecord(record);
        }
    };

    public Storage(Disruptor<RecordEvent> disruptor, RecordWorkHandler[] handlers) {
        this.disruptor = disruptor;
        disruptor.handleEventsWithWorkerPool(handlers);
        ringBuffer = disruptor.start();
    }

    public void put(Record record) {
        disruptor.publishEvent(TRANSLATOR, record);
    }

    public void put(Record[] records) {
        for (Record record : records) {
            put(record);
        }
    }

    public boolean isEmpty() {
        return ringBuffer.remainingCapacity() == ringBuffer.getBufferSize();
    }

    public int size() {
        return ringBuffer.getBufferSize();
    }

    public void close() {
        disruptor.shutdown();
    }

}
