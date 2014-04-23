package opensource.hdata.core;

import opensource.hdata.exception.HDataException;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.YieldingWaitStrategy;

public class WaitStrategyFactory {

    /**
     * 构造线程等待策略
     * 
     * @param name
     * @return
     */
    public static WaitStrategy build(String name) {
        WaitStrategy waitStrategy = null;
        if ("BlockingWaitStrategy".equals(name)) {
            waitStrategy = new BlockingWaitStrategy();
        } else if ("BusySpinWaitStrategy".equals(name)) {
            waitStrategy = new BusySpinWaitStrategy();
        } else if ("SleepingWaitStrategy".equals(name)) {
            waitStrategy = new SleepingWaitStrategy();
        } else if ("YieldingWaitStrategy".equals(name)) {
            waitStrategy = new YieldingWaitStrategy();
        } else {
            throw new HDataException("Invalid wait strategy: " + name);
        }
        return waitStrategy;
    }
}
