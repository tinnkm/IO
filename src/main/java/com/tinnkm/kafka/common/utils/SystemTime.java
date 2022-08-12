package com.tinnkm.kafka.common.utils;

import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

public class SystemTime implements Time {

    @Override
    public long milliseconds() {
        return System.currentTimeMillis();
    }

    @Override
    public long nanoseconds() {
        return System.nanoTime();
    }

    @Override
    public void sleep(long ms) {
        Utils.sleep(ms);
    }

    @Override
    public void waitObject(Object obj, Supplier<Boolean> condition, long deadlineMs) throws InterruptedException {
        synchronized (obj) {
            while (true) {
                if (condition.get())
                    return;

                long currentTimeMs = milliseconds();
                if (currentTimeMs >= deadlineMs)
                    throw new InterruptedException("Condition not satisfied before deadline");

                obj.wait(deadlineMs - currentTimeMs);
            }
        }
    }

}
