package com.github.pushingice;

import java.util.concurrent.atomic.AtomicLong;

public class IDCounter {

    public static AtomicLong count = new AtomicLong(1);

    public static long next() {
        return count.getAndIncrement();
    }

}
