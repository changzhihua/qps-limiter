package com.github.bruce.limiter;

import com.github.bruce.utils.Preconditions;

import java.util.concurrent.atomic.DoubleAdder;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class LeakyBucket implements RateLimiter {
    private final long rate;
    private volatile long lastUpdateTime;
    private DoubleAdder water;
    private final Lock lock;

    public LeakyBucket(long rate) {
        Preconditions.checkArgument(rate > 0, "The rate parameter must be a number greater than 0");
        this.rate = rate;
        water = new DoubleAdder();
        lock = new ReentrantLock();
        this.lastUpdateTime = now();
    }

    private long now() {
        return System.currentTimeMillis();
    }

    @Override
    public boolean allow(String name) {
        return allow();
    }

    @Override
    public boolean allow() {
        update();
        if (water.sum() > rate) {
            return false;
        }
        water.add(1);
        return true;
    }

    private void update() {
        long now = now();
        if (now - lastUpdateTime < 100) {
            return;
        }
        if (lock.tryLock()) {
            long elapsedTime = now - lastUpdateTime;
            lastUpdateTime = now;
            double elapsedWater = (double)elapsedTime * rate / 1000;
            if (water.sum() > elapsedWater) {
                water.add(-elapsedWater);
            }
            else {
                water.reset();
            }
            lock.unlock();
        }

    }

}
