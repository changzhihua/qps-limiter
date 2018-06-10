package com.github.bruce.limiter;


public interface RateLimiter {
    public boolean allow(String name);
    public boolean allow();
}
