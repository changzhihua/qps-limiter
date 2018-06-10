package com.github.bruce.limiter;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.LongAdder;

public class DistributedRateLimiter implements RateLimiter {
    private static final int DEFAULT_RATE = 2000;
    private static final int ALARM_THRESHOLD = 2;
    private static final int REFUSE_THRESHOLD = 3;
    /**
     * 多个调用方的最大qps配置
     */
    private Map<String, Integer> rates;
    /**
     * 每个调用方当前本地的请求次数
     */
    private ConcurrentHashMap<String, LongAdder> currentRates;
    /**
     * 每个调用方近似全局的请求次数
     */
    private Map<String, Long> snapshots;
    private int defaultRate;

    private ScheduledExecutorService executePool;
    private JedisPool jedisPool;

    public DistributedRateLimiter(Map<String, Integer> callers, JedisPool jedisPool) {
        this(callers, DEFAULT_RATE, jedisPool);
    }

    public DistributedRateLimiter(Map<String, Integer> rates, int defaultRate, JedisPool jedisPool) {
        this.rates = rates;
        this.defaultRate = defaultRate;
        currentRates = new ConcurrentHashMap<>();
        snapshots = new HashMap<>();
        this.jedisPool = jedisPool;
        init();
    }

    private void init() {
        for (String name : rates.keySet()) {
            currentRates.put(name, new LongAdder());
            snapshots.put(name, 0L);
        }
        executePool = Executors.newScheduledThreadPool(1);
        executePool.scheduleAtFixedRate(new Updater(), 50, 50, TimeUnit.MILLISECONDS);
    }

    @Override
    public boolean allow() {
        throw new UnsupportedOperationException("The distributed limiter not supports this method");
    }
    @Override
    public boolean allow(String name) {
        int limit = rates.getOrDefault(name, defaultRate);
        LongAdder localCount = currentRates.get(name);
        localCount.increment();
        long alarmLimit = ALARM_THRESHOLD * limit;
        long refuseLimit = REFUSE_THRESHOLD * limit;
        Long remoteCount = snapshots.getOrDefault(name, 0L);
        long total = localCount.sum() + remoteCount;
        if (total > alarmLimit) {
            // do alarm 进行报警
        }
        return total <= refuseLimit;
    }

    private String makeKey(String name) {
        return name + "_" + Instant.now().getEpochSecond();
    }

    public long getRate(String name) {
        return currentRates.get(name).sum() + snapshots.get(name);
    }

    class Updater implements Runnable {

        @Override
        public void run() {
            Jedis jedis = null;
            try {
                jedis = jedisPool.getResource();
                Map<String, Response<Long>> responseMap = new HashMap<>();
                Pipeline pipeline = jedis.pipelined();
                String key;
                for (Map.Entry<String, LongAdder> entry : currentRates.entrySet()) {
                    key = makeKey(entry.getKey());
                    responseMap.put(entry.getKey(), pipeline.incrBy(key, entry.getValue().sum()));
                    pipeline.expire(key, 5);
                }
                pipeline.sync();
                for (Map.Entry<String, Response<Long>> entry : responseMap.entrySet()) {
                    String caller = entry.getKey();
                    long value = entry.getValue().get();
                    snapshots.put(caller, value);
                    currentRates.get(caller).reset();

                }
            }
            catch (Exception e) {
                //throw e;
            }
            finally {
                if (jedis != null) {
                    jedis.close();
                }
            }
        }

    }
}

