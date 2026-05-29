package com.rx;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ComputationScheduler implements Scheduler {
    private final ExecutorService executor;

    public ComputationScheduler() {
        this(Runtime.getRuntime().availableProcessors());
    }

    public ComputationScheduler(int threads) {
        this.executor = Executors.newFixedThreadPool(Math.max(1, threads));
    }

    @Override
    public void execute(Runnable task) {
        executor.execute(task);
    }

    @Override
    public void shutdown() {
        executor.shutdownNow();
    }
}
