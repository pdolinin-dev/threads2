package com.rx;

public interface Scheduler {
    void execute(Runnable task);

    default void shutdown() {
    }
}
