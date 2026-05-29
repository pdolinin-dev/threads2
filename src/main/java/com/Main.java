package com;

import com.rx.Observable;
import com.rx.Observer;
import com.rx.SingleThreadScheduler;

import java.util.concurrent.CountDownLatch;

public class Main {
    public static void main(String[] args) throws InterruptedException {
        SingleThreadScheduler scheduler = new SingleThreadScheduler();
        CountDownLatch completed = new CountDownLatch(1);

        Observable.<Integer>create(emitter -> {
                    for (int i = 1; i <= 5 && !emitter.isDisposed(); i++) {
                        emitter.onNext(i);
                    }
                    emitter.onComplete();
                })
                .filter(value -> value % 2 == 1)
                .map(value -> "value=" + value + ", thread=" + Thread.currentThread().getName())
                .observeOn(scheduler)
                .subscribe(new Observer<>() {
                    @Override
                    public void onNext(String item) {
                        System.out.println(item);
                    }

                    @Override
                    public void onError(Throwable t) {
                        t.printStackTrace();
                        completed.countDown();
                    }

                    @Override
                    public void onComplete() {
                        System.out.println("complete");
                        completed.countDown();
                    }
                });

        completed.await();
        scheduler.shutdown();
    }
}
