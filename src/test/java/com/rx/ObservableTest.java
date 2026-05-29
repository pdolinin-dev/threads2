package com.rx;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ObservableTest {
    @Test
    void subscribeReceivesItemsAndCompletion() {
        List<Integer> values = new ArrayList<>();
        AtomicBoolean completed = new AtomicBoolean(false);

        Observable.<Integer>create(emitter -> {
            emitter.onNext(1);
            emitter.onNext(2);
            emitter.onComplete();
        }).subscribe(new TestObserver<>() {
            @Override
            public void onNext(Integer item) {
                values.add(item);
            }

            @Override
            public void onComplete() {
                completed.set(true);
            }
        });

        assertEquals(List.of(1, 2), values);
        assertTrue(completed.get());
    }

    @Test
    void mapAndFilterTransformStream() {
        List<String> values = new ArrayList<>();

        Observable.<Integer>create(emitter -> {
                    for (int i = 1; i <= 5; i++) {
                        emitter.onNext(i);
                    }
                    emitter.onComplete();
                })
                .filter(value -> value % 2 == 0)
                .map(value -> "n=" + value)
                .subscribe(new TestObserver<>() {
                    @Override
                    public void onNext(String item) {
                        values.add(item);
                    }
                });

        assertEquals(List.of("n=2", "n=4"), values);
    }

    @Test
    void flatMapMergesInnerObservables() {
        List<Integer> values = new ArrayList<>();
        AtomicBoolean completed = new AtomicBoolean(false);

        Observable.<Integer>create(emitter -> {
                    emitter.onNext(1);
                    emitter.onNext(2);
                    emitter.onComplete();
                })
                .flatMap(value -> Observable.<Integer>create(inner -> {
                    inner.onNext(value);
                    inner.onNext(value * 10);
                    inner.onComplete();
                }))
                .subscribe(new TestObserver<>() {
                    @Override
                    public void onNext(Integer item) {
                        values.add(item);
                    }

                    @Override
                    public void onComplete() {
                        completed.set(true);
                    }
                });

        assertEquals(List.of(1, 10, 2, 20), values);
        assertTrue(completed.get());
    }

    @Test
    void sourceErrorsAreDeliveredToObserver() {
        IllegalStateException expected = new IllegalStateException("boom");
        AtomicReference<Throwable> actual = new AtomicReference<>();

        Observable.create(emitter -> {
            throw expected;
        }).subscribe(new TestObserver<>() {
            @Override
            public void onError(Throwable t) {
                actual.set(t);
            }
        });

        assertEquals(expected, actual.get());
    }

    @Test
    void mapperErrorsAreDeliveredToObserver() {
        AtomicReference<Throwable> actual = new AtomicReference<>();

        Observable.<Integer>create(emitter -> {
                    emitter.onNext(1);
                    emitter.onComplete();
                })
                .map(value -> {
                    throw new IllegalArgumentException("bad map");
                })
                .subscribe(new TestObserver<>() {
                    @Override
                    public void onError(Throwable t) {
                        actual.set(t);
                    }
                });

        assertNotNull(actual.get());
        assertEquals("bad map", actual.get().getMessage());
    }

    @Test
    void disposableStopsAsyncSource() throws InterruptedException {
        CountDownLatch firstItem = new CountDownLatch(1);
        AtomicBoolean completed = new AtomicBoolean(false);

        Disposable disposable = Observable.<Integer>create(emitter -> {
            Thread worker = new Thread(() -> {
                int value = 0;
                while (!emitter.isDisposed()) {
                    emitter.onNext(value++);
                }
                emitter.onComplete();
            });
            worker.start();
        }).subscribe(new TestObserver<>() {
            @Override
            public void onNext(Integer item) {
                firstItem.countDown();
            }

            @Override
            public void onComplete() {
                completed.set(true);
            }
        });

        assertTrue(firstItem.await(1, TimeUnit.SECONDS));
        disposable.dispose();

        assertTrue(disposable.isDisposed());
        assertFalse(completed.get());
    }

    @Test
    void subscribeOnRunsSourceOnSchedulerThread() throws InterruptedException {
        SingleThreadScheduler scheduler = new SingleThreadScheduler();
        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<String> sourceThread = new AtomicReference<>();
        String testThread = Thread.currentThread().getName();

        try {
            Observable.<Integer>create(emitter -> {
                        sourceThread.set(Thread.currentThread().getName());
                        emitter.onNext(1);
                        emitter.onComplete();
                    })
                    .subscribeOn(scheduler)
                    .subscribe(new TestObserver<>() {
                        @Override
                        public void onComplete() {
                            done.countDown();
                        }
                    });

            assertTrue(done.await(1, TimeUnit.SECONDS));
            assertNotEquals(testThread, sourceThread.get());
        } finally {
            scheduler.shutdown();
        }
    }

    @Test
    void observeOnRunsObserverOnSchedulerThread() throws InterruptedException {
        SingleThreadScheduler scheduler = new SingleThreadScheduler();
        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<String> observerThread = new AtomicReference<>();
        String testThread = Thread.currentThread().getName();

        try {
            Observable.<Integer>create(emitter -> {
                        emitter.onNext(1);
                        emitter.onComplete();
                    })
                    .observeOn(scheduler)
                    .subscribe(new TestObserver<>() {
                        @Override
                        public void onNext(Integer item) {
                            observerThread.set(Thread.currentThread().getName());
                        }

                        @Override
                        public void onComplete() {
                            done.countDown();
                        }
                    });

            assertTrue(done.await(1, TimeUnit.SECONDS));
            assertNotEquals(testThread, observerThread.get());
        } finally {
            scheduler.shutdown();
        }
    }

    @Test
    void singleThreadSchedulerExecutesTasksOnOneThread() throws InterruptedException {
        SingleThreadScheduler scheduler = new SingleThreadScheduler();
        CountDownLatch done = new CountDownLatch(3);
        List<String> threads = Collections.synchronizedList(new ArrayList<>());

        try {
            for (int i = 0; i < 3; i++) {
                scheduler.execute(() -> {
                    threads.add(Thread.currentThread().getName());
                    done.countDown();
                });
            }

            assertTrue(done.await(1, TimeUnit.SECONDS));
            assertEquals(1, threads.stream().distinct().count());
        } finally {
            scheduler.shutdown();
        }
    }

    private static class TestObserver<T> implements Observer<T> {
        @Override
        public void onNext(T item) {
        }

        @Override
        public void onError(Throwable t) {
        }

        @Override
        public void onComplete() {
        }
    }
}
