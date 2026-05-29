package com.rx;

import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;

public class Observable<T> {
    private final ObservableOnSubscribe<T> source;

    private Observable(ObservableOnSubscribe<T> source) {
        this.source = source;
    }

    public static <T> Observable<T> create(ObservableOnSubscribe<T> source) {
        return new Observable<>(Objects.requireNonNull(source));
    }

    public Disposable subscribe(Observer<? super T> observer) {
        Objects.requireNonNull(observer);
        SimpleDisposable disposable = new SimpleDisposable();
        SafeEmitter<T> emitter = new SafeEmitter<>(observer, disposable);
        try {
            source.subscribe(emitter);
        } catch (Throwable t) {
            emitter.onError(t);
        }
        return disposable;
    }

    public <R> Observable<R> map(Function<? super T, ? extends R> mapper) {
        Objects.requireNonNull(mapper);
        return create(emitter -> subscribe(new Observer<>() {
            @Override
            public void onNext(T item) {
                if (!emitter.isDisposed()) {
                    emitter.onNext(mapper.apply(item));
                }
            }

            @Override
            public void onError(Throwable t) {
                emitter.onError(t);
            }

            @Override
            public void onComplete() {
                emitter.onComplete();
            }
        }));
    }

    public Observable<T> filter(Predicate<? super T> predicate) {
        Objects.requireNonNull(predicate);
        return create(emitter -> subscribe(new Observer<>() {
            @Override
            public void onNext(T item) {
                if (!emitter.isDisposed() && predicate.test(item)) {
                    emitter.onNext(item);
                }
            }

            @Override
            public void onError(Throwable t) {
                emitter.onError(t);
            }

            @Override
            public void onComplete() {
                emitter.onComplete();
            }
        }));
    }

    public <R> Observable<R> flatMap(Function<? super T, Observable<? extends R>> mapper) {
        Objects.requireNonNull(mapper);
        return create(emitter -> {
            CompositeDisposable composite = new CompositeDisposable();
            AtomicInteger active = new AtomicInteger(1);
            AtomicReference<Throwable> firstError = new AtomicReference<>();

            Runnable completeIfDone = () -> {
                if (active.decrementAndGet() == 0 && !emitter.isDisposed() && firstError.get() == null) {
                    emitter.onComplete();
                }
            };

            composite.add(subscribe(new Observer<>() {
                @Override
                public void onNext(T item) {
                    if (emitter.isDisposed() || firstError.get() != null) {
                        return;
                    }

                    Observable<R> inner;
                    try {
                        @SuppressWarnings("unchecked")
                        Observable<R> typedInner = (Observable<R>) Objects.requireNonNull(mapper.apply(item));
                        inner = typedInner;
                    } catch (Throwable t) {
                        onError(t);
                        return;
                    }

                    active.incrementAndGet();
                    Disposable innerDisposable = inner.subscribe(new Observer<>() {
                        @Override
                        public void onNext(R value) {
                            if (!emitter.isDisposed() && firstError.get() == null) {
                                emitter.onNext(value);
                            }
                        }

                        @Override
                        public void onError(Throwable t) {
                            if (firstError.compareAndSet(null, t)) {
                                composite.dispose();
                                emitter.onError(t);
                            }
                        }

                        @Override
                        public void onComplete() {
                            completeIfDone.run();
                        }
                    });
                    composite.add(innerDisposable);
                }

                @Override
                public void onError(Throwable t) {
                    if (firstError.compareAndSet(null, t)) {
                        composite.dispose();
                        emitter.onError(t);
                    }
                }

                @Override
                public void onComplete() {
                    completeIfDone.run();
                }
            }));
        });
    }

    public Observable<T> subscribeOn(Scheduler scheduler) {
        Objects.requireNonNull(scheduler);
        return create(emitter -> scheduler.execute(() -> {
            if (emitter.isDisposed()) {
                return;
            }
            subscribe(new Observer<>() {
                @Override
                public void onNext(T item) {
                    emitter.onNext(item);
                }

                @Override
                public void onError(Throwable t) {
                    emitter.onError(t);
                }

                @Override
                public void onComplete() {
                    emitter.onComplete();
                }
            });
        }));
    }

    public Observable<T> observeOn(Scheduler scheduler) {
        Objects.requireNonNull(scheduler);
        return create(emitter -> {
            Queue<Runnable> queue = new ConcurrentLinkedQueue<>();
            AtomicInteger workInProgress = new AtomicInteger();

            Runnable[] drain = new Runnable[1];
            drain[0] = () -> {
                int missed = 1;
                while (true) {
                    Runnable task;
                    while ((task = queue.poll()) != null) {
                        task.run();
                    }
                    missed = workInProgress.addAndGet(-missed);
                    if (missed == 0) {
                        break;
                    }
                }
            };

            Function<Runnable, Runnable> enqueue = task -> () -> {
                queue.offer(task);
                if (workInProgress.getAndIncrement() == 0) {
                    scheduler.execute(drain[0]);
                }
            };

            subscribe(new Observer<>() {
                @Override
                public void onNext(T item) {
                    enqueue.apply(() -> emitter.onNext(item)).run();
                }

                @Override
                public void onError(Throwable t) {
                    enqueue.apply(() -> emitter.onError(t)).run();
                }

                @Override
                public void onComplete() {
                    enqueue.apply(emitter::onComplete).run();
                }
            });
        });
    }
}
