package com.rx;

import java.util.concurrent.atomic.AtomicBoolean;

final class SafeEmitter<T> implements Emitter<T>, Disposable {
    private final Observer<? super T> observer;
    private final AtomicBoolean terminated = new AtomicBoolean(false);
    private final Disposable upstream;

    SafeEmitter(Observer<? super T> observer, Disposable upstream) {
        this.observer = observer;
        this.upstream = upstream;
    }

    @Override
    public void onNext(T item) {
        if (!isDisposed() && !terminated.get()) {
            try {
                observer.onNext(item);
            } catch (Throwable t) {
                onError(t);
            }
        }
    }

    @Override
    public void onError(Throwable t) {
        if (!isDisposed() && terminated.compareAndSet(false, true)) {
            try {
                observer.onError(t);
            } finally {
                dispose();
            }
        }
    }

    @Override
    public void onComplete() {
        if (!isDisposed() && terminated.compareAndSet(false, true)) {
            try {
                observer.onComplete();
            } finally {
                dispose();
            }
        }
    }

    @Override
    public void dispose() {
        upstream.dispose();
    }

    @Override
    public boolean isDisposed() {
        return upstream.isDisposed();
    }
}
