package com.rx;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

final class CompositeDisposable implements Disposable {
    private final AtomicBoolean disposed = new AtomicBoolean(false);
    private final List<Disposable> disposables = new CopyOnWriteArrayList<>();

    void add(Disposable disposable) {
        if (disposed.get()) {
            disposable.dispose();
        } else {
            disposables.add(disposable);
            if (disposed.get()) {
                disposable.dispose();
            }
        }
    }

    @Override
    public void dispose() {
        if (disposed.compareAndSet(false, true)) {
            disposables.forEach(Disposable::dispose);
            disposables.clear();
        }
    }

    @Override
    public boolean isDisposed() {
        return disposed.get();
    }
}
