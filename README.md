# RxJava, реализация аналогичной RxJava-библиотеки

## Архитектура

Проект представялет собой реактивную библиотеку в пакете `com.rx`.

- `Observer<T>` принимает события потока `onNext`, `onError`, `onComplete`.
- `Observable<T>` хранит источник данных и предоставляет `subscribe`, `create`, `map`, `filter`, `flatMap`, `subscribeOn`, `observeOn`.
- `Emitter<T>` передается источнику и позволяет отправлять элементы, ошибку или завершение.
- `Disposable` отменяет подписку. Реализация проверяется перед отправкой событий, поэтому источник может остановить генерацию через `emitter.isDisposed()`.
- `SafeEmitter` защищает контракт потока т.к. после ошибки или завершения новые события не доставляются.

Ошибки, возникшие внутри источника или операторов, передаются в `onError`. После терминального события подписка отменяется

## Операторы

- `map` применяет `Function` к каждому элементу и отправляет результат дальше.
- `filter` применяет `Predicate` и пропускает только подходящие элементы.
- `flatMap` преобразует каждый элемент в новый `Observable` и объединяет элементы внутренних потоков в один результирующий поток. Первая ошибка завершает всю цепочку через `onError`.

## Schedulers

`Scheduler` содержит метод `execute(Runnable task)`. Он отделяет описание реактивной цепочки от конкретного способа выполнения задач.

- `IOThreadScheduler` использует `CachedThreadPool`. Подходит для I/O-задач, где потоки часто ждут сеть, файл или внешние сервисы.
- `ComputationScheduler` использует `FixedThreadPool` по числу процессоров. Подходит для CPU-bound вычислений.
- `SingleThreadScheduler` использует один поток. Подходит для последовательной обработки и сценариев, где важен строгий порядок.

`subscribeOn` переносит выполнение подписки и источника данных на выбранный scheduler. `observeOn` переносит доставку событий наблюдателю на выбранный scheduler и сериализует события, чтобы `onNext`, `onError`, `onComplete` не обгоняли друг друга.

## Тестирование

Юнит-тесты покрывают:

- базовую подписку и завершение;
- операторы `map` и `filter`;
- объединение потоков через `flatMap`;
- передачу ошибок в `onError`;
- отмену подписки через `Disposable`;
- выполнение источника через `subscribeOn`;
- доставку событий через `observeOn`;
- работу `SingleThreadScheduler` в одном потоке.

Запуск:

```bash
mvn test
```

## Пример использования

```java
SingleThreadScheduler scheduler = new SingleThreadScheduler();

Observable.<Integer>create(emitter -> {
    emitter.onNext(1);
    emitter.onNext(2);
    emitter.onNext(3);
    emitter.onComplete();
})
    .filter(value -> value % 2 == 1)
    .map(value -> "item=" + value)
    .observeOn(scheduler)
    .subscribe(new Observer<>() {
        public void onNext(String item) {
            System.out.println(item);
        }

        public void onError(Throwable t) {
            t.printStackTrace();
        }

        public void onComplete() {
            System.out.println("complete");
        }
    });
```
