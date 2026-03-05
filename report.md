# Code Review Report: go-rabbitmq-consumers

**Дата:** 2026-03-05  
**Проект:** `github.com/KoNekoD/go-rabbitmq-consumers`  
**Go:** 1.23.6

---

## Сводка

| Severity | Кол-во |
|----------|--------|
| 🔴 Critical | 4 |
| 🟠 High | 3 |
| 🟡 Medium | 5 |
| 🔵 Low | 3 |

Проект компилируется, `go vet` проходит без ошибок. Основные проблемы — в runtime-логике `consumer.go`.

---

## 🔴 Critical

### C1. Deadlock: `mu.Unlock()` не вызывается при `break` из цикла (`consumer.go:280–340`)

В цикле `for msg := range delivery` мьютекс `mu.Lock()` захватывается в начале каждой итерации, но **все `break`-выходы из цикла не вызывают `mu.Unlock()`**.

```go
for msg := range delivery {
    mu.Lock()

    if stopNeeded {
        break // ← mu остаётся залоченным
    }

    err = json.Unmarshal(msg.Body, &job)
    if err != nil {
        // ...
        break // ← mu остаётся залоченным
    }

    err = c.handleFunc(job)
    if err != nil {
        // ...
        break // ← mu остаётся залоченным (несколько путей)
    }

    err = msg.Ack(false)
    if err != nil {
        // ...
        break // ← mu остаётся залоченным
    }

    mu.Unlock()
}
```

**Последствия:** После выхода из цикла горутина-остановщик (строки 266–276) пытается захватить `mu.Lock()` → вечный deadlock. Даже если горутина уже отработала, мьютекс навсегда остаётся залоченным — goroutine leak при повторном вызове `Stop()`.

---

### C2. Consumer останавливается после первой ошибки обработки (`consumer.go:300–325`)

Когда `handleFunc` возвращает ошибку, код выполняет retry-логику (публикация в delay queue, Ack оригинального сообщения), а затем делает **`break`**. Консьюмер полностью прекращает работу после **одного** неудачного сообщения.

```go
err = c.handleFunc(job)
if err != nil {
    // ... retry logic ...
    if ackErr := msg.Ack(false); ackErr != nil {
        // ...
        break
    }
    mu.Unlock()
    break // ← consumer полностью останавливается
}
```

**Ожидаемое поведение:** `continue` — продолжить обработку следующих сообщений.

---

### C3. Consumer останавливается при ошибке десериализации (`consumer.go:290–295`)

При ошибке `json.Unmarshal` — `break` без Nack/Ack. Консьюмер останавливается, сообщение остаётся unacknowledged. После закрытия соединения RabbitMQ будет повторно доставлять это сообщение → бесконечный цикл перезапуск-падение.

```go
err = json.Unmarshal(msg.Body, &job)
if err != nil {
    multiErr = multierror.Append(multiErr, errors.WithStack(err))
    break // ← нет Nack(false, false) или Reject, нет continue
}
```

---

### C4. Goroutine leak (`consumer.go:265–276`)

Горутина, слушающая `c.stopNeeded`, продолжает жить после возврата из `Init()`, если `Init()` завершился по ошибке (а не по `Stop()`). Она держит ссылки на `channel`, `mu`, `multiErr` — ресурсы, которые уже невалидны.

```go
go func() {
    <-c.stopNeeded // ← будет ждать вечно, если Stop() не вызван
    mu.Lock()
    // ...
}()
```

Если `Init()` вернул ошибку, а затем вызывается `Stop()`, горутина проснётся и попытается работать с закрытым каналом.

---

## 🟠 High

### H1. Race condition на `multiErr` (`consumer.go`)

`multiErr` разделяется между основной горутиной (цикл обработки) и горутиной-остановщиком. После выхода из цикла основная горутина пишет в `multiErr` (строки 340–355) **без захвата `mu`**, в то время как горутина-остановщик может параллельно писать в `multiErr` **с захватом `mu`**. Гонка данных.

---

### H2. Новое соединение на каждое сообщение — `Pusher.Push()` (`rabbitmq_pusher.go:162`)

`Push()` создаёт новое AMQP-соединение на **каждый** вызов. AMQP-соединение — это TCP-соединение с TLS-хэндшейком и AMQP-хэндшейком. Это крайне дорого.

---

### H3. Новое соединение на каждый delayed publish (`delay_publisher.go:101`)

`PublishDelayedJSON()` также создаёт новое соединение на каждый вызов. В контексте retry-логики это усугубляется: при ошибке обработки сообщения создаётся дополнительное соединение для delayed publish.

---

## 🟡 Medium

### M1. Двойной `channel.Cancel` (`consumer.go`)

Горутина-остановщик вызывает `channel.Cancel(c.consumerTag, false)` (строка 272). После выхода из цикла тот же `channel.Cancel` вызывается повторно (строка 340). Двойной Cancel может вернуть ошибку, которая попадёт в `multiErr`, засоряя реальные ошибки.

---

### M2. `int32` overflow в delay publisher (`delay_publisher.go:121–125`)

```go
ttl := int32(delayMs)
expires := int32(delayMs + int(p.queueExpireGrace.Milliseconds()))
```

`delayMs` — `int` (64 бита на 64-bit платформе). Значения > 2,147,483,647 мс (~24.8 дней) приведут к silent overflow при конвертации в `int32`.

---

### M3. Deprecated `channel.Publish` (`rabbitmq_pusher.go:194`)

`channel.Publish` deprecated в `amqp091-go`. Следует использовать `channel.PublishWithContext(ctx, ...)`.

---

### M4. `Push()` не принимает `context.Context` (`rabbitmq_pusher.go:162`)

Публичный метод, выполняющий I/O, не принимает `context.Context`. Нарушает Go best practices: невозможно задать таймаут или отменить операцию.

---

### M5. Хрупкий парсинг DSN (`util.go:14–30`)

`unwrapQueueFromDSN` разбирает DSN по `/` с хардкодом `i == 4 && len(splitDsn) == 5`. Не обрабатывает:
- DSN без имени очереди (менее 5 сегментов) — `queueName` будет пустой строкой, ошибка не возвращается.
- DSN с закодированными символами в vhost (`%2F`).
- DSN с портом, но без explicit vhost.

Нет валидации, нет возврата ошибки.

---

## 🔵 Low

### L1. Нет тестов

В проекте отсутствуют `_test.go` файлы. Критичная логика (deadlock, retry, DSN-парсинг, backoff) не покрыта тестами.

---

### L2. Нет логирования

Ошибки молча накапливаются в `multiErr` и возвращаются при завершении. Во время работы консьюмера нет возможности наблюдать, что происходит. Рекомендуется принимать `*slog.Logger` или использовать `log/slog`.

---

### L3. `github.com/pkg/errors` не поддерживается

Пакет фактически заброшен. Стандартная библиотека Go (начиная с 1.13) поддерживает `fmt.Errorf("%w", err)` и `errors.Is`/`errors.As`. Рекомендуется миграция.

---

## Детальный обзор `consumer.go`

### Архитектурные замечания

1. **`Init()` — блокирующий метод.** Вызывающий код должен запускать его в отдельной горутине. Это неочевидно из сигнатуры `Init() error` и не задокументировано.

2. **Нет reconnection-логики.** При обрыве AMQP-соединения `delivery` канал закроется, цикл завершится, `Init()` вернёт ошибку (или nil). Консьюмер не переподключается автоматически.

3. **Структура цикла обработки.** Текущая логика:
   - Один `break` по любой ошибке → полная остановка.
   - Мьютекс используется для координации с горутиной-остановщиком, но lock/unlock не сбалансированы.
   - `multiErr` как аккумулятор ошибок — после первого `break` все последующие ошибки (cleanup) добавляются туда же.

4. **`SetupConsumerChild` — неидиоматичный паттерн.** Функция принимает `parent *AbstractConsumer[JobType]` и мутирует его, устанавливая `handleFunc`. Неочевидно, зачем нужна generic-функция, если она просто вызывает `SetHandleFunc`.

### Рекомендуемые исправления для `consumer.go`

```go
// Вместо текущего цикла:
for msg := range delivery {
    mu.Lock()
    if stopNeeded {
        mu.Unlock()
        break
    }
    mu.Unlock()

    var job JobType
    if err := json.Unmarshal(msg.Body, &job); err != nil {
        _ = msg.Nack(false, false) // reject без requeue
        // log error
        continue
    }

    if err := c.handleFunc(job); err != nil {
        // retry logic...
        _ = msg.Ack(false)
        continue
    }

    if err := msg.Ack(false); err != nil {
        // log error
        continue
    }
}
```

---

## Сводная таблица по файлам

| Файл | Critical | High | Medium | Low |
|------|----------|------|--------|-----|
| `consumer.go` | 4 | 1 | 1 | — |
| `delay_publisher.go` | — | 1 | 1 | — |
| `rabbitmq_pusher.go` | — | 1 | 2 | — |
| `util.go` | — | — | 1 | — |
| Проект в целом | — | — | — | 3 |
