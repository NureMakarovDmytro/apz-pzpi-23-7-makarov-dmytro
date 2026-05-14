
from __future__ import annotations

import logging
import random
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Iterator

# ──────────────────────────────────────────────
# Налаштування логування
# ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ──────────────────────────────────────────────
# Моделі даних
# ──────────────────────────────────────────────

@dataclass
class PostEvent:
    """Подія нового поста, що надходить з Kafka."""
    post_id: int
    author_id: int
    content: str
    timestamp: float = field(default_factory=time.time)


@dataclass
class FeedEntry:
    """Запис у стрічці конкретного користувача."""
    user_id: int
    post_id: int
    author_id: int
    inserted_at: float = field(default_factory=time.time)


# ──────────────────────────────────────────────
# Імітація TAO — графова БД Meta (соціальний граф)
# ──────────────────────────────────────────────

class TAOClient:
    """
    Спрощена імітація графової бази даних TAO.
    Зберігає зв'язки «підписник → автор».
    """

    def __init__(self) -> None:
        # graph[author_id] = список user_id, що підписані на нього
        self._graph: dict[int, list[int]] = {
            1: [10, 11, 12, 13, 14],   # автор 1 → 5 підписників
            2: [10, 15, 16],            # автор 2 → 3 підписники
            3: [11, 12, 17, 18, 19, 20],# автор 3 → 6 підписників
        }

    def get_followers(self, author_id: int) -> list[int]:
        """Повертає список ID підписників автора."""
        followers = self._graph.get(author_id, [])
        log.info(f"[TAO] Отримано {len(followers)} підписників автора {author_id}: {followers}")
        return followers

    def add_follower(self, author_id: int, follower_id: int) -> None:
        """Додає підписника до графу."""
        self._graph.setdefault(author_id, []).append(follower_id)
        log.info(f"[TAO] Користувач {follower_id} підписався на {author_id}")


# ──────────────────────────────────────────────
# Імітація Cassandra — зберігання стрічок (Feed)
# ──────────────────────────────────────────────

class CassandraClient:
    """
    Спрощена імітація Cassandra.
    Зберігає стрічку (Feed) кожного користувача.
    Cassandra оптимізована під write-heavy навантаження.
    """

    def __init__(self, failure_rate: float = 0.0) -> None:
        # feed[user_id] = список FeedEntry
        self._feed: dict[int, list[FeedEntry]] = defaultdict(list)
        self._failure_rate = failure_rate  # для симуляції збоїв

    def insert_feed(self, user_id: int, post_id: int, author_id: int) -> None:
        """Вставляє пост у стрічку користувача."""
        if random.random() < self._failure_rate:
            raise ConnectionError(
                f"[Cassandra] Збій при записі: user_id={user_id}, post_id={post_id}"
            )
        entry = FeedEntry(user_id=user_id, post_id=post_id, author_id=author_id)
        self._feed[user_id].append(entry)
        log.debug(f"[Cassandra] Записано пост {post_id} у стрічку користувача {user_id}")

    def get_feed(self, user_id: int, limit: int = 10) -> list[FeedEntry]:
        """Повертає стрічку користувача (останні N записів)."""
        return sorted(
            self._feed[user_id],
            key=lambda e: e.inserted_at,
            reverse=True,
        )[:limit]


# ──────────────────────────────────────────────
# Імітація Kafka — черга подій
# ──────────────────────────────────────────────

class KafkaProducer:
    """Відправляє події нових постів у топік Kafka."""

    def __init__(self) -> None:
        self._queue: list[PostEvent] = []

    def publish(self, event: PostEvent) -> None:
        self._queue.append(event)
        log.info(f"[Kafka] Опубліковано подію: post_id={event.post_id}, author_id={event.author_id}")

    @property
    def queue(self) -> list[PostEvent]:
        return self._queue


class KafkaConsumer:
    """Зчитує події з топіку Kafka."""

    def __init__(self, producer: KafkaProducer) -> None:
        self._producer = producer
        self._offset = 0

    def consume(self) -> Iterator[PostEvent]:
        """Генератор: повертає необроблені події по одній."""
        while self._offset < len(self._producer.queue):
            event = self._producer.queue[self._offset]
            self._offset += 1
            yield event


# ──────────────────────────────────────────────
# Dead Letter Queue — черга для помилкових подій
# ──────────────────────────────────────────────

class DeadLetterQueue:
    """Зберігає події, які не вдалося обробити, для повторної спроби."""

    def __init__(self) -> None:
        self._items: list[dict] = []

    def push(self, event: PostEvent, error: Exception) -> None:
        self._items.append({"event": event, "error": str(error)})
        log.warning(f"[DLQ] Подія {event.post_id} переміщена до DLQ: {error}")

    def replay(self) -> list[dict]:
        """Повертає всі елементи для повторної обробки."""
        return list(self._items)

    def size(self) -> int:
        return len(self._items)


# ──────────────────────────────────────────────
# Метрики
# ──────────────────────────────────────────────

class Metrics:
    """Простий лічильник метрик (у реальній системі — Prometheus/ODS)."""

    def __init__(self) -> None:
        self._counters: dict[str, int] = defaultdict(int)

    def increment(self, key: str, value: int = 1) -> None:
        self._counters[key] += value

    def report(self) -> None:
        log.info("=" * 40)
        log.info("📊 МЕТРИКИ:")
        for key, val in sorted(self._counters.items()):
            log.info(f"   {key}: {val}")
        log.info("=" * 40)


# ──────────────────────────────────────────────
# Fanout Service — головний сервіс
# ──────────────────────────────────────────────

class FanoutService:
    """
    Fanout Service відповідає за розповсюдження нового поста
    по стрічках (Feed) усіх підписників автора.

    Принцип роботи:
    1. Отримати список підписників з TAO (графова БД)
    2. Для кожного підписника записати post_id у Cassandra
    3. У разі збою — передати подію до Dead Letter Queue
    4. Оновити метрики
    """

    def __init__(
        self,
        tao: TAOClient,
        cassandra: CassandraClient,
        dlq: DeadLetterQueue,
        metrics: Metrics,
        batch_size: int = 100,
    ) -> None:
        self.tao = tao
        self.cassandra = cassandra
        self.dlq = dlq
        self.metrics = metrics
        self.batch_size = batch_size

    def process_new_post(self, event: PostEvent) -> bool:
        """
        Обробляє подію нового поста.
        Повертає True при успіху, False при частковій/повній помилці.
        """
        log.info(f"\n{'─'*50}")
        log.info(f"[Fanout] Обробка поста {event.post_id} від автора {event.author_id}")

        # Крок 1: Отримання підписників з TAO
        followers = self.tao.get_followers(event.author_id)
        if not followers:
            log.info(f"[Fanout] Автор {event.author_id} не має підписників. Пропускаємо.")
            self.metrics.increment("fanout.skipped_no_followers")
            return True

        self.metrics.increment("fanout.followers_total", len(followers))

        # Крок 2: Батчевий запис у Cassandra
        success_count = 0
        fail_count = 0

        for i in range(0, len(followers), self.batch_size):
            batch = followers[i : i + self.batch_size]
            for user_id in batch:
                try:
                    self.cassandra.insert_feed(
                        user_id=user_id,
                        post_id=event.post_id,
                        author_id=event.author_id,
                    )
                    success_count += 1
                except ConnectionError as e:
                    fail_count += 1
                    log.error(f"[Fanout] Помилка запису для user_id={user_id}: {e}")

        # Крок 3: Метрики
        self.metrics.increment("fanout.feed_writes_success", success_count)
        self.metrics.increment("fanout.feed_writes_failed", fail_count)

        if fail_count == 0:
            log.info(
                f"[Fanout] ✅ Пост {event.post_id} успішно розповсюджено "
                f"{success_count} підписникам."
            )
            self.metrics.increment("fanout.completed")
            return True
        else:
            log.warning(
                f"[Fanout] ⚠️  Пост {event.post_id}: успіх={success_count}, помилки={fail_count}"
            )
            self.metrics.increment("fanout.partial_failure")
            return False


# ──────────────────────────────────────────────
# Kafka Consumer Loop — точка входу
# ──────────────────────────────────────────────

def kafka_consumer_loop(
    consumer: KafkaConsumer,
    fanout: FanoutService,
    dlq: DeadLetterQueue,
) -> None:
    """
    Нескінченний цикл обробки подій з Kafka.
    У разі критичної помилки — подія йде до DLQ.
    """
    log.info("[Kafka Consumer] Запуск циклу обробки подій...")
    for event in consumer.consume():
        try:
            fanout.process_new_post(event)
        except Exception as e:
            # Повторна спроба через DLQ
            dlq.push(event, e)


# ──────────────────────────────────────────────
# Демонстрація
# ──────────────────────────────────────────────

def demo() -> None:
    log.info("=" * 50)
    log.info("Instagram Fanout Service — демонстрація")
    log.info("=" * 50)

    # Ініціалізація компонентів
    tao = TAOClient()
    cassandra = CassandraClient(failure_rate=0.0)  # 0% збоїв для чистої демо
    dlq = DeadLetterQueue()
    metrics = Metrics()
    fanout = FanoutService(tao, cassandra, dlq, metrics)
    kafka_producer = KafkaProducer()
    kafka_consumer = KafkaConsumer(kafka_producer)

    # Сценарій 1: Автор 1 публікує пост
    kafka_producer.publish(PostEvent(post_id=1001, author_id=1, content="Фото з відпустки 🏖️"))

    # Сценарій 2: Автор 2 публікує пост
    kafka_producer.publish(PostEvent(post_id=1002, author_id=2, content="Новий рецепт 🍕"))

    # Сценарій 3: Автор 3 публікує пост
    kafka_producer.publish(PostEvent(post_id=1003, author_id=3, content="Reels відео 🎬"))

    # Сценарій 4: Автор без підписників
    kafka_producer.publish(PostEvent(post_id=1004, author_id=99, content="Пост невідомого автора"))

    # Запуск обробки
    kafka_consumer_loop(kafka_consumer, fanout, dlq)

    # ──────────────────────────────────────────
    # Перевірка стрічок
    # ──────────────────────────────────────────
    log.info("\n" + "=" * 50)
    log.info("📋 СТРІЧКИ КОРИСТУВАЧІВ (Cassandra):")
    log.info("=" * 50)

    for user_id in [10, 11, 12, 13, 14, 15, 16, 17]:
        feed = cassandra.get_feed(user_id)
        post_ids = [e.post_id for e in feed]
        log.info(f"  Користувач {user_id}: пости у стрічці → {post_ids}")

    # ──────────────────────────────────────────
    # Демонстрація збоїв та DLQ
    # ──────────────────────────────────────────
    log.info("\n" + "=" * 50)
    log.info("⚡ ДЕМОНСТРАЦІЯ ЗБОЇВ (failure_rate=30%):")
    log.info("=" * 50)

    cassandra_unreliable = CassandraClient(failure_rate=0.3)
    fanout_unreliable = FanoutService(tao, cassandra_unreliable, dlq, metrics)

    kafka_producer2 = KafkaProducer()
    kafka_consumer2 = KafkaConsumer(kafka_producer2)
    kafka_producer2.publish(PostEvent(post_id=2001, author_id=1, content="Тест збоїв 🔥"))

    kafka_consumer_loop(kafka_consumer2, fanout_unreliable, dlq)

    # DLQ статус
    if dlq.size() > 0:
        log.info(f"\n[DLQ] Елементів у черзі помилок: {dlq.size()}")
        for item in dlq.replay():
            log.info(f"  → post_id={item['event'].post_id}: {item['error']}")
    else:
        log.info("\n[DLQ] Черга порожня — всі події оброблені успішно.")

    # Метрики
    metrics.report()


if __name__ == "__main__":
    demo()
