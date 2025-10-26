const { Kafka } = require('kafkajs');
const express = require('express');
const app = express();
const port = process.env.PORT || 8082;


app.use(express.json());
// Middleware для логирования (после парсинга)
app.use((req, res, next) => {
  if (req.body && Object.keys(req.body).length > 0) {
    console.log(`[DEBUG] Incoming ${req.method} ${req.url}`);
    console.log(`[DEBUG] Parsed body:`, JSON.stringify(req.body, null, 2));
  }
  next();
});


// === Обработчики событий (вынесены отдельно) ===

async function handleUserEvent(messageValue) {
  const event = JSON.parse(messageValue);
  console.log(`[UserEvent] Processing:`, event);
  // Здесь может быть бизнес-логика: обновление профиля, аудит, уведомления и т.д.
}

async function handlePaymentEvent(messageValue) {
  const event = JSON.parse(messageValue);
  console.log(`[PaymentEvent] Processing:`, event);
  // Например: обновление баланса, отправка чека, интеграция с бухгалтерией
}

async function handleMovieEvent(messageValue) {
  const event = JSON.parse(messageValue);
  console.log(`[MovieEvent] Processing:`, event);
  // Например: обновление статистики просмотров, рекомендаций
}

// === Инициализация Kafka ===

const kafka = new Kafka({
  brokers: ['kafka:9092'],
  clientId: 'events-service'
});

const producer = kafka.producer({ retries: 3 });
const consumer = kafka.consumer({ groupId: 'events-group' });

async function startKafka() {
  try {
    await producer.connect();
    await consumer.connect();

    await consumer.subscribe({ topic: 'user-events', fromBeginning: true });
    await consumer.subscribe({ topic: 'payment-events', fromBeginning: true });
    await consumer.subscribe({ topic: 'movie-events', fromBeginning: true });

    await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const offset = message.offset;
      const value = message.value?.toString() || '';

      console.log(`[Consumer] Received from ${topic}[partition=${partition}, offset=${offset}]: ${value}`);

      let status = 'success';
      let error = null;

      try {
        switch (topic) {
          case 'user-events':
            await handleUserEvent(value);
            break;
          case 'payment-events':
            await handlePaymentEvent(value);
            break;
          case 'movie-events':
            await handleMovieEvent(value);
            break;
          default:
            console.warn(`[Consumer] Unknown topic: ${topic}`);
            return;
        }
      } catch (err) {
        status = 'failed';
        error = err.message || 'Unknown error';
        console.error(`[Consumer] Error processing message from ${topic}[offset=${offset}]:`, err);
      } finally {
        // Логируем результат обработки
        if (status === 'success') {
          console.log(`[Consumer] ✅ Successfully processed ${topic}[offset=${offset}]`);
        } else {
          console.log(`[Consumer] ❌ Failed to process ${topic}[offset=${offset}]: ${error}`);
        }
      }
    }
  });

    console.log('✅ Kafka consumer is running and subscribed to topics');
  } catch (err) {
    console.error('❌ Failed to start Kafka:', err);
  }
}

// === HTTP-эндпоинты (только продюсеры) ===

app.post('/api/events/movie', async (req, res) => {
  try {
    const event = req.body;
    if (!event.movie_id || !event.title || !event.action) {
      return res.status(400).json({ error: 'Missing required fields: movie_id, title, action' });
    }

    const result = await producer.send({
      topic: 'movie-events',
      messages: [{ value: JSON.stringify(event) }]
    });

    const record = result[0];
    return res.status(201).json({
      status: 'success',
      partition: record.partition,
      offset: parseInt(record.offset, 10),
      event: {
        id: `${event.movie_id}-${event.action}-${Date.now()}`,
        type: 'movie',
        timestamp: new Date().toISOString(),
        payload: event
      }
    });
  } catch (err) {
    console.error('Error in /api/events/movie:', err);
    return res.status(500).json({ error: 'Internal Server Error' });
  }
});

app.post('/api/events/user', async (req, res) => {
  try {
    const event = req.body;
    if (!event.user_id || !event.action || !event.timestamp) {
      return res.status(400).json({ error: 'Missing required fields: user_id, action, timestamp' });
    }

    const result = await producer.send({
      topic: 'user-events',
      messages: [{ value: JSON.stringify(event) }]
    });

    const record = result[0];
    return res.status(201).json({
      status: 'success',
      partition: record.partition,
      offset: parseInt(record.offset, 10),
      event: {
        id: `${event.user_id}-${event.action}-${Date.now()}`,
        type: 'user',
        timestamp: event.timestamp,
        payload: event
      }
    });
  } catch (err) {
    console.error('Error in /api/events/user:', err);
    return res.status(500).json({ error: 'Internal Server Error' });
  }
});

app.post('/api/events/payment', async (req, res) => {
  try {
    const event = req.body;
    if (!event.payment_id || !event.user_id || !event.amount || !event.status || !event.timestamp) {
      return res.status(400).json({ error: 'Missing required fields: payment_id, user_id, amount, status, timestamp' });
    }

    const result = await producer.send({
      topic: 'payment-events',
      messages: [{ value: JSON.stringify(event) }]
    });

    const record = result[0];
    return res.status(201).json({
      status: 'success',
      partition: record.partition,
      offset: parseInt(record.offset, 10),
      event: {
        id: `${event.payment_id}-${event.status}-${Date.now()}`,
        type: 'payment',
        timestamp: event.timestamp,
        payload: event
      }
    });
  } catch (err) {
    console.error('Error in /api/events/payment:', err);
    return res.status(500).json({ error: 'Internal Server Error' });
  }
});

app.get('/api/events/health', (req, res) => {
  res.json({ status: true });
});



app.use((err, req, res, next) => {
  console.error('Unhandled error:', err);
  if (err.type === 'entity.parse.failed' || err.message?.includes('JSON')) {
    return res.status(400).json({ error: 'Invalid JSON payload' });
  }
  res.status(500).json({
    error: 'Internal Server Error',
    message: err.message // ⚠️ только если безопасно!
  });
});


// Запуск
startKafka().catch(console.error);

app.listen(port, () => {
  console.log(`Events service running on http://localhost:${port}`);
});