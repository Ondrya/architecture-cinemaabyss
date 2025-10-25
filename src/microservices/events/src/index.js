const { Kafka } = require('kafkajs');
const express = require('express');
const app = express();
const port = 3000;

// Middleware для парсинга JSON
app.use(express.json());

// Инициализация Kafka
const kafka = new Kafka({
  brokers: ['kafka:9092'],
  clientId: 'events-service'
});

const producer = kafka.producer({ retries: 3 });
const consumer = kafka.consumer({ groupId: 'events-group' });

// Подключение и подписка на топики
async function createTopics() {
  await producer.connect();
  await consumer.connect();

  const topics = ['user-events', 'payment-events', 'movie-events'];
  for (const topic of topics) {
    await consumer.subscribe({ topic, fromBeginning: true });
  }
}

// Обработка входящих сообщений (логирование)
consumer.run({
  eachMessage: async ({ topic, message }) => {
    const value = message.value.toString();
    console.log(`[Consumer] Received from ${topic}: ${value}`);
  }
});

// --- Эндпоинты согласно OpenAPI ---

// POST /api/events/movie
app.post('/api/events/movie', async (req, res) => {
  try {
    const event = req.body;
    // Валидация минимальная (в production — использовать Joi/Zod)
    if (!event.movie_id || !event.title || !event.action) {
      return res.status(400).json({ error: 'Missing required fields: movie_id, title, action' });
    }

    const result = await producer.send({
      topic: 'movie-events',
      messages: [{ value: JSON.stringify(event) }]
    });

    const record = result[0].offsets[0];
    return res.status(201).json({
      status: 'success',
      partition: record.partition,
      offset: parseInt(record.offset),
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

// POST /api/events/user
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

    const record = result[0].offsets[0];
    return res.status(201).json({
      status: 'success',
      partition: record.partition,
      offset: parseInt(record.offset),
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

// POST /api/events/payment
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

    const record = result[0].offsets[0];
    return res.status(201).json({
      status: 'success',
      partition: record.partition,
      offset: parseInt(record.offset),
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

// Health check
app.get('/api/events/health', (req, res) => {
  res.json({ status: true });
});

// Запуск сервиса
app.listen(port, async () => {
  console.log(`Events service running on http://localhost:${port}`);
  await createTopics();
  console.log('Connected to Kafka and subscribed to topics');
});