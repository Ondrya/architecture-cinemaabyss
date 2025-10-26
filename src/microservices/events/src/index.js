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

// Подключение и запуск consumer'а — ДО старта HTTP-сервера
async function startKafka() {
  try {
    await producer.connect();
    await consumer.connect();

    // Подписка ДО запуска
    await consumer.subscribe({ topic: 'user-events', fromBeginning: true });
    await consumer.subscribe({ topic: 'payment-events', fromBeginning: true });
    await consumer.subscribe({ topic: 'movie-events', fromBeginning: true });

    // Запуск обработки сообщений
    await consumer.run({
      eachMessage: async ({ topic, message }) => {
        const value = message.value.toString();
        console.log(`[Consumer] Received from ${topic}: ${value}`);
      }
    });

    console.log('✅ Kafka consumer is running and subscribed to topics');
  } catch (err) {
    console.error('❌ Failed to start Kafka:', err);
    // Не завершаем процесс — даём HTTP-серверу работать
  }
}


// --- Эндпоинты согласно OpenAPI ---

// POST /api/events/movie
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

    // ✅ Правильная структура: result[0] — объект с partition и offset
    if (!result || result.length === 0) {
      throw new Error('Producer returned empty result');
    }

    const record = result[0]; // ← не result[0].offsets[0]!

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

    if (!result || result.length === 0) {
      throw new Error('Producer returned empty result');
    }
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

    if (!result || result.length === 0) {
      throw new Error('Producer returned empty result');
    }
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

// Health check
app.get('/api/events/health', (req, res) => {
  res.json({ status: true });
});



// Запуск сервиса

startKafka().catch(console.error); // Запускаем Kafka асинхронно

app.listen(port, async () => {
  console.log(`Events service running on http://localhost:${port}`);
});