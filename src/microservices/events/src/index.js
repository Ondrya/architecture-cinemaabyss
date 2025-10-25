const { Kafka } = require('kafkajs')
const express = require('express')
const app = express()
const port = 3000

// Инициализация Kafka
const kafka = new Kafka({
  brokers: ['kafka:9092'],
  clientId: 'events-service'
})

// Producer для отправки сообщений
const producer = kafka.producer({
  retries: 3
})

// Consumer для чтения сообщений
const consumer = kafka.consumer({
  groupId: 'events-group'
})

// Создание топиков
async function createTopics() {
  await producer.connect()
  await consumer.connect()
  await consumer.subscribe({
    topic: 'user-events',
    fromBeginning: true
  })
  await consumer.subscribe({
    topic: 'payment-events',
    fromBeginning: true
  })
  await consumer.subscribe({
    topic: 'movie-events',
    fromBeginning: true
  })
}

// Запуск consumer'а
consumer.run({
  eachMessage: async ({ topic, message }) => {
    const value = message.value.toString()
    console.log(`Received message in ${topic}: ${value}`)
  }
})

// API endpoints
app.post('/api/user', async (req, res) => {
  await producer.send({
    topic: 'user-events',
    messages: [{ value: JSON.stringify(req.body) }]
  })
  res.status(201).send('User event created')
})

app.post('/api/payment', async (req, res) => {
  await producer.send({
    topic: 'payment-events',
    messages: [{ value: JSON.stringify(req.body) }]
  })
  res.status(201).send('Payment event created')
})

app.post('/api/movie', async (req, res) => {
  await producer.send({
    topic: 'movie-events',
    messages: [{ value: JSON.stringify(req.body) }]
  })
  res.status(201).send('Movie event created')
})

// Запуск сервиса
app.listen(port, async () => {
  console.log(`Events service running on http://localhost:${port}`)
  await createTopics()
})
