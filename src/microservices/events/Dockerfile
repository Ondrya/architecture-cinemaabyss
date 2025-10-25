# Используем официальный образ Node.js как базовый
FROM node:18-alpine

# Устанавливаем рабочую директорию
WORKDIR /app

# Копируем package.json и package-lock.json
COPY package*.json ./

# Устанавливаем зависимости
RUN npm install --only=production

# Копируем исходный код
COPY . .

# Создаем директорию для логов
RUN mkdir /logs

# Открываем порт для приложения
EXPOSE 3000

# Определяем команду для запуска
CMD ["node", "src/index.js"]

# Опциональные настройки для производительности
# Устанавливаем максимальное использование памяти для Node.js
ENV NODE_OPTIONS=--max-old-space-size=4096

# Устанавливаем переменные окружения для Kafka
ENV KAFKA_BROKERS=kafka:9092
ENV KAFKA_CLIENT_ID=events-service
