Кинобездна (To-Be)

```plantuml
@startuml
!include https://raw.githubusercontent.com/plantuml-stdlib/C4-PlantUML/master/C4_Container.puml

Person(user, "Пользователь", "Использует Web, Mobile, Smart TV")

Container(api, "API Gateway", "Kong/Traefik", "Единая точка входа, маршрутизация, auth")

System_Boundary(c1, "Events SubSystem") {
  
  Container(events, "Event Processor", "Go", "Обработка событий из Kafka")
  
  ContainerQueue(kafka, "Apache Kafka", "Шина событий", "user-events, content-events, billing-events")
}

System_Boundary(c2, "Billing SubSystem") {
  Container(billing, "Billing Service", "Go", "Подписки и платежи")
  ContainerDb(db_billing, "PostgreSQL (billing)", "DB", "")
}

System_Boundary(c3, "Catalog SubSystem") {
  Container(catalog, "Catalog Service", "Go", "Метаданные фильмов, избранное")
  ContainerDb(db_catalog, "PostgreSQL (catalog)", "DB", "")
  Container(s3, "S3 / CDN", "Хранилище видео и обложек", "")
  Container(content, "Content Delivery Service", "Go", "Доставка видео (URL, CDN)")
}

System_Boundary(c4, "Auth SubSystem") {
  Container(auth, "Auth Service", "Go", "Аутентификация и управление пользователями")
  ContainerDb(db_auth, "PostgreSQL (users)", "DB", "")
}

System_Boundary(c5, "Loyalty SubSystem") {
  Container(auth, "Auth Service", "Go", "Аутентификация и управление пользователями")
  Container(loyalty, "Loyalty Service", "Go", "Партнёрские интеграции")
  ContainerDb(db_loyalty, "PostgreSQL (loyalty)", "DB", "")
}

System_Ext(payment, "Платёжная система", "Внешний партнёр")
System_Ext(reco, "Рекомендательная система", "Внешний ML-сервис")
System_Ext(partners, "Маркетплейсы / Партнёры", "Системы лояльности")

Rel(user, api, "HTTPS")
Rel(api, auth, "HTTP/gRPC")
Rel(api, catalog, "HTTP/gRPC")
Rel(api, content, "HTTP/gRPC")
Rel(api, billing, "HTTP/gRPC")
Rel(api, loyalty, "HTTP/gRPC")

Rel(auth, db_auth, "Чтение/запись")
Rel(catalog, db_catalog, "Чтение/запись")
Rel(billing, db_billing, "Чтение/запись")
Rel(loyalty, db_loyalty, "Чтение/запись")

Rel(catalog, s3, "S3 API")
Rel(billing, payment, "REST/Webhook")
Rel(events, reco, "HTTPS или Kafka")
Rel(events, partners, "HTTPS")

Rel(auth, kafka, "Публикует события")
Rel(catalog, kafka, "Публикует события")
Rel(billing, kafka, "Публикует события")
Rel(loyalty, kafka, "Публикует события")

Rel(events, kafka, "Подписывается на события")

@enduml
```
