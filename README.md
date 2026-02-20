# XmlProcessor

В качестве источника используется XML:
https://expro.ru/bitrix/catalog_export/export_Sai.xml

---

Локальный запуск

1. Запустить PostgreSQL
`docker compose up -d`

PostgreSQL будет доступен по адресу:
jdbc:postgresql://localhost:8080/catalog

2. Собрать проект
`mvn clean package`

4. Запустить приложение
`java -jar target/XMLProcessor-1.0-SNAPSHOT.jar`

---

Стек

- Java

- PostgreSQL

- JDBC

- Groovy XmlSlurper

- Docker Compose
