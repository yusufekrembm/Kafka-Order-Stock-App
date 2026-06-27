# Kafka-Order-Stock-App 🛒

Apache **Kafka** + **Spring Boot** ile yazılmış, **event-driven (olay tabanlı)** bir
e-ticaret sipariş işleme örneği. Kafka'yı sıfırdan öğrenmek için adım adım kuruldu.

Bir müşteri sipariş verir → sipariş Kafka'ya yazılır → **Stok Servisi** ve
**Bildirim Servisi** bu siparişi birbirinden bağımsız olarak işler.

---

## 📑 İçindekiler

- [Kafka nedir? (Basit anlatım)](#-kafka-nedir-basit-anlatım)
- [Temel kavramlar](#-temel-kavramlar)
- [Mimari](#-mimari)
- [Teknolojiler](#-teknolojiler)
- [Kurulum ve Çalıştırma](#-kurulum-ve-çalıştırma)
- [Test etme](#-test-etme)
- [Projede Kafka nasıl çalışıyor?](#-projede-kafka-nasıl-çalışıyor)
- [Partition ve ölçekleme](#-partition-ve-ölçekleme)
- [Proje yapısı](#-proje-yapısı)
- [Karşılaşılan tuzak: advertised listeners](#-karşılaşılan-tuzak-advertised-listeners)

---

## 🧠 Kafka nedir? (Basit anlatım)

Bir restoran düşün 🍽️.

**Kafka'sız dünya:** Garson, siparişi vermek için bizzat mutfağa, sonra kasaya,
sonra müşteriye koşturur. Aşçı meşgulse bekler; kasa çökerse sipariş kaybolur.
Her şey birbirine sıkı sıkıya bağlıdır.

**Kafka'lı dünya:** Garson siparişi bir **fişe yazıp panoya asar** ve işi biter.
Aşçı, kasa, bildirim görevlisi panoyu **kendi hızında** okur. Biri hata yaparsa
fiş panoda durur, geri gelince kaldığı yerden devam eder.

> **Kafka, işte o panodur** — bir *event streaming platformu*. Servisler birbirine
> doğrudan konuşmaz; olayları (event) ortak bir yere yazar ve oradan okurlar.
> Bu "gevşek bağlılık" (loose coupling), Kafka'nın tüm gücüdür.

---

## 📚 Temel kavramlar

| Terim | Ne demek | Bu projede |
|-------|----------|------------|
| **Producer** | Mesaj yazan | Sipariş servisi (`OrderProducer`) |
| **Consumer** | Mesaj okuyan | Stok ve Bildirim servisleri |
| **Topic** | Belirli bir konunun panosu | `orders` |
| **Message/Event** | Tek bir kayıt | "Sipariş #42 oluştu" verisi |
| **Broker** | Kafka sunucusu | Docker'da çalışır |
| **Partition** | Topic'in paralel parçaları | `orders` → 3 partition |
| **Consumer Group** | Aynı işi paylaşan okuyucular | `stock-service`, `notification-service` |
| **Offset** | "Nereye kadar okudum" işareti | Her grup için ayrı tutulur |

---

## 🏗 Mimari

```
                                    ┌─> Stok Servisi (group: stock-service)
   POST /orders ─> OrderProducer ─> [ orders topic ]
   (REST, JSON)     (Producer)      (3 partition)
                                    └─> Bildirim Servisi (group: notification-service)
```

- Müşteri `POST /orders` ile JSON sipariş gönderir.
- `OrderProducer` siparişi `orders` topic'ine yazar.
- **İki ayrı consumer group** aynı mesajın **birer kopyasını** alır:
  - **Stok Servisi** → stoğu düşürür.
  - **Bildirim Servisi** → müşteriye e-posta/SMS gönderir (simüle).
- Producer, kimin okuduğunu **bilmez** — yeni bir servis eklemek için
  producer koduna dokunmak gerekmez.

---

## 🛠 Teknolojiler

- Java 21
- Spring Boot 3.5 (Web + Spring Kafka)
- Apache Kafka (KRaft modu — Zookeeper'sız)
- Docker & Docker Compose
- Kafka UI (panoyu tarayıcıdan izlemek için)
- Maven (wrapper ile — ayrıca kurmaya gerek yok)

---

## 🚀 Kurulum ve Çalıştırma

### Gereksinimler
- Docker Desktop
- Java 21

### 1) Kafka'yı ayağa kaldır
```bash
docker compose up -d
```
- Kafka broker → `localhost:9092`
- Kafka UI → http://localhost:8080

### 2) Uygulamayı çalıştır
```bash
cd order-app
./mvnw spring-boot:run        # Windows: .\mvnw.cmd spring-boot:run
```
Uygulama → http://localhost:8081 (Kafka UI 8080'de olduğu için 8081 kullanıldı)

### Durdurmak için
```bash
docker compose down           # Kafka'yı durdur
```

---

## 🧪 Test etme

Bir sipariş gönder:

```bash
curl -X POST http://localhost:8081/orders \
  -H "Content-Type: application/json" \
  -d '{"orderId":"ORD-1","product":"Fren Balatasi","quantity":2,"customer":"Ahmet","totalPrice":450.0}'
```

PowerShell ile:
```powershell
$body = @{ orderId="ORD-1"; product="Fren Balatasi"; quantity=2; customer="Ahmet"; totalPrice=450.0 } | ConvertTo-Json
Invoke-RestMethod -Uri "http://localhost:8081/orders" -Method Post -Body $body -ContentType "application/json"
```

Uygulama loglarında her iki servisin de tepki verdiğini göreceksin:
```
📦 [STOK]     partition=2 | siparis=ORD-1 (2 adet 'Fren Balatasi')
📧 [BILDIRIM] Sayin Ahmet, 'ORD-1' siparisiniz alindi. Tutar: 450.0 TL ✅
```

Mesajları **Kafka UI**'dan da görebilirsin: http://localhost:8080 → Topics → orders → Messages.

---

## ⚙️ Projede Kafka nasıl çalışıyor?

**1. Producer (yazma)** — `OrderController` REST isteğini alır, `OrderProducer`
`KafkaTemplate` ile siparişi `orders` topic'ine yazar. Mesaj anahtarı (key)
olarak `orderId`, gövde (value) olarak `Order` nesnesi JSON'a çevrilerek gönderilir.

```java
kafkaTemplate.send(ordersTopic, order.orderId(), order);
```

**2. Consumer (okuma)** — `@KafkaListener` ile işaretlenen metotlar topic'i
sürekli dinler; yeni mesaj geldiğinde Spring metodu otomatik çağırır. Gelen JSON,
otomatik olarak `Order` nesnesine çevrilir (deserialize).

```java
@KafkaListener(topics = "orders", groupId = "stock-service")
public void handleOrder(Order order) { ... }
```

**3. İki bağımsız tüketici** — `stock-service` ve `notification-service` **farklı
consumer group** oldukları için her mesajın birer kopyasını alır. Tek sipariş →
iki servis birden çalışır.

---

## 📈 Partition ve ölçekleme

`orders` topic'i **3 partition**'a bölünmüştür. Stok servisi `concurrency=3` ile
çalışır, yani 3 paralel consumer açar:

```
stock-service (3 consumer):           notification-service (1 consumer):
   consumer-1 ─> partition 0             tek consumer ─> partition 0,1,2
   consumer-2 ─> partition 1
   consumer-3 ─> partition 2
```

Altın kurallar:
- Bir partition'ı, bir grup içinde **sadece bir** consumer okur.
- Mesaj hangi partition'a gider? → `hash(key) % partition_sayısı`.
- **Sıra garantisi sadece partition içinde** geçerlidir; partition'lar arasında
  global sıra yoktur. Sıra önemliyse key seçimi kritiktir (örn. `customerId`).
- Partition sayısı yalnızca **artırılabilir**, azaltılamaz.

---

## 📁 Proje yapısı

```
KAFKA/
├── docker-compose.yml          # Kafka (KRaft) + Kafka UI
├── 00-Kafka-Notlari.md         # Adım adım öğrenme notları (Türkçe)
├── README.md
└── order-app/                  # Spring Boot uygulaması
    ├── pom.xml
    └── src/main/
        ├── java/com/tedlim/orders/
        │   ├── OrderAppApplication.java
        │   ├── model/Order.java                 # Sipariş veri modeli (record)
        │   ├── config/KafkaTopicConfig.java     # 'orders' topic'ini oluşturur
        │   ├── producer/OrderProducer.java      # Kafka'ya yazar
        │   ├── controller/OrderController.java   # POST /orders
        │   └── consumer/
        │       ├── StockConsumer.java           # Stok servisi (concurrency=3)
        │       └── NotificationConsumer.java     # Bildirim servisi
        └── resources/application.properties      # Kafka producer/consumer ayarları
```

---

## ⚠️ Karşılaşılan tuzak: advertised listeners

Kurulum sırasında Kafka UI cluster'a bağlanamadı (sonsuz loading). Sebep:
broker bağlananlara `localhost:9092` adresini "ilan ediyordu". Bu adres host'taki
Spring Boot için doğru, ama Docker container'ındaki Kafka UI için `localhost`
kendi container'ı demek.

**Çözüm — iki ayrı listener (kapı):**
- `EXTERNAL://localhost:9092` → host'tan bağlananlar (Spring Boot)
- `INTERNAL://kafka:29092` → Docker ağı içinden bağlananlar (Kafka UI)

> Kural: Kafka'ya kim nereden bağlanıyorsa, broker ona **erişebileceği** bir adres
> ilan etmelidir. Docker + Kafka'da en sık yapılan hata budur.

---

## 📝 Lisans

Eğitim amaçlı örnek proje. Serbestçe kullanabilirsin.
