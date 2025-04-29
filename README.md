# 🛰️ GoMQTT - Modern, Geliştirilebilir, Hafif MQTT Broker

## 📖 Proje Amacı

GoMQTT, IoT ve edge cihazlar için tasarlanmış, hafif ve yüksek performanslı bir MQTT broker yazılımıdır.  
C ile yazılmış mevcut brokerlara alternatif olarak, daha **kolay geliştirilebilir**, **özelleştirilebilir**, **modern** bir altyapı sunar.  
Go dili kullanılarak yazılacak ve PostgreSQL gibi modern veritabanlarıyla sorunsuz entegre olacaktır.

Bu proje sayesinde:

- Cihazlar arası iletişim hızlı, hafif ve güvenli şekilde gerçekleşir.
- Geliştiriciler, kolayca plugin ekleyerek broker'ı genişletebilir.
- Sistem yönetimi için bir REST API (go-chi) ve entegre bir Admin Panel (Go+HTMX+templ) sunulacaktır.

---

## 🏗️ Hedef Özellikler

| Özellik                | Açıklama                                                   |
| :--------------------- | :--------------------------------------------------------- |
| MQTT Broker            | Temel publish/subscribe mekanizması                        |
| Plug-in Sistemi        | Kolayca event-based genişletme yapılabilecek yapı          |
| REST API               | Cihaz yönetimi, mesaj geçmişi, online/offline kontrolü     |
| Custom Authentication  | API Key - JWT bazlı kimlik doğrulama                       |
| Access Control         | Cihaz bazlı topic yetkilendirme                            |
| PostgreSQL Integration | Mesaj geçmişi ve cihaz kayıtları veritabanında saklama     |
| Monitoring Dashboard   | Go+HTMX+templ ile bağlı client'lar ve sistem durumu izleme |

---

## 📋 Yapılacaklar Listesi (Roadmap)

### 1. Temel Broker Altyapısı

- [x] TCP Server kur (port: 1883 default)
- [x] MQTT v3.1.1 / v5 protokol parser yaz
- [x] Connect, Subscribe, Publish, Ping, Disconnect paketleri işle

### 2. Kimlik Doğrulama Sistemi

- [x] JWT destekli auth

### 3. Mesaj Routing Sistemi

- [x] Topic bazlı mesaj yönlendirme
- [x] QoS 0 ve QoS 1 desteği

### 4. REST API Sunucusu

- [x] Client listesi (bağlı cihazlar)
- [x] Publish geçmişi listeleme
- [x] Online / Offline cihaz sorgulama

### 5. Veritabanı Bağlantısı

- [x] PostgreSQL bağlantısı kur
- [x] Publish edilen mesajları kaydet
- [x] Client bilgilerini sakla

### 6. Plugin Sistemi

- [x] Publish/Subscribe olaylarına hook yazabilme altyapısı
- [x] Basit örnek plugin (Webhook tetikleme)

### 7. Admin Panel (Go+HTMX+templ)

- [x] Client listesi, mesaj grafikleri ve sistem durumu izleme
- [x] HTMX ile dinamik ve hızlı UI deneyimi
- [x] templ ile Go entegrasyonu

### 8. Deployment

- [x] Dockerfile ve docker-compose.yml dosyası
- [x] Minimal konfigürasyonla ayağa kaldırılabilir yapı

---

## 🖥️ Sistem Gereksinimleri

| Bileşen        | Minimum Gereksinim                         |
| :------------- | :----------------------------------------- |
| Go             | 1.24+                                      |
| PostgreSQL     | 16+                                        |
| Linux Server   | Ubuntu 22.04 önerilir                      |
| MQTT Clientlar | MQTT v3.1.1 veya v5 destekli tüm clientlar |

---

## ⚙️ Geliştirme Ortamı Kurulumu

```bash
# Go kurulumu
sudo apt update
sudo apt install golang-go

# PostgreSQL kurulumu
sudo apt install postgresql postgresql-contrib

# Geliştirme için kütüphaneler
go get github.com/eclipse/paho.mqtt.golang
go get github.com/jackc/pgx/v5
go get github.com/go-chi/chi/v5
go get github.com/a-h/templ
```

---

## 📡 MQTT Clientlar İçin Örnekler

### React Native (mobil cihazlar için)

```bash
npm install mqtt
```

```javascript
import mqtt from "mqtt";

const client = mqtt.connect("wss://broker-address:port");

client.on("connect", () => {
  client.subscribe("sensor/temperature");
  client.publish("sensor/temperature", "23°C");
});
```

### Go (IoT cihazı için)

```go
package main

import (
    MQTT "github.com/eclipse/paho.mqtt.golang"
    "fmt"
)

func main() {
    opts := MQTT.NewClientOptions().AddBroker("tcp://broker-address:1883")
    client := MQTT.NewClient(opts)

    if token := client.Connect(); token.Wait() && token.Error() != nil {
        panic(token.Error())
    }

    token := client.Publish("sensor/temperature", 0, false, "23°C")
    token.Wait()

    fmt.Println("Message published")
}
```

---

## 📚 İlham Kaynakları

- [Eclipse Mosquitto](https://mosquitto.org/)
- [EMQX](https://www.emqx.io/)
- [VerneMQ](https://vernemq.com/)
- [NanoMQ](https://nanomq.io/)

---

# 🚀 Hedefimiz

> C ile yazılan brokerların performansını koruyup,  
> Go diliyle **esnek, genişletilebilir** ve **modern** bir MQTT dünyası kurmak!

---

## 📦 Lisans

MIT License (özgür kullanım)

---

# ✍️ NOT

Bu proje **gerçek bir production-ready** broker olma potansiyeline sahiptir.  
İlk versiyonda hafif tutulacak, sonra ileri seviye optimizasyonlar (Zero-Copy IO, event-driven networking) yapılabilir.
