# 🛰️ GoMQTT - Modern, Geliştirilebilir, Hafif MQTT Broker

## 📖 Proje Amacı

GoMQTT, IoT ve edge cihazlar için tasarlanmış, hafif ve yüksek performanslı bir MQTT broker yazılımıdır.  
C ile yazılmış mevcut brokerlara alternatif olarak, daha **kolay geliştirilebilir**, **özelleştirilebilir**, **modern** bir altyapı sunar.  
Go dili kullanılarak yazılacak ve PostgreSQL gibi modern veritabanlarıyla sorunsuz entegre olacaktır.

Bu proje sayesinde:

- Cihazlar arası iletişim hızlı, hafif ve güvenli şekilde gerçekleşir.
- Geliştiriciler, kolayca plugin ekleyerek broker'ı genişletebilir.
- Sistem yönetimi için bir REST API ve basit bir Admin Panel sunulacaktır.

---

## 🏗️ Hedef Özellikler

| Özellik                          | Açıklama                                                     |
| :------------------------------- | :----------------------------------------------------------- |
| MQTT Broker                      | Temel publish/subscribe mekanizması                          |
| Plug-in Sistemi                  | Kolayca event-based genişletme yapılabilecek yapı            |
| REST API                         | Cihaz yönetimi, mesaj geçmişi, online/offline kontrolü       |
| Custom Authentication            | API Key / JWT bazlı kimlik doğrulama                         |
| Access Control                   | Cihaz bazlı topic yetkilendirme                              |
| PostgreSQL Integration           | Mesaj geçmişi ve cihaz kayıtları veritabanında saklama       |
| Monitoring Dashboard (Opsiyonel) | Bağlı client'lar, mesaj istatistikleri, sistem durumu izleme |

---

## 📋 Yapılacaklar Listesi (Roadmap)

### 1. Temel Broker Altyapısı

- [ ] TCP Server kur (port: 1883 default)
- [ ] MQTT v3.1.1 / v5 protokol parser yaz
- [ ] Connect, Subscribe, Publish, Ping, Disconnect paketleri işle

### 2. Kimlik Doğrulama Sistemi

- [ ] Basit kullanıcı-parola ile auth
- [ ] JWT destekli auth opsiyonu

### 3. Mesaj Routing Sistemi

- [ ] Topic bazlı mesaj yönlendirme
- [ ] QoS 0 ve QoS 1 desteği

### 4. REST API Sunucusu

- [ ] Client listesi (bağlı cihazlar)
- [ ] Publish geçmişi listeleme
- [ ] Online / Offline cihaz sorgulama

### 5. Veritabanı Bağlantısı

- [ ] PostgreSQL bağlantısı kur
- [ ] Publish edilen mesajları kaydet
- [ ] Client bilgilerini sakla

### 6. Plugin Sistemi

- [ ] Publish/Subscribe olaylarına hook yazabilme altyapısı
- [ ] Basit örnek plugin (Webhook tetikleme)

### 7. Admin Panel (Opsiyonel, İleri Seviye)

- [ ] Web UI'da client listesi, mesaj grafikleri
- [ ] WebSocket ile canlı veri akışı

### 8. Deployment

- [ ] Dockerfile ve docker-compose.yml dosyası
- [ ] Minimal konfigürasyonla ayağa kaldırılabilir yapı

---

## 🖥️ Sistem Gereksinimleri

| Bileşen        | Minimum Gereksinim                         |
| :------------- | :----------------------------------------- |
| Go             | 1.20+                                      |
| PostgreSQL     | 13+                                        |
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

---

📜  
Bu README ilk etap için çok sağlam bir temel olur.  
İstersen buna göre bir **repo oluşturup**, hemen dosya yapısına başlayabiliriz.

👉 Başlayalım mı?  
(Hemen sana `cmd/broker`, `pkg/mqtt`, `internal/api` gibi dizin yapısını da çıkarırım istersen.) 🚀  
Hazır mısın?
