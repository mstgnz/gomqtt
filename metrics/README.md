# GoMQTT Monitoring with Prometheus and Grafana

Bu belge, GoMQTT için Prometheus ve Grafana kullanarak izleme sistemi kurulumunu açıklar.

## İçindekiler

- [Genel Bakış](#genel-bakış)
- [Kurulum](#kurulum)
  - [Prometheus Kurulumu](#prometheus-kurulumu)
  - [Grafana Kurulumu](#grafana-kurulumu)
- [Yapılandırma](#yapılandırma)
  - [Prometheus Yapılandırması](#prometheus-yapılandırması)
  - [Grafana Yapılandırması](#grafana-yapılandırması)
- [Dashboard'lar](#dashboardlar)
- [Alarm Yapılandırması](#alarm-yapılandırması)

## Genel Bakış

GoMQTT, çeşitli performans metriklerini Prometheus formatında sunar. Bu metrikler şunları içerir:

- Bağlı istemci sayısı
- Mesaj alış/veriş oranları
- QoS düzeylerine göre mesaj istatistikleri
- Sistem kaynak kullanımı (CPU, bellek)
- Abonelik sayıları
- Kimlik doğrulama istatistikleri
- İşlem süreleri
- Cluster durumu

## Kurulum

### Prometheus Kurulumu

#### Docker ile Kurulum

```bash
docker run -d --name prometheus \
  -p 9090:9090 \
  -v $(pwd)/prometheus.yml:/etc/prometheus/prometheus.yml \
  prom/prometheus
```

#### Kubernetes ile Kurulum

Kubernetes için [kube-prometheus-stack](https://github.com/prometheus-community/helm-charts/tree/main/charts/kube-prometheus-stack) kullanabilirsiniz:

```bash
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update
helm install prometheus prometheus-community/kube-prometheus-stack -f prometheus-values.yaml
```

### Grafana Kurulumu

#### Docker ile Kurulum

```bash
docker run -d --name grafana \
  -p 3000:3000 \
  -v $(pwd)/grafana-data:/var/lib/grafana \
  grafana/grafana
```

#### Kubernetes ile Kurulum

`kube-prometheus-stack` Grafana'yı da içerir. Ayrı kurulum gerektirmez.

## Yapılandırma

### Prometheus Yapılandırması

GoMQTT metriklerini toplamak için `prometheus.yml` dosyanızı şu şekilde düzenleyin:

```yaml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: "gomqtt"
    static_configs:
      - targets: ["localhost:9090"] # GoMQTT metrics endpoint

  - job_name: "gomqtt-cluster"
    static_configs:
      - targets:
          - "gomqtt-node1:9090"
          - "gomqtt-node2:9090"
          - "gomqtt-node3:9090"
```

### Grafana Yapılandırması

1. Grafana'ya tarayıcınızdan erişin: `http://localhost:3000` (varsayılan kullanıcı/şifre: admin/admin)
2. Veri kaynağı olarak Prometheus'u ekleyin:
   - Configuration > Data Sources > Add data source
   - Prometheus'u seçin
   - URL olarak Prometheus adresinizi girin (örn: `http://prometheus:9090`)
   - Save & Test'e tıklayın

## Dashboard'lar

GoMQTT için iki hazır Grafana dashboard'u bulunmaktadır:

1. **GoMQTT Metrics Dashboard** - Temel metrikler ve sistem durumu
2. **GoMQTT Cluster Dashboard** - Cluster durumu ve node bazlı performans

### Dashboard'ları İçe Aktarma

1. Grafana'da + işaretine tıklayın ve "Import"u seçin
2. Dashboard ID'si olarak `gomqtt-metrics` veya `gomqtt-cluster` girin

   Alternatif olarak, JSON dosyalarını doğrudan yükleyebilirsiniz:

   - `metrics/dashboards/gomqtt_dashboard.json`
   - `metrics/dashboards/cluster_dashboard.json`

## Alarm Yapılandırması

Kritik metrikler için alarm yapılandırması örneği:

### Yüksek CPU Kullanımı Alarmı

```yaml
groups:
  - name: gomqtt_alerts
    rules:
      - alert: HighCpuUsage
        expr: gomqtt_system_cpu_percent > 80
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Yüksek CPU kullanımı ({{ $value }}%)"
          description: "GoMQTT instance'ı {{ $labels.instance }} 5 dakikadan uzun süredir %80'in üzerinde CPU kullanıyor."

      - alert: TooManyConnections
        expr: gomqtt_connected_clients > 50000
        for: 2m
        labels:
          severity: warning
        annotations:
          summary: "Çok sayıda bağlantı ({{ $value }})"
          description: "GoMQTT instance'ı {{ $labels.instance }} 50,000'den fazla eşzamanlı bağlantıya sahip."

      - alert: HighMessageLatency
        expr: histogram_quantile(0.95, sum(rate(gomqtt_message_processing_seconds_bucket[5m])) by (le)) > 0.1
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Yüksek mesaj işleme gecikmesi ({{ $value }}s)"
          description: "Mesaj işleme için 95. yüzdelik değeri 100ms'den büyük."

      - alert: NodeDown
        expr: up{job="gomqtt-cluster"} == 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "Node down: {{ $labels.instance }}"
          description: "GoMQTT cluster node'u {{ $labels.instance }} en az 1 dakikadır erişilemez durumda."
```

Bu alarm kurallarını Prometheus'a eklemek için, yukarıdaki konfigürasyonu `alerts.yml` dosyasına kaydedin ve Prometheus yapılandırmasına ekleyin:

```yaml
rule_files:
  - "alerts.yml"
```

## Metrik Listesi

GoMQTT tarafından sağlanan tüm metrikler:

| Metrik                                  | Tür       | Açıklama                                    |
| --------------------------------------- | --------- | ------------------------------------------- |
| gomqtt_connected_clients                | Gauge     | Bağlı istemci sayısı                        |
| gomqtt_connections_total                | Counter   | Toplam bağlantı sayısı                      |
| gomqtt_messages_received_total          | Counter   | Alınan toplam mesaj sayısı                  |
| gomqtt_messages_sent_total              | Counter   | Gönderilen toplam mesaj sayısı              |
| gomqtt_active_subscriptions             | Gauge     | Aktif abonelik sayısı                       |
| gomqtt_subscribe_operations_total       | Counter   | Toplam abone olma işlemi sayısı             |
| gomqtt_unsubscribe_operations_total     | Counter   | Toplam abonelikten çıkma işlemi sayısı      |
| gomqtt_qos_messages_received_total      | Counter   | QoS seviyesine göre alınan mesaj sayısı     |
| gomqtt_qos_messages_sent_total          | Counter   | QoS seviyesine göre gönderilen mesaj sayısı |
| gomqtt_retained_messages                | Gauge     | Saklanan mesaj sayısı                       |
| gomqtt_system_memory_bytes              | Gauge     | Bellek kullanımı (byte)                     |
| gomqtt_system_cpu_percent               | Gauge     | CPU kullanımı (yüzde)                       |
| gomqtt_auth_success_total               | Counter   | Başarılı kimlik doğrulama sayısı            |
| gomqtt_auth_failure_total               | Counter   | Başarısız kimlik doğrulama sayısı           |
| gomqtt_message_size_bytes               | Histogram | Mesaj boyutu dağılımı                       |
| gomqtt_message_processing_seconds       | Histogram | Mesaj işleme süresi                         |
| gomqtt_cluster_nodes_active             | Gauge     | Aktif cluster node sayısı                   |
| gomqtt_cluster_messages_forwarded_total | Counter   | Diğer node'lara iletilen mesaj sayısı       |
