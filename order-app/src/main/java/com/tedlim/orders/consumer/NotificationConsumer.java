package com.tedlim.orders.consumer;

import com.tedlim.orders.model.Order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

/**
 * CONSUMER #2 — Bildirim Servisi. AYNI "orders" topic'ini dinler,
 * ama FARKLI bir consumer group ile: "notification-service".
 *
 * 🔑 İşte Kafka'nın gücü:
 * StockConsumer  -> groupId = "stock-service"
 * Bu servis      -> groupId = "notification-service"
 *
 * Farklı group oldukları için Kafka, her sipariş mesajının BİRER KOPYASINI
 * her iki gruba da gönderir. Yani tek siparişte hem stok düşülür hem bildirim
 * gider — birbirinden habersiz, bağımsız çalışırlar.
 *
 * NOT: Producer kodunda HİÇBİR değişiklik yapmadık. Yeni bir tüketici eklemek
 * sadece "panoyu okumaya başlamak" kadar kolay.
 */
@Service
public class NotificationConsumer {

    private static final Logger log = LoggerFactory.getLogger(NotificationConsumer.class);

    @KafkaListener(topics = "${app.kafka.orders-topic}", groupId = "notification-service")
    public void handleOrder(Order order) {
        log.info("📧 [BILDIRIM] '{}' siparisi alindi. Musteriye e-posta gonderiliyor -> {}",
                order.orderId(), order.customer());

        // Gerçek hayatta burada e-posta/SMS gönderilirdi. Simüle ediyoruz:
        log.info("📧 [BILDIRIM] Sayin {}, '{}' siparisiniz alindi. Tutar: {} TL ✅",
                order.customer(), order.orderId(), order.totalPrice());
    }
}
