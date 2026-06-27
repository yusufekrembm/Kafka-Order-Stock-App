package com.tedlim.orders.consumer;

import com.tedlim.orders.model.Order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

/**
 * CONSUMER — Stok Servisi. "orders" topic'ini (panoyu) sürekli dinler.
 *
 * @KafkaListener: Spring'in büyüsü burada. Bu metoda bu anotasyonu koyunca
 * Spring arka planda sürekli topic'i dinleyen bir consumer çalıştırır.
 * Yeni mesaj düştüğü an bu metodu BİZİM İÇİN otomatik çağırır.
 *
 * Gelen JSON, application.properties'teki ayarlar sayesinde otomatik olarak
 * Order nesnesine çevrilir (deserialize). Biz hazır Order alıyoruz.
 *
 * Not: Bu servis siparişin nereden geldiğini, kimin gönderdiğini BİLMEZ.
 * Sadece "orders" panosunu okur. Producer ile arasında hiçbir doğrudan bağ yok.
 */
@Service
public class StockConsumer {

    private static final Logger log = LoggerFactory.getLogger(StockConsumer.class);

    // concurrency = "3" -> bu grupta 3 paralel consumer (thread) calistir.
    // 3 partition'imiz oldugu icin her thread'e 1 partition duser.
    @KafkaListener(topics = "${app.kafka.orders-topic}", groupId = "stock-service", concurrency = "3")
    public void handleOrder(Order order,
                            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition) {
        // Hangi partition'dan geldi, hangi thread isliyor? -> ölçeklemeyi gözle görelim
        String thread = Thread.currentThread().getName();
        log.info("📦 [STOK] partition={} | thread={} | siparis={} ({} adet '{}')",
                partition, thread, order.orderId(), order.quantity(), order.product());
    }
}
