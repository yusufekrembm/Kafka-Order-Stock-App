package com.tedlim.orders.producer;

import com.tedlim.orders.model.Order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

/**
 * PRODUCER — Kafka'ya (panoya) mesaj yazan taraf.
 *
 * KafkaTemplate, Spring'in bizim için hazırladığı "mesaj gönderme aracı".
 * Biz sadece "şu topic'e, şu anahtarla, şu nesneyi gönder" diyoruz;
 * JSON'a çevirme, broker'a bağlanma gibi işleri Spring hallediyor.
 */
@Service
public class OrderProducer {

    private static final Logger log = LoggerFactory.getLogger(OrderProducer.class);

    private final KafkaTemplate<String, Order> kafkaTemplate;
    private final String ordersTopic;

    public OrderProducer(KafkaTemplate<String, Order> kafkaTemplate,
                         @Value("${app.kafka.orders-topic}") String ordersTopic) {
        this.kafkaTemplate = kafkaTemplate;
        this.ordersTopic = ordersTopic;
    }

    /**
     * Siparişi "orders" topic'ine gönderir.
     * Mesaj anahtarı (key) olarak orderId kullanıyoruz — ileride partition
     * konusunu işlerken bunun neden önemli olduğunu göreceğiz.
     */
    public void sendOrder(Order order) {
        log.info("Kafka'ya gonderiliyor -> topic={}, order={}", ordersTopic, order);
        kafkaTemplate.send(ordersTopic, order.orderId(), order);
    }
}
