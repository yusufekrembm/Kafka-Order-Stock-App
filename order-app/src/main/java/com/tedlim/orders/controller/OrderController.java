package com.tedlim.orders.controller;

import com.tedlim.orders.model.Order;
import com.tedlim.orders.producer.OrderProducer;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST giriş kapısı. Müşteri (ya da test aracımız) buraya HTTP isteği atar.
 *
 * POST /orders  -> gövdesinde JSON sipariş -> OrderProducer ile Kafka'ya yazılır.
 *
 * Dikkat: Bu controller siparişi kimin işleyeceğini BİLMEZ. Sadece panoya asar.
 * Stok/Bildirim servislerinden haberi bile yok. İşte "gevşek bağlılık" bu.
 */
@RestController
@RequestMapping("/orders")
public class OrderController {

    private final OrderProducer orderProducer;

    public OrderController(OrderProducer orderProducer) {
        this.orderProducer = orderProducer;
    }

    @PostMapping
    public ResponseEntity<String> createOrder(@RequestBody Order order) {
        orderProducer.sendOrder(order);
        return ResponseEntity.ok("Siparis alindi ve Kafka'ya gonderildi: " + order.orderId());
    }
}
