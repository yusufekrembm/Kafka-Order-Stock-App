package com.tedlim.orders.model;

/**
 * Bir siparişi temsil eden veri modeli.
 *
 * Java "record" kullandık: sadece veri tutan, değişmez (immutable) bir sınıf.
 * Spring, bu nesneyi otomatik olarak JSON'a çevirip Kafka'ya yazacak.
 *
 * Örnek JSON:
 * { "orderId": "ORD-1", "product": "Fren Balatası", "quantity": 2, "customer": "Ahmet", "totalPrice": 450.0 }
 */
public record Order(
        String orderId,    // siparişin benzersiz kimliği (mesaj anahtarı olarak da kullanacağız)
        String product,    // ürün adı
        int quantity,      // adet
        String customer,   // müşteri adı
        double totalPrice  // toplam tutar
) {
}
