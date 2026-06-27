package com.tedlim.orders.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Topic'i (panoyu) kod ile oluşturuyoruz.
 *
 * Bu @Bean sayesinde uygulama açılırken "orders" topic'i Kafka'da yoksa
 * otomatik oluşturulur. Böylece Kafka UI'dan elle topic açmamıza gerek kalmaz.
 *
 * partitions = 3  -> panoyu 3 parçaya böldük (paralel işleme için)
 * replicas   = 1  -> tek broker'ımız olduğu için tek kopya
 *
 * NOT: Spring (KafkaAdmin) açılışta, var olan topic'in partition sayısı
 * buradakinden AZ ise otomatik artırır. (Partition sayısı yalnızca ARTIRILABİLİR,
 * azaltılamaz — Kafka'nın kuralı.)
 */
@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic ordersTopic(@Value("${app.kafka.orders-topic}") String topicName) {
        return TopicBuilder.name(topicName)
                .partitions(3)
                .replicas(1)
                .build();
    }
}
