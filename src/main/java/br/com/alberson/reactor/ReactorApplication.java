package br.com.alberson.reactor;

import br.com.alberson.reactor.consumer.StreamConsumer;
import br.com.alberson.reactor.producer.StreamProducer;
import br.com.alberson.reactor.serdes.JsonPOJOSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import reactor.core.publisher.Flux;

import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

@SpringBootApplication
public class ReactorApplication {

    private static final String DISPONIBILIDADE = "disponibilidade";

    public static void main(String[] args) {
        SpringApplication.run(ReactorApplication.class, args);

        Properties config = new Properties();

        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonPOJOSerializer.class);

        config.put(ProducerConfig.LINGER_MS_CONFIG, "10000");

        config.put(ProducerConfig.ACKS_CONFIG, "1");
        config.put(ProducerConfig.BATCH_SIZE_CONFIG, "10");

        KafkaProducer<Long, StreamProducer.Disponibilidade> producer = new KafkaProducer<>(config);

        Flux<StreamProducer.Disponibilidade> flux = new StreamProducer().disponibilidadeFlux();
        flux.subscribe(d -> producer.send(new ProducerRecord<>(DISPONIBILIDADE, d.getCorrelationId(), d)));

        StreamConsumer evenConsumer = new StreamConsumer((k, v) -> v.getCorrelationId() % 2 == 0);
        StreamConsumer oddConsumer = new StreamConsumer((k, v) -> v.getCorrelationId() % 2 != 0);

        CompletableFuture.runAsync(evenConsumer);
        CompletableFuture.runAsync(oddConsumer);
    }
}
