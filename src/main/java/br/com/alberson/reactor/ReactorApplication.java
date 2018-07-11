package br.com.alberson.reactor;

import br.com.alberson.reactor.consumer.StreamConsumer;
import br.com.alberson.reactor.dao.DisponibilidadeDao;
import br.com.alberson.reactor.factory.SerdeFactory;
import br.com.alberson.reactor.producer.StreamProducer;
import br.com.alberson.reactor.serdes.JsonPOJOSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import reactor.core.publisher.Flux;
import reactor.core.publisher.TopicProcessor;
import reactor.util.concurrent.WaitStrategy;

import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;

@SpringBootApplication
public class ReactorApplication {

    private static final String DISPONIBILIDADE = "disponibilidade";

    private static final Logger LOG = LoggerFactory.getLogger(ReactorApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(ReactorApplication.class, args);

        Properties config = new Properties();

        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonPOJOSerializer.class);
        config.put(ProducerConfig.LINGER_MS_CONFIG, "1");
        config.put(ProducerConfig.ACKS_CONFIG, "0");

        TopicProcessor.Builder<StreamProducer.Disponibilidade> topicBuilder = TopicProcessor.builder();
        TopicProcessor<StreamProducer.Disponibilidade> subscriber = topicBuilder
                .name("Stream")
                .waitStrategy(WaitStrategy.yielding())
                .autoCancel(false)
                .share(true)
                .build();

        final DisponibilidadeDao disponibilidadeDao = new DisponibilidadeDao();

        Flux<StreamProducer.Disponibilidade> oneFlux = subscriber.filter(one);
        Flux<StreamProducer.Disponibilidade> zeroFlux = subscriber.filter(zero);

        oneFlux.subscribe(d -> {
            LOG.info("************** Recebi a disponibilidade *UM* [" + d.getCorrelationId() + "]");
            disponibilidadeDao.delete(d);
        });

        zeroFlux.subscribe(d -> {
            LOG.info("************** Recebi a disponibilidade *ZERO* [" + d.getCorrelationId() + "]");
            disponibilidadeDao.delete(d);
        });


        StreamConsumer evenConsumer = new StreamConsumer(disponibilidadeDao, subscriber);
        StreamConsumer oddConsumer = new StreamConsumer(disponibilidadeDao, subscriber);

        CompletableFuture<Void> futureEven = CompletableFuture.runAsync(evenConsumer);
        futureEven.thenRunAsync(oddConsumer)
                .thenRun(() -> {
                    Serde<StreamProducer.Disponibilidade> disponibilidadeSerde = SerdeFactory.createSerde(StreamProducer.Disponibilidade.class, new HashMap<>());
                    KafkaProducer<String, StreamProducer.Disponibilidade> producer = new KafkaProducer<>(config, Serdes.String().serializer(), disponibilidadeSerde.serializer());

                    Flux<StreamProducer.Disponibilidade> flux = new StreamProducer().disponibilidadeFlux();
                    flux.subscribe(d -> {
                        disponibilidadeDao.add(d);
                        ProducerRecord<String, StreamProducer.Disponibilidade> record = new ProducerRecord<>(DISPONIBILIDADE, d.getCorrelationId(), d);
                        producer.send(record);
                    });
                });
    }

    private static Predicate<StreamProducer.Disponibilidade> one = disponibilidade -> disponibilidade.getCorrelationId().startsWith("1");
    private static Predicate<StreamProducer.Disponibilidade> zero = disponibilidade -> disponibilidade.getCorrelationId().startsWith("0");
}
