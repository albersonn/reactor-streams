package br.com.alberson.reactor.consumer;

import br.com.alberson.reactor.dao.DisponibilidadeDao;
import br.com.alberson.reactor.factory.SerdeFactory;
import br.com.alberson.reactor.producer.StreamProducer;
import lombok.NonNull;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Properties;

public class StreamConsumer implements Runnable{

    private static final Logger LOG = LoggerFactory.getLogger(StreamConsumer.class);

    private final Properties config;

    @NonNull
    private Predicate<? super String, ? super StreamProducer.Disponibilidade> filterPredicate;
    private final DisponibilidadeDao disponibilidadeDao;

    public StreamConsumer(Predicate<? super String, ? super StreamProducer.Disponibilidade> filterPredicate, DisponibilidadeDao disponibilidadeDao) {
        this(disponibilidadeDao);
        this.filterPredicate = filterPredicate;
    }

    public StreamConsumer(DisponibilidadeDao disponibilidadeDao) {
        this.disponibilidadeDao = disponibilidadeDao;
        config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "reactor_app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    }

    @Override
    public void run() {
        StreamsBuilder builder = new StreamsBuilder();

        Serde<StreamProducer.Disponibilidade> disponibilidadeSerde = SerdeFactory.createSerde(StreamProducer.Disponibilidade.class, new HashMap<>());

        KStream<String, StreamProducer.Disponibilidade> disponibilidadeStream = builder.stream("disponibilidade", Consumed.with(Serdes.String(), disponibilidadeSerde));
        if (this.filterPredicate != null) {
            disponibilidadeStream.filter(this.filterPredicate);
        }

        disponibilidadeStream.peek((k, v) -> {
            LOG.info("Capturei o numero [{}]", v.getCorrelationId());
            this.disponibilidadeDao.delete(v);
        });

        KafkaStreams streams = new KafkaStreams(builder.build(), config);

        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
