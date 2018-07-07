package br.com.alberson.reactor.producer;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.ThreadLocalRandom;

@Component
public class StreamProducer {

    private static final Logger LOG = LoggerFactory.getLogger(StreamProducer.class);

    public Flux<Disponibilidade> disponibilidadeFlux() {
//        return Flux.<Disponibilidade>generate(sink -> sink.next(new Disponibilidade(gerarId())))
//                .delayElements(Duration.ofSeconds(1));

        ArrayList<Disponibilidade> disps = new ArrayList<>();
        for (long i = 0; i < 11; i++) {
            disps.add(new Disponibilidade(i));
        }

        return Flux.fromIterable(disps);
    }

//    private Long gerarId() {
//        long i = ThreadLocalRandom.current().nextLong(1655435756156L, 1655435756356L);
//        boolean impar = i % 2L != 0;
//        LOG.info("Gerando número [" + (impar ? "ÍMPAR" : "PAR") + "]: [" + i + "]");
//        return i;
//    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Disponibilidade {
        @NonNull
        private Long correlationId;
    }
}
