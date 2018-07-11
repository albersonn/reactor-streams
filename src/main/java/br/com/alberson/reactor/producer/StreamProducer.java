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
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

@Component
public class StreamProducer {

    private static final Logger LOG = LoggerFactory.getLogger(StreamProducer.class);

    public Flux<Disponibilidade> disponibilidadeFlux() {
        Flux<Disponibilidade> flux1 = obterFluxDisponibilidade(1000);
        Flux<Disponibilidade> flux2 = obterFluxDisponibilidade(700);
        Flux<Disponibilidade> flux3 = obterFluxDisponibilidade(300);
        Flux<Disponibilidade> flux4 = obterFluxDisponibilidade(400);
        Flux<Disponibilidade> flux5 = obterFluxDisponibilidade(500);

        return flux1.mergeWith(flux2).mergeWith(flux3).mergeWith(flux4).mergeWith(flux5);
    }


    private Flux<Disponibilidade> obterFluxDisponibilidade(final int milis) {
        return Flux.fromIterable(tenDisps())
                .delayElements(Duration.ofMillis(milis));
    }

    private List<Disponibilidade> tenDisps() {
        ArrayList<Disponibilidade> list = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            list.add(new Disponibilidade(gerarId()));
        }
        return list;
    }

    private String gerarId() {
        String correlation = UUID.randomUUID().toString().replace("-", "");
        String b = ThreadLocalRandom.current().nextBoolean() ? "1" : "0";
        LOG.info("Gerando correlation [" + b + correlation + "]");
        return b + correlation;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Disponibilidade {
        @NonNull
        private String correlationId;
    }
}
