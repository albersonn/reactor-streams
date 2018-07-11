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
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

@Component
public class StreamProducer {

    private static final Logger LOG = LoggerFactory.getLogger(StreamProducer.class);

    public Flux<Disponibilidade> disponibilidadeFlux() {

        List<Flux<Disponibilidade>> fluxes = Arrays.asList(obterFluxDisponibilidade(1000),
                obterFluxDisponibilidade(700),
                obterFluxDisponibilidade(300),
                obterFluxDisponibilidade(400),
                obterFluxDisponibilidade(200),
                obterFluxDisponibilidade(150),
                obterFluxDisponibilidade(350),
                obterFluxDisponibilidade(500));

        return fluxes.stream().reduce(Flux.empty(), Flux::mergeWith);
    }


    private Flux<Disponibilidade> obterFluxDisponibilidade(final int milis) {
        return Flux.fromIterable(disps())
                .delayElements(Duration.ofMillis(milis));
    }

    private List<Disponibilidade> disps() {
        ArrayList<Disponibilidade> list = new ArrayList<>();
        for (int i = 0; i < ThreadLocalRandom.current().nextInt(0,50); i++) {
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
