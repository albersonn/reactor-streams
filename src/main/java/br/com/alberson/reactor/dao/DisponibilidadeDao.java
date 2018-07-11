package br.com.alberson.reactor.dao;

import br.com.alberson.reactor.producer.StreamProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class DisponibilidadeDao {

    private static final Logger LOG = LoggerFactory.getLogger(DisponibilidadeDao.class);

    private final Map<String, StreamProducer.Disponibilidade> data = new HashMap<>();

    public synchronized void add(StreamProducer.Disponibilidade disponibilidade) {
        this.data.put(disponibilidade.getCorrelationId(), disponibilidade);
        logDangling();
    }

    public synchronized void delete(StreamProducer.Disponibilidade disponibilidade) {
        this.data.remove(disponibilidade.getCorrelationId());
        logDangling();
    }

    public synchronized void logDangling() {
        LOG.info("*************************** Existem [" + data.values().size() + "] disponibilidades órfãs. ***************************");
    }
}
