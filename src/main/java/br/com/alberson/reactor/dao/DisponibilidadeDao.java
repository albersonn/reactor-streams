package br.com.alberson.reactor.dao;

import br.com.alberson.reactor.producer.StreamProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class DisponibilidadeDao {

    private static final Logger LOG = LoggerFactory.getLogger(DisponibilidadeDao.class);

    private final List<StreamProducer.Disponibilidade> data = new ArrayList<>();

    public synchronized void add(StreamProducer.Disponibilidade disponibilidade) {
        this.data.add(disponibilidade);
        logDangling();
    }

    public synchronized void delete(StreamProducer.Disponibilidade disponibilidade) {
        this.data.remove(disponibilidade);
        logDangling();
    }

    public synchronized void logDangling() {
        LOG.info("*************************** Existem [" + data.size() + "] disponibilidades órfãs. ***************************");
    }
}
