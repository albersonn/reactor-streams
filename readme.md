## Setup

1. Iniciando um cluster Kafka localmente 
    ```
    docker run --rm -it \
               -p 2181:2181 -p 3030:3030 -p 8081:8081 \
               -p 8082:8082 -p 8083:8083 -p 9092:9092 \
               -e ADV_HOST=127.0.0.1 \
               landoop/fast-data-dev
    ```
    
    Este bash pode ficar em segundo plano.

2. Depois em outro bash para ter acesso ao bash do Kafka
    
    ```
    docker run --rm -it --net=host landoop/fast-data-dev bash
    ```
    
3. No bash do kafka, crie o tópico

    ```
    kafka-topics --zookeeper 127.0.0.1:2181 \
                --create --topic disponibilidade \
                --partitions 3 \
                --replication-factor 1
    ```
4. Inicie a aplicação via `SpringBoot`.