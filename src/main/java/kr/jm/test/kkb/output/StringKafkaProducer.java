package kr.jm.test.kkb.output;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serdes;

import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

public class StringKafkaProducer extends KafkaProducer<String, String> {

    private static final org.slf4j.Logger log =
            org.slf4j.LoggerFactory.getLogger(StringKafkaProducer.class);
    private String defaultTopic;
    private Properties producerProperties;

    public String getDefaultTopic() {
        return defaultTopic;
    }

    public Properties getProducerProperties() {
        return producerProperties;
    }

    public StringKafkaProducer(String bootstrapServers,
            String defaultTopic) {
        this(bootstrapServers, null, defaultTopic);
    }

    public StringKafkaProducer(String bootstrapServers, String producerId,
            String defaultTopic) {
        this(bootstrapServers, producerId, defaultTopic, 2, 16384, 33554432, 1);
    }

    public StringKafkaProducer(String bootstrapServers, String producerId,
            String defaultTopic, int retries, int batchSize, int bufferMemory,
            int lingerMs) {
        this(buildProperties(bootstrapServers, producerId, retries, batchSize,
                bufferMemory, lingerMs), defaultTopic);
    }

    public static Properties buildProperties(String bootstrapServers,
            String producerId, Integer retries, Integer batchSize,
            Integer bufferMemory, Integer lingerMs) {
        return new Properties() {{
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                    bootstrapServers);
            Optional.ofNullable(producerId).ifPresent(
                    clientId -> put(ProducerConfig.CLIENT_ID_CONFIG, clientId));
            Optional.ofNullable(retries).ifPresent(
                    retries -> put(ProducerConfig.RETRIES_CONFIG, retries));
            Optional.ofNullable(batchSize).ifPresent(
                    batchSize -> put(ProducerConfig.BATCH_SIZE_CONFIG,
                            batchSize));
            Optional.ofNullable(bufferMemory).ifPresent(
                    bufferMemory -> put(ProducerConfig.BUFFER_MEMORY_CONFIG,
                            bufferMemory));
            Optional.ofNullable(lingerMs).ifPresent(
                    lingerMs -> put(ProducerConfig.LINGER_MS_CONFIG, lingerMs));
            put(ProducerConfig.ACKS_CONFIG, "all");
        }};
    }

    public StringKafkaProducer(Properties producerProperties,
            String defaultTopic) {
        super(producerProperties, Serdes.String().serializer(),
                Serdes.String().serializer());
        this.producerProperties = producerProperties;
        this.defaultTopic = defaultTopic;
    }

    public Future<RecordMetadata> sendStringData(String key,
            String stringData) {
        log.debug("sendStringData({}, {}, {})", defaultTopic, key, stringData);
        return Optional.ofNullable(stringData).map(
                string -> send(new ProducerRecord<>(defaultTopic, key, string)))
                .orElseGet(() -> CompletableFuture.completedFuture(null));
    }

}
