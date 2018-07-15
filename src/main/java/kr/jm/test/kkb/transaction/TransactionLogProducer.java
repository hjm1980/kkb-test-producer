package kr.jm.test.kkb.transaction;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import kr.jm.test.kkb.output.LineFileWriter;
import kr.jm.test.kkb.output.StringKafkaProducer;
import kr.jm.test.kkb.transaction.log.TransactionLogInterface;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TransactionLogProducer implements AutoCloseable {

    private static final org.slf4j.Logger log =
            org.slf4j.LoggerFactory.getLogger(TransactionLogProducer.class);

    private ObjectMapper objectMapper;
    private TransactionLogGenerator transactionLogGenerator;
    private LineFileWriter lineFileWriter;
    private StringKafkaProducer stringKafkaProducer;

    public TransactionLogProducer(String bootstrapServers,
            String defaultTopic) {
        this.objectMapper = new ObjectMapper()
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .enable(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL);
        this.transactionLogGenerator = new TransactionLogGenerator(100,
                12345678);
        Path archiveFilePath = FileSystems.getDefault().getPath("archive.log")
                .toAbsolutePath();
        try {
            Files.deleteIfExists(archiveFilePath);
            Files.createFile(archiveFilePath);
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.lineFileWriter =
                new LineFileWriter(archiveFilePath);
        this.stringKafkaProducer =
                new StringKafkaProducer(bootstrapServers, defaultTopic);
    }

    public static void main(String[] args) {
        if (args.length < 3) {
            String message =
                    "Wrong Args !!! - Args: <kafkaConnect> <topic> " +
                            "<userNamesAsCSV> [delayMillis:default=100]";
            System.err.println(message);
        }

        String bootstrapServers = args[0];
        String defaultTopic = args[1];
        String[] userNames = args[2].split(",");
        long delayMillis = args.length > 3 ? Long.valueOf(args[3]) : 100;

        ExecutorService executorService = Executors.newSingleThreadExecutor();

        long startTimestamp = System.currentTimeMillis();

        executorService.execute(() -> {
            TransactionLogProducer transactionLogProducer =
                    new TransactionLogProducer(bootstrapServers, defaultTopic);
            System.out.println(
                    LocalDateTime
                            .ofInstant(Instant.ofEpochMilli(startTimestamp),
                                    ZoneId.systemDefault()) +
                            " TransactionLogGenerator Start !!!" +
                            " - " +
                            bootstrapServers + " " + defaultTopic + " " +
                            args[2] +
                            " " +
                            delayMillis + "ms");
            transactionLogProducer
                    .generateTransactionLog(delayMillis, userNames);
            transactionLogProducer.close();
        });
        executorService.shutdown();
        while (!executorService.isTerminated()) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println(System.currentTimeMillis());
        }
        long stopTimestamp = System.currentTimeMillis();
        System.out.println(
                LocalDateTime.ofInstant(Instant.ofEpochMilli(stopTimestamp),
                        ZoneId.systemDefault()) +
                        " TransactionLogGenerator Stop !!! - tookMills = " +
                        (stopTimestamp - startTimestamp));
    }

    public void generateTransactionLog(long delayMillis, String... userNames) {
        this.transactionLogGenerator
                .generateAbout100TransactionLogEach(this::writeAndSend,
                        delayMillis, userNames);
    }

    public void writeAndSend(
            TransactionLogInterface transactionLog) {
        String jsonTransactionLog = buildJsonString(transactionLog);
        log.info(jsonTransactionLog);
        System.out.println(jsonTransactionLog);
        this.lineFileWriter.appendLine(jsonTransactionLog);
        this.stringKafkaProducer
                .sendStringData(String.valueOf(transactionLog.getUserNumber()),
                        jsonTransactionLog);
    }

    private String buildJsonString(Object dataObject) {
        try {
            return objectMapper.writeValueAsString(dataObject);
        } catch (Exception e) {
            log.error("buildJSonString({})", dataObject, e);
            return null;
        }
    }


    @Override
    public void close() {
        this.lineFileWriter.close();
        this.stringKafkaProducer.close();
    }
}
