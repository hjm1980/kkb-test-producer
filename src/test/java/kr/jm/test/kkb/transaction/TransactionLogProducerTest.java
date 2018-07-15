package kr.jm.test.kkb.transaction;

import kr.jm.test.kkb.output.StringKafkaProducer;
import kr.jm.utils.helper.JMPath;
import kr.jm.utils.helper.JMPathOperation;
import kr.jm.utils.helper.JMThread;
import kr.jm.utils.kafka.JMKafkaBroker;
import kr.jm.utils.kafka.client.JMKafkaConsumer;
import kr.jm.utils.zookeeper.JMZookeeperServer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class TransactionLogProducerTest {

    static {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "info");
    }

    private String topic = "testLocal";
    private JMZookeeperServer zooKeeper;
    private JMKafkaBroker kafkaBroker;
    private JMKafkaConsumer kafkaConsumer;
    private String bootstrapServer;
    private StringKafkaProducer stringKafkaProducer;

    /**
     * Sets the up.
     *
     * @throws Exception the exception
     */
    @Before
    public void setUp() throws Exception {
        Optional.of(JMPath.getPath("zookeeper-dir")).filter(JMPath::exists)
                .ifPresent(JMPathOperation::deleteDir);
        Optional.of(JMPath.getPath("kafka-broker-log")).filter(JMPath::exists)
                .ifPresent(JMPathOperation::deleteDir);
        this.zooKeeper = new JMZookeeperServer("localhost", 12181);
        this.zooKeeper.start();
        JMThread.sleep(3000);
        this.kafkaBroker =
                new JMKafkaBroker(zooKeeper.getZookeeperConnect(), "localhost",
                        9300);
        this.kafkaBroker.startup();
        JMThread.sleep(3000);
        this.bootstrapServer = "localhost:9300";
        this.stringKafkaProducer =
                new StringKafkaProducer(bootstrapServer, topic);

    }

    /**
     * Tear down.
     */
    @After
    public void tearDown() {
        {
            stringKafkaProducer.close();
            kafkaConsumer.close();
            kafkaBroker.stop();
            zooKeeper.stop();
            Optional.of(JMPath.getPath("zookeeper-dir")).filter(JMPath::exists)
                    .ifPresent(JMPathOperation::deleteDir);
            Optional.of(JMPath.getPath("kafka-broker-log"))
                    .filter(JMPath::exists)
                    .ifPresent(JMPathOperation::deleteDir);
        }
    }

    @Test
    public void testMain() throws InterruptedException {
        List<ConsumerRecord> consumerRecordList =
                Collections.synchronizedList(new ArrayList<>());
        this.kafkaConsumer = new JMKafkaConsumer(false, bootstrapServer,
                "testGroup", consumerRecords -> {
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println(consumerRecord);
                consumerRecordList.add(consumerRecord);
            }
        }, topic);
        this.kafkaConsumer.start();
        TransactionLogProducer
                .main(new String[]{bootstrapServer, topic, "jemin,제민"});
        Thread.sleep(5000);
        System.out.println(consumerRecordList.size());


        Assert.assertTrue(183 < consumerRecordList.size() &&
                consumerRecordList.size() < 225);
    }
}