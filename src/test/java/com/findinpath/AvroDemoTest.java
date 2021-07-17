/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package com.findinpath;

import akka.actor.ActorSystem;
import akka.kafka.testkit.KafkaTestkitTestcontainersSettings;
import akka.kafka.testkit.javadsl.TestcontainersKafkaJunit4Test;
import akka.testkit.javadsl.TestKit;
import com.findinpath.avro.BookmarkEvent;
import io.confluent.kafka.serializers.*;
import io.confluent.kafka.serializers.subject.TopicNameStrategy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.AfterClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class AvroDemoTest extends TestcontainersKafkaJunit4Test {

    private static final Logger LOGGER = LoggerFactory.getLogger(AvroDemoTest.class);
    private static final String URL = "https://www.findinpath.com";
    private static final long POLL_INTERVAL_MS = 100L;
    private static final long POLL_TIMEOUT_MS = 10_000L;

    private static final ActorSystem sys = ActorSystem.create("SchemaRegistrySerializationTest");
    private static final Executor ec = Executors.newSingleThreadExecutor();

    public AvroDemoTest() {
        // #schema-registry-settings
        // NOTE: Overriding KafkaTestkitTestcontainersSettings doesn't necessarily do anything here
        // because the JUnit testcontainer abstract classes run the testcontainers as a singleton.
        // Whatever JUnit test spawns first is the only one that can override settings. To workaround
        // this I've enabled the schema registry container for all tests in an application.conf.
        // #schema-registry-settings
        super(
                sys,
                KafkaTestkitTestcontainersSettings.create(sys)
                        .withInternalTopicsReplicationFactor(1)
                        .withSchemaRegistry(true));
    }

    /**
     * This demo test simply verifies whether an AVRO encoded record is successfully serialized and
     * sent over Apache Kafka by a producer in order to subsequently be deserialized and read by a
     * consumer.
     */
    @Test
    public void demo() {
        final String topic = createTopic();

        final UUID userUuid = UUID.randomUUID();
        final BookmarkEvent bookmarkEvent = new BookmarkEvent(userUuid.toString(), URL,
                Instant.now().toEpochMilli());

        produce(topic, bookmarkEvent);
        LOGGER.info(
                String.format("Successfully sent 1 BookmarkEvent message to the topic called %s", topic));

        var consumerRecords = dumpTopic(topic, 1, POLL_TIMEOUT_MS);
        LOGGER.info(String.format("Retrieved %d consumer records from the topic %s",
                consumerRecords.size(), topic));

        assertThat(consumerRecords.size(), equalTo(1));
        assertThat(consumerRecords.get(0).key(), equalTo(bookmarkEvent.getUserUuid()));
        assertThat(consumerRecords.get(0).value(), equalTo(bookmarkEvent));
    }

    private void produce(String topic, BookmarkEvent bookmarkEvent) {
        try (KafkaProducer<String, BookmarkEvent> producer = createBookmarkEventKafkaProducer()) {
            final ProducerRecord<String, BookmarkEvent> record = new ProducerRecord<>(
                    topic, bookmarkEvent.getUserUuid(), bookmarkEvent);
            producer.send(record);
            producer.flush();
        } catch (final SerializationException e) {
            LOGGER.error(String.format(
                    "Serialization exception occurred while trying to send message %s to the topic %s",
                    bookmarkEvent, topic), e);
        }
    }

    private KafkaProducer<String, BookmarkEvent> createBookmarkEventKafkaProducer() {
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, getSchemaRegistryUrl());
        props.put(KafkaAvroSerializerConfig.VALUE_SUBJECT_NAME_STRATEGY,
                TopicNameStrategy.class.getName());

        return new KafkaProducer<>(props);
    }

    private List<ConsumerRecord<String, BookmarkEvent>> dumpTopic(
            String topic,
            int minMessageCount,
            long pollTimeoutMillis) {

        List<ConsumerRecord<String, BookmarkEvent>> consumerRecords = new ArrayList<>();
        var consumerGroupId = UUID.randomUUID().toString();
        try (final KafkaConsumer<String, BookmarkEvent> consumer = createBookmarkEventKafkaConsumer(
                consumerGroupId)) {

            // assign the consumer to all the partitions of the topic
            var topicPartitions = consumer.partitionsFor(topic).stream()
                    .map(partitionInfo -> new TopicPartition(topic, partitionInfo.partition()))
                    .collect(Collectors.toList());
            consumer.assign(topicPartitions);

            var start = System.currentTimeMillis();
            while (true) {
                final ConsumerRecords<String, BookmarkEvent> records = consumer
                        .poll(Duration.ofMillis(POLL_INTERVAL_MS));

                records.forEach(consumerRecords::add);
                if (consumerRecords.size() >= minMessageCount) {
                    break;
                }

                if (System.currentTimeMillis() - start > pollTimeoutMillis) {
                    throw new IllegalStateException(
                            String.format(
                                    "Timed out while waiting for %d messages from the %s. Only %d messages received so far.",
                                    minMessageCount, topic, consumerRecords.size()));
                }
            }
        }
        return consumerRecords;
    }

    private KafkaConsumer<String, BookmarkEvent> createBookmarkEventKafkaConsumer(
            String consumerGroupId) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, getSchemaRegistryUrl());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        props.put(KafkaAvroSerializerConfig.VALUE_SUBJECT_NAME_STRATEGY, TopicNameStrategy.class.getName());

        return new KafkaConsumer<>(props);
    }

    @AfterClass
    public static void afterClass() {
        TestKit.shutdownActorSystem(sys);
    }
}
