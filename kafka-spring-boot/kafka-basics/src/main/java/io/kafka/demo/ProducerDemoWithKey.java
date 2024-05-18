package io.kafka.demo;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.Properties;

public class ProducerDemoWithKey {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) {
        log.info("hello world!");

        //Create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "400");
//        properties.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, RoundRobinPartitioner.class.getName());


        //Create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //Adding a list of messages using a for-loop

            for(int i=0;i<10;i++) {

                String topic = "demo_topic";
                String key = "id_" + i;
                String value = "Hello world " + i;

                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);
                //send data

                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if(Optional.ofNullable(exception).isEmpty()) {
                            log.info("The message has been successfully processed in kafka");
                            log.info(metadata.topic());
                            log.info(String.valueOf(metadata.offset()));
                            log.info(metadata.partition() + "Key " + key);
                        }else{
                            log.error(String.valueOf(exception));
                        }
                    }
                });

                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        //flush and close the producer
        producer.flush();
        producer.close();
        }
    }

