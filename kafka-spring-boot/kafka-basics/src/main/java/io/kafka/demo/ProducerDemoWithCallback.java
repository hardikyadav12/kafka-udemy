package io.kafka.demo;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.Properties;

public class ProducerDemoWithCallback {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) {
        log.info("hello world!");

        //Create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "400");
//        properties.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, RoundRobinPartitioner.class.getName());


        //Create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //Adding a list of messages using a for-loop

        for(int j=0;j<10;j++) {

            for(int i=0;i<10;i++) {
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>("first_topic", "Sending message: " + i );
                //send data

                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if(Optional.ofNullable(exception).isEmpty()) {
                            log.info("The message has been successfully processed in kafka");
                            log.info(metadata.topic());
                            log.info(String.valueOf(metadata.timestamp()));
                            log.info(String.valueOf(metadata.offset()));
                            log.info(String.valueOf(metadata.partition()));
                        }else{
                            log.error(String.valueOf(exception));
                        }
                    }
                });
            }

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }



        //In the above for loop, it uses one partition only to send all the messages and this is called sticky partitioner
        //This was done automatically as kafka saw that multiple messages are coming so why to use round robin, lets you this approach
        //for better efficiancy



        //flush and close the producer
        producer.flush();
        producer.close();
    }
}
