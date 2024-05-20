package org.wikimedia.producer;



import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikimediaChangeHandler implements EventHandler {

    KafkaProducer<String, String> kafkaProducer;
    String topic;
    private final Logger log = LoggerFactory.getLogger(WikimediaChangeHandler.class.getName());

    public WikimediaChangeHandler(KafkaProducer<String, String> kafkaProducer, String topic) {
        this.kafkaProducer = kafkaProducer;
        this.topic = topic;
    }

    @Override
    public void onOpen() throws Exception {
        //nope - nothing
    }

    @Override
    public void onClosed() {
        kafkaProducer.flush();
        kafkaProducer.close();
    }

    @Override
    public void onMessage(String event, MessageEvent messageEvent) throws Exception {
        // send message to kafka topic here
        kafkaProducer.send(new ProducerRecord<>(this.topic, messageEvent.getData()));
    }

    @Override
    public void onComment(String comment) throws Exception {
        // Nothing here
    }

    @Override
    public void onError(Throwable t) {
        log.error("Error in stream reading", t);
    }
}