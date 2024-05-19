package org.wikimedia.producer;

import com.launchdarkly.eventsource.EventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaProducer {

    static KafkaProducer<String, String> kafkaProducer;
    static String wikimedia_url = "https://stream.wikimedia.org/v2/stream/recentchange";

    public static void main(String[] args) throws IOException, InterruptedException {

        Properties properties = getProperties();
        kafkaProducer = new KafkaProducer<>(properties);

        WikimediaChangeHandler eventHandler = new WikimediaChangeHandler(kafkaProducer, "demo_topic");
        EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(wikimedia_url));
        EventSource eventSource = builder.build();

        //start the producer in another thread
        eventSource.start();

        TimeUnit.MINUTES.sleep(10);

//        URL getWikimediaEventUrl = new URL(wikimedia_url);
//        HttpsURLConnection connection = (HttpsURLConnection) getWikimediaEventUrl.openConnection();
//        connection.setRequestMethod("GET");
//
//        if(connection.getResponseCode() == HttpURLConnection.HTTP_OK) {
//            BufferedReader br = new BufferedReader(new InputStreamReader(connection.getInputStream()));
//            String response;
//            String inputLine;
//
//            while(Optional.ofNullable(inputLine = br.readLine()).isPresent()) {
//                if(inputLine.startsWith("data:")) {
//                    response = inputLine;
//                    sendMessageToKafkaTopic(response);
//                    response = null;
//                }
//            }
//        }

        // flush and close the kafka
//        kafkaProducer.flush();
//        kafkaProducer.close();

    }

    private static Properties getProperties() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
//        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
//        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
//        properties.setProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "12000");
        return properties;
    }
//
//    private static void sendMessageToKafkaTopic(String response) {
//        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_topic", response);
//        kafkaProducer.send(producerRecord);
//
//    }
}