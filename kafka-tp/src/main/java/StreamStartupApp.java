
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

public class StreamStartupApp {
    public static void main(String[] args) {
        Properties props = new Properties();
        /*
         * ApplicationId is specific to application which is used for consumer
         * groupId/default clientId prefix
         */
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-stream-wrod-count");
        /* List of IP address:port that need to be connected */
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9091, 127.0.0.1:9092");
        /* Start topic consumption from Start */
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        /* For serialization and desrialization of data */
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        KStreamBuilder builder = new KStreamBuilder();
        /* Stream from Kafka : name of topic */
        KStream<String, String> wordCountInputStream = builder.stream("word-count-input-topic");

        /* Convert word to lowercase */
        KTable<String, Long> wordCounts = wordCountInputStream.mapValues(textLineInput -> textLineInput.toLowerCase())
                /* Convert word to lowercase */
                .flatMapValues(lowerCaseTextLine -> Arrays.asList(lowerCaseTextLine.split(" ")))
                /* Select key */
                .selectKey((ignoredKey, value) -> value)
                /* Using default grouping using Serdes.String().getClass() */
                .groupByKey()
                /* Compute word count*/
                .count("Count");
        /* Publish wordcounts output to output kafka */
        wordCounts.to(Serdes.String(), Serdes.Long(), "word-count-output-topic");

        KafkaStreams kafkaStreams = new KafkaStreams(builder, props);
        kafkaStreams.start();

        /*Print topology details*/
        System.out.println(kafkaStreams);

        /* Graceful degradation of app - add Shutdownhook */
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }
}