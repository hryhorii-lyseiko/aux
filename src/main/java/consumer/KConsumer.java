package consumer;

import fibonachi.Fibonachi;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class KConsumer {

    private static Fibonachi fibonacci = new Fibonachi();


    public static void main(String[] args) throws Exception {

        if (args.length == 0) {
            System.out.println("Enter topic name");
            return;
        }

        //reading arguments
        String topicName = args[0];
        int part = Integer.parseInt(args[1]);

        Properties consumerProperties = initConsumerProperties();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties);

        if (part <= 0){
            System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
            System.out.println("!!!!!!!!!!!!!Wrong amount!!!!!!!!!!!!!!!!!");
            System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
            consumer.close();
        }

        consumer.subscribe(Collections.singletonList(topicName));
        System.out.println("Subscribed to topic " + topicName);

        List<Integer> fNumbers = new ArrayList<>();

        ConsumerRecords<String, String> records = consumer.poll(7000);

        for (ConsumerRecord<String, String> record : records) {
            fNumbers.add(Integer.valueOf(record.value()));
        }

        for (Integer item : fNumbers) {
            System.out.print(item + " ");
        }

        System.out.println();


        fibonacci.sumOfNumbers(fNumbers, part);

        consumer.close();
    }

    private static Properties initConsumerProperties() {

        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put("group.id", "fibonachi");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        return props;
    }

}