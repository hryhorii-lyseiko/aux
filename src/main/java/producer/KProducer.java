package producer;

import fibonachi.Fibonachi;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;


import java.util.List;
import java.util.Properties;

public class KProducer {

    private static Fibonachi fibo = new Fibonachi();

    public static void main(String[] args) throws Exception {

        if (args.length == 0) {
            System.out.println("Enter topic name");
            return;
        }

        //reading arguments
        String topicName = args[0];
        Integer size = Integer.parseInt(args[1]);

        //creating producer with properties
        Properties producerProperties = initProducersProperties();
        Producer<String, String> producer = new KafkaProducer<>(producerProperties);
        if (size <= 0){
            System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
            System.out.println("!!!!!!!!!!!!!Wrong amount!!!!!!!!!!!!!!!!!");
            System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
            producer.close();
        }

        List<Integer> fibonacciNumbers = fibo.createFList(size);


        System.out.println("Producer send " + size + " fibonachi numbers");


        //sending numbers
        for (Integer fNumber : fibonacciNumbers) {
            producer.send(new ProducerRecord<String, String>(topicName, fNumber.toString()));
            System.out.print(fNumber.toString() + " ");
        }

        System.out.println();


        producer.close();

    }

    private static Properties initProducersProperties() {

        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put("group.id", "fibonachi");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        return props;
    }

}