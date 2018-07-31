import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;

import java.util.Arrays;
import java.util.Properties;
import java.util.Scanner;

public class KafkaConsumer {
    private static Scanner in;

    public static void main(String[] args) throws InterruptedException {
        if(args.length !=2) {
            System.out.println("TopicName and groupId need to be provided in the order");
            System.exit(-1);
        }

        in = new Scanner(System.in);
        String topicName = args[0];
        String groupId = args[1];

        ConsumerThread consumerThread = new ConsumerThread(topicName, groupId);
        consumerThread.start();

        String line = "";
        while (!line.equalsIgnoreCase("exit")) {
            line = in.next();
        }

        consumerThread.getKafkaConsumer().wakeup();
        System.out.println("Stopping consumer");
        consumerThread.join();

    }

    private static class ConsumerThread extends Thread {
        private String topicName;
        private String groupId;
        private org.apache.kafka.clients.consumer.KafkaConsumer<String, String> kafkaConsumer;

        public ConsumerThread(String topicName, String groupId) {
            this.topicName = topicName;
            this.groupId = groupId;
        }

        public void run() {
            Properties consumerProperties = new Properties();
            consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
            consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            consumerProperties.put(ConsumerConfig.CLIENT_ID_CONFIG,"simple");

            kafkaConsumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(consumerProperties);
            kafkaConsumer.subscribe(Arrays.asList(topicName));

            try {
                while (true) {
                    ConsumerRecords<String, String> records = kafkaConsumer.poll(1000);
                    for(ConsumerRecord<String, String> record: records) {
                        System.out.println(record.value());
                    }
                }
            }
            catch (WakeupException we) {
                System.out.println("Exception while consuming messages : " + we.getMessage());
            }
            finally {
                kafkaConsumer.close();
                System.out.println("Closed KafkaConsumer");
            }
        }

        public org.apache.kafka.clients.consumer.KafkaConsumer<String,String> getKafkaConsumer(){
            return this.kafkaConsumer;
        }
    }
}
