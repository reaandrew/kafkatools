package kafkaexamples.producer;

import java.util.Properties;
import java.util.Scanner;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class Main{

  private static Scanner in; 

  public static void main(String[] argv){
    if (argv.length != 1) {
        System.err.printf("Usage: producer.sh <topicName>\n");
        System.exit(-1);
    }

    String topicName = argv[0];
    in = new Scanner(System.in);
    System.out.println("Enter message(type exit to quit)");

    Map<String, String> env = System.getenv();
    String kafkaServers = env.get("KAFKA_SERVERS");
    Properties configProperties = new Properties();
    configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,kafkaServers);
    configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.ByteArraySerializer");
    configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");

    org.apache.kafka.clients.producer.Producer producer = new KafkaProducer<String, String>(configProperties);
    String line = in.nextLine();
    while(!line.equals("exit")) {
      System.out.println("Sending message");
      ProducerRecord<String, String> rec = new ProducerRecord<String, String>(topicName, line);
      producer.send(rec);
      line = in.nextLine();
    }
    in.close();
    producer.close();

  }
}
