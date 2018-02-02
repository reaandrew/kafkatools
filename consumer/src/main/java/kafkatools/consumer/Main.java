package kafkatools.consumer;

import java.util.Arrays;
import java.util.Properties;
import java.util.Scanner;
import java.util.Map;
import java.lang.Runtime;

import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;

import picocli.CommandLine;

public class Main {
      private static Scanner in;
      private static boolean stop = false;

      public static void main(String[] argv) throws Exception{

        CommandLineArguments config = CommandLine.populateCommand(new CommandLineArguments(), argv);

        String topic = config.topic;
        String groupId = config.groupId;
        String brokers = config.brokers;

        ConsumerThread consumerRunnable = new ConsumerThread(topic,groupId, brokers);
        Runtime.getRuntime().addShutdownHook(new Thread() {
              public void run() {
                consumerRunnable.getKafkaConsumer().wakeup();
                System.out.println("Stopping consumer .....");
                try{
                  consumerRunnable.join();
                }catch(Exception ex){

                }
              }
        });
        consumerRunnable.start();
        for(;;);
      }

      private static class ConsumerThread extends Thread{
          private String topicName;
          private String groupId;
          private String brokers;
          private KafkaConsumer<String,String> kafkaConsumer;

          public ConsumerThread(String topicName, String groupId, String brokers){
              this.topicName = topicName;
              this.groupId = groupId;
              this.brokers = brokers;
          }
          public void run() {

              String kafkaServers = brokers;

              Properties configProperties = new Properties();
              configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers);
              configProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
              configProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
              configProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
              configProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, "simple_consumer");

              //Figure out where to start processing messages from
              kafkaConsumer = new KafkaConsumer<String, String>(configProperties);
              kafkaConsumer.subscribe(Arrays.asList(topicName));
              //Start processing messages
              try {
                  while (true) {
                      ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
                      for (ConsumerRecord<String, String> record : records)
                          System.out.println(record.value());
                  }
              }catch(WakeupException ex){
                  System.out.println("Exception caught " + ex.getMessage());
              }finally{
                  kafkaConsumer.close();
                  System.out.println("After closing KafkaConsumer");
              }
          }
          public KafkaConsumer<String,String> getKafkaConsumer(){
             return this.kafkaConsumer;
          }
      }
  }
