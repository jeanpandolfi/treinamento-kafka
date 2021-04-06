package br.com.jeanpandolfi;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.regex.Pattern;

public class LogService {

    public static void main(String[] args) {
        var logService = new LogService();
        try(var kafkaService = new KafkaService(
                LogService.class.getSimpleName(),
                Pattern.compile("ECOMMERCE.*"),
                logService::parse,
                String.class)){
            kafkaService.run();
        }
    }

    private void parse(ConsumerRecord<String, String> record) {
        System.out.println("-----------------------------");
        System.out.println("LOGGER: " + record.topic());
        System.out.println("Key: "+ record.key());
        System.out.println("Value: "+ record.value());
        System.out.println("Partition: "+ record.partition());
        System.out.println("Offset: "+ record.offset());
    }

}
