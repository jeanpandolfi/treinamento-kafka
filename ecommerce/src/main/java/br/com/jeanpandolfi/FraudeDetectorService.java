package br.com.jeanpandolfi;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class FraudeDetectorService {

    public static void main(String[] args) {
        var fraudService = new FraudeDetectorService();
        /**O try seria para tentar receber e se caso ocorra alguma exeption ele fecha o porta de conexão*/
        try(var kafkaService = new KafkaService(
                FraudeDetectorService.class.getSimpleName(),
                "ECOMMERCE_NEW_ORDER", fraudService::parse)){
            kafkaService.run();
        }

    }

    private void parse(ConsumerRecord<String, String> record) {
        System.out.println("-----------------------------");
        System.out.println("Processing new order, checking for fraud");
        System.out.println("Key: "+record.key());
        System.out.println("Value: "+record.value());
        System.out.println("Partition: "+record.partition());
        System.out.println("Offset: "+record.offset());
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("Order processed");
    }

}
