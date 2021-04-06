package br.com.jeanpandolfi;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class EmailService {

    public static void main(String[] args) {
        var emailService = new EmailService();
        /**O try seria para tentar receber e se caso ocorra alguma exeption ele fecha o porta de conexão*/
        try(var kafkaService = new KafkaService(
                EmailService.class.getSimpleName(),
                "ECOMMERCE_SEND_EMAIL",
                emailService::parse,
                String.class)){
            kafkaService.run();
        }

    }

    private void parse(ConsumerRecord<String, String> record){
        System.out.println("-----------------------------");
        System.out.println("Send email");
        System.out.println("Key: "+record.key());
        System.out.println("Value: "+record.value());
        System.out.println("Partition: "+record.partition());
        System.out.println("Offset: "+record.offset());
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("Email sent");
    }
}
