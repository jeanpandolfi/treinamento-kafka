package br.com.jeanpandolfi;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;

public class FraudeDetectorService {

    public static void main(String[] args) {
        var fraudService = new FraudeDetectorService();
        /**O try seria para tentar receber e se caso ocorra alguma exeption ele fecha o porta de conexão*/
        try(var kafkaService = new KafkaService<Order>(
                FraudeDetectorService.class.getSimpleName(),
                "ECOMMERCE_NEW_ORDER", fraudService::parse,
                Order.class,
                new HashMap<>())){
            kafkaService.run();
        }

    }

    private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>();

    private void parse(ConsumerRecord<String, Order> record) throws ExecutionException, InterruptedException {
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

        var order = record.value();
        if(isFraud(order)){
            // pretending that the fraud happers when the amount id >= 4500
            System.out.println("Order is a fraud!!!!" + order);
            orderDispatcher.send("ECOMMERCE_ORDER_REJECTED", order.getEmail(), order);
        }else{
            System.out.println("Approved: " + order);
            orderDispatcher.send("ECOMMERCE_ORDER_APPROVED", order.getEmail(), order);
        }

    }

    private boolean isFraud(Order order) {
        return order.getAmount().compareTo(new BigDecimal("4500")) >= 0;
    }

}
