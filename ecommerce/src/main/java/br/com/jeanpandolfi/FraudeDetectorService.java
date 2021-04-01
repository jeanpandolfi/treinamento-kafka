package br.com.jeanpandolfi;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

public class FraudeDetectorService {

    public static void main(String[] args) {
        var consumer = new KafkaConsumer<String, String>(properties());
        /**Passa uma lista de topicos que esse consumidor irá escutar. Por boas práticas um consumidor só escuta um tópico*/
        consumer.subscribe(Collections.singletonList("ECOMMERCE_NEW_ORDER"));
        while(true){
            var records = consumer.poll(Duration.ofMillis(100));
            if(!records.isEmpty()){
                System.out.println("Encontrei " + records.count() + " registros");
                for (ConsumerRecord<String, String> record : records) {
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

        }

    }

    private static Properties properties() {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        /** @param GROUP_ID_CONFIG é necessário para definir em qual grupo de consumidores está o serviço
         * Isso é necessário para que um grupo receba todas as mensagens de um tópico. Caso um Grupo tenha mais
         * de um serviço as mensagens são distribuídas */
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, FraudeDetectorService.class.getSimpleName());
        /**define um ID para o servico-cosumer */
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, FraudeDetectorService.class.getSimpleName() + UUID.randomUUID().toString());
        /**define que comits de mensagens processadas devem ser de um em um. Recebi uma mensagem processei commitei.
         * Isso é necessário para evitar que durante um processamento de mensagens haja um balanceamento e vc
         * tenha que processar novamente as mensagens já processadas pois não foi commitada.*/
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        return properties;
    }
}
