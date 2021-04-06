package br.com.jeanpandolfi;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.regex.Pattern;

public class KafkaService<T> implements Closeable {
    private final KafkaConsumer<String, T> consumer;
    private ConsumerFunction parse;

    public KafkaService(String groupId, String topic, ConsumerFunction parse, Class<T> typeClass) {
        this(groupId, parse, typeClass);
        /**Passa uma lista de topicos que esse consumidor irá escutar. Por boas práticas um consumidor só escuta um tópico*/
        consumer.subscribe(Collections.singletonList(topic));
    }

    public KafkaService(String groupId, Pattern pattern, ConsumerFunction parse, Class<T> typeClass) {
        this(groupId, parse, typeClass);
        /**Passa uma lista de topicos que esse consumidor irá escutar. Por boas práticas um consumidor só escuta um tópico*/
        consumer.subscribe(pattern);
    }

    private KafkaService(String groupId, ConsumerFunction parse, Class<T> typeClass) {
        this.parse = parse;
        this.consumer = new KafkaConsumer<String, T>(properties(typeClass, groupId));
    }

    public void run() {
        while(true){
            var records = consumer.poll(Duration.ofMillis(100));
            if(!records.isEmpty()){
                System.out.println("Encontrei " + records.count() + " registros");
                for (ConsumerRecord<String, T> record : records) {
                    this.parse.consume(record);
                }
            }

        }
    }

    private Properties properties(Class<T> typeClass, String groupId) {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());
        /** @param GROUP_ID_CONFIG é necessário para definir em qual grupo de consumidores está o serviço
         * Isso é necessário para que um grupo receba todas as mensagens de um tópico. Caso um Grupo tenha mais
         * de um serviço as mensagens são distribuídas */
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        /**define um ID para o servico-cosumer */
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        /**define que comits de mensagens processadas devem ser de um em um. Recebi uma mensagem processei commitei.
         * Isso é necessário para evitar que durante um processamento de mensagens haja um balanceamento e vc
         * tenha que processar novamente as mensagens já processadas pois não foi commitada.*/
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");

        properties.setProperty(GsonDeserializer.TYPE_CONFIG, typeClass.getName());
        return properties;
    }

    @Override
    public void close(){
        consumer.close();
    }
}
