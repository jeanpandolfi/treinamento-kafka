package br.com.jeanpandolfi.treinamentokafka.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import java.util.Random;
import java.util.function.Consumer;
import java.util.function.Supplier;


@Configuration
public class FirstConfiguration {

    private static final String[] chaves = new String[]{"1", "2", "3", "4"};
    private static final Random RANDOM = new Random(System.currentTimeMillis());
    private static final Logger logger =
            LoggerFactory.getLogger(FirstConfiguration.class);

    @Bean
    public Consumer<Message<String>> listenFirst() {
        return mensagem -> logger.info(
                String.format("%s [%s]",
                        mensagem.getPayload(),
                        mensagem.getHeaders().get(KafkaHeaders.RECEIVED_PARTITION_ID)));
    }
    @Bean
    public Supplier<Message<String>> sendFirst() {
        return () -> {
            String chave = chaves[RANDOM.nextInt(chaves.length)];
            logger.info(String.format("Enviando %s", chave));
            return MessageBuilder
                    .withPayload(String.format("Message from Spring [%s]", chave))
                    .setHeader("partitionKey", chave)
                    .build();
        };
    }
}
