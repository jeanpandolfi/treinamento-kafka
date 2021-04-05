package br.com.jeanpandolfi;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        /**O try seria para tentar enviar e se caso ocorra alguma exeption ele fecha o porta de conexão*/
        try(var dispatcher = new KafkaDispatcher()) {
            for (var i = 0; i < 10; i++) {
                var key = UUID.randomUUID().toString();
                var value = "Jean Pandolfi";
                dispatcher.send("ECOMMERCE_NEW_ORDER", key, value);

                var email = "Thank you for your order! We are processing your order!";
                dispatcher.send("ECOMMERCE_SEND_EMAIL", key, email);
            }
        }
    }



}
