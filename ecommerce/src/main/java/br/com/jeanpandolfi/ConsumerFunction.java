package br.com.jeanpandolfi;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface ConsumerFunction {
     void consume(ConsumerRecord<String, String> record);
}
