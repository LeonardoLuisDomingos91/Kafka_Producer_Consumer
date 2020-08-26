package br.com.alura.econmerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface ConsumerFunction {

    void consume(ConsumerRecord<String,String>record);
}
