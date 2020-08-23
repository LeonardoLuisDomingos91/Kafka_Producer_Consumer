package br.com.alura.econmerce;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class EmailService {
    public static void main(String[] args) {
        var consumer = new KafkaConsumer<String, String>(properties());
        consumer.subscribe(Collections.singletonList("ECOMMERCE_SEND_EMAIL")); // consumindo a lista do topico
        while (true) {
            var records = consumer.poll(Duration.ofMillis(100)); // perguntando se tem registro nesse tempo determinado e pegando os registro retornados
            if (!records.isEmpty()) {
//                System.out.println("Não encontrei" + records.count() + " regristo");
                for (var record : records) {
                    System.out.println("--------------------------------------------");
                    System.out.println("Send email, checking for  fraud");
                    System.out.println("KEY: "+record.key());
                    System.out.println("Value: "+record.value());
                    System.out.println("partition: "+record.partition());
                    System.out.println("offset: "+ record.offset());
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        //ignora estamos fingindo que estamos processando
                        e.printStackTrace();
                    }
                    System.out.println("Email Send");
                }
            }
        }
    }

    private static Properties properties() {
        var properties = new Properties(); // criando propriedade para retornar no consumidar
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092"); // passando o localhost e porta onde o kafka esta rodando
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()); // deserializando a chave de bites em string
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());// deserializando o valor de bites em string
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, EmailService.class.getSimpleName()); // criando um grupo necessario para esse consumidar fazer parte
        return properties;
    }
}
