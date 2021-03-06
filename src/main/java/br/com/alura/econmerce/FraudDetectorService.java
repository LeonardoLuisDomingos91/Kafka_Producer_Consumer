package br.com.alura.econmerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.HashMap;

public class FraudDetectorService {
    public static void main(String[] args) {
        var fraudService = new FraudDetectorService();
        try(var service = new KafkaService<>(FraudDetectorService.class.getSimpleName(),
                "ECOMMERCE_NEW_ORDER",
                fraudService::parse,
                Order.class,
                new HashMap<>())){

            service.run();
        }
    }

    private void parse(ConsumerRecord<String, Order> record){
        System.out.println("--------------------------------------------");
        System.out.println("Processando nova ordem, checando se é fraude");
        System.out.println("KEY: "+record.key());
        System.out.println("Value: "+record.value());
        System.out.println("partition: "+record.partition());
        System.out.println("offset: "+ record.offset());
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            //ignora estamos fingindo que estamos processando
            e.printStackTrace();
        }
        System.out.println("Ordem processada");
    }
}
