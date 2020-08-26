package br.com.alura.econmerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class FraudDetectorService {
    public static void main(String[] args) {
        var fraudService = new FraudDetectorService();
        var service = new KafkaService(FraudDetectorService.class.getSimpleName(), "ECOMMERCE_NEW_ORDER", fraudService::parse);

        service.run();
    }

    private void parse(ConsumerRecord<String, String> record){
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
