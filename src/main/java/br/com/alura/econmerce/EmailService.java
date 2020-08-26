package br.com.alura.econmerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class EmailService {
    public static void main(String[] args) {
        var emailService = new EmailService();
        var service = new KafkaService(EmailService.class.getSimpleName(),"ECOMMERCE_SEND_EMAIL",emailService::parse);

        service.run();
    }

    private void parse(ConsumerRecord<String,String> record){
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
