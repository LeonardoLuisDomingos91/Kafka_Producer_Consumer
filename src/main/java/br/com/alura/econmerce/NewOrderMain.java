package br.com.alura.econmerce;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
      try(var dispatcher = new kafkaDispatcher()){
          for(var i = 0; i< 10; i++) {
              var key = UUID.randomUUID().toString();
              var value = key + "11111,1111,16";
              dispatcher.send("ECOMMERCE_NEW_ORDER", key, value);

              var email = "Thank you for your order! We are processing your order !";
              dispatcher.send("ECOMMERCE_SEND_EMAIL", key, value);
          }
      }
    }
}
