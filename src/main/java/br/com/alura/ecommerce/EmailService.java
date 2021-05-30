package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;


public class EmailService {

    public static void main(String[] args) {
        var emailService = new EmailService();
        var service = new KafkaService(emailService.getClass().getSimpleName(), "ECOMMERCE_SEND_EMAIL", emailService::parse);
        service.run();
    }

    private void parse(ConsumerRecord<String, String> record) {
        System.out.println("-------------------------------");
        System.out.println("Send email");
        System.out.println(record.value());
        try {
            Thread.sleep(1000);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("Email sent");
    }
}
