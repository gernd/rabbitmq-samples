package de.gernd.directexchange;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class DirectExchangeDemo {


    public static final String DIRECT_EXCHANGE_NAME = "my-direct-exchange";
    public static final String ROUTING_KEY = "simpleroute";

    /**
     * continuously sends messages using a direct exchange to a single queue
     * several consumers are listening on the same queue, so that messages are distributed via round robin
     */
    public static void main(String[] args) throws IOException, TimeoutException {
        // set up sending connection to rabbitmq broker
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection rabbitMqConnection = factory.newConnection();

        // create direct exchange
        Channel channel = rabbitMqConnection.createChannel();
        channel.exchangeDeclare(DIRECT_EXCHANGE_NAME, "direct", true);

        // generate auto deleted-queue and bind it to the direct exchange
        String queueName = channel.queueDeclare().getQueue();
        channel.queueBind(queueName, DIRECT_EXCHANGE_NAME, ROUTING_KEY);

        // start sending random messages to the direct exchange
        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(new DirectExchangeRandomMessageSender(channel),
                0, 5, TimeUnit.SECONDS);

        // create 2 consumers for the generated queue -> messages are distributed using
        // round robin
        Consumer firstMessageConsumer = new DefaultConsumer(channel) {

            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body)
                    throws IOException {
                String receivedMessage = new String(body, "UTF-8");
                System.out.println("First consumer got message: " + receivedMessage);
            }
        };

        channel.basicConsume(queueName, true, firstMessageConsumer);

        Consumer secondMessageConsumer = new DefaultConsumer(channel) {

            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body)
                    throws IOException {
                String receivedMessage = new String(body, "UTF-8");
                System.out.println("Second consumer got message: " + receivedMessage);
            }
        };

        channel.basicConsume(queueName, true, secondMessageConsumer);
    }

}
