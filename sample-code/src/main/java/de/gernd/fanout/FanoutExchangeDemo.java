package de.gernd.fanout;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * demonstrates the functionality of a fanout exchange
 */
public class FanoutExchangeDemo {

    public static final String FANOUT_EXCHANGE_NAME = "fanout-exchange";

    public static void main(String[] args) throws IOException, TimeoutException {

        // set up sending connection to rabbitmq broker
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection rabbitMqConnection = factory.newConnection();

        // create fanout exchange
        Channel channel = rabbitMqConnection.createChannel();
        channel.exchangeDeclare(FANOUT_EXCHANGE_NAME, "fanout", true);

        // generate two auto deleted-queue and bind it to the fanout exchange
        String firstQueueName = channel.queueDeclare().getQueue();
        String secondQueueName = channel.queueDeclare().getQueue();

        // the routing key will be ignored for binding keys to a fanout
        channel.queueBind(firstQueueName, FANOUT_EXCHANGE_NAME, "");
        channel.queueBind(secondQueueName, FANOUT_EXCHANGE_NAME, "");

        // start sending random messages to the direct exchange
        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(new FanoutExchangeRandomMessageSender(channel),
                0, 5, TimeUnit.SECONDS);


        // create some consumers for both queues -> messages should be distributed to all
        // queues
        // as two consumers are registered to the same queue (the first one),
        // messages between these two consumers are load balanced
        Consumer firstMessageConsumer = new DefaultConsumer(channel) {

            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body)
                    throws IOException {
                String receivedMessage = new String(body, "UTF-8");
                System.out.println("First consumer for first queue got message: " + receivedMessage);
            }
        };
        channel.basicConsume(firstQueueName, true, firstMessageConsumer);

        Consumer secondMessageConsumer = new DefaultConsumer(channel) {

            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body)
                    throws IOException {
                String receivedMessage = new String(body, "UTF-8");
                System.out.println("Second consumer for first queue got message: " + receivedMessage);
            }
        };

        channel.basicConsume(firstQueueName, true, secondMessageConsumer);

        Consumer consumerForSecondQueue = new DefaultConsumer(channel) {

            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body)
                    throws IOException {
                String receivedMessage = new String(body, "UTF-8");
                System.out.println("Consumer for second queue got message: " + receivedMessage);
            }
        };

        channel.basicConsume(secondQueueName, true, consumerForSecondQueue);
    }
}
