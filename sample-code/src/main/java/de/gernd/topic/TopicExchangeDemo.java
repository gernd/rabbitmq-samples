package de.gernd.topic;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Demonstrates the usage of a topic exchange
 */
public class TopicExchangeDemo {

    public static final String TOPIC_EXCHANGE_NAME = "test-topic-exchange";

    public static void main(String[] args) throws IOException, TimeoutException {

        // set up connection to rabbitmq broker
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection rabbitMqConnection = factory.newConnection();

        // create topic exchange
        Channel channel = rabbitMqConnection.createChannel();
        channel.exchangeDeclare(TOPIC_EXCHANGE_NAME, "topic", true);

        // generate two auto-deleted queues and bind it to the topic exchange
        String allSecurityQueueName = channel.queueDeclare().getQueue();
        String securityInfoQueueName = channel.queueDeclare().getQueue();

        // our routes have the format "<component>.<subcomponent>.<loglevel>, e.g.
        // security.authentication.info or render.compression.error
        // bind queue to all events that start with security
        channel.queueBind(allSecurityQueueName, TOPIC_EXCHANGE_NAME, "security.#");
        // bind queue to all security events with log level "info"
        channel.queueBind(securityInfoQueueName, TOPIC_EXCHANGE_NAME, "security.*.info");

        // start sending random messages to the fanout exchange
        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(new TopicExchangeRandomMessageSender(channel),
                0, 5, TimeUnit.SECONDS);

        // bind consumers to queues
        Consumer allSecurityMessageConsumer = new DefaultConsumer(channel) {

            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body)
                    throws IOException {
                String receivedMessage = new String(body, "UTF-8");
                System.out.println("Consumer for security.# got " + receivedMessage);
            }
        };
        channel.basicConsume(allSecurityQueueName, true, allSecurityMessageConsumer);

        Consumer securityInfoMessageConsumer = new DefaultConsumer(channel) {

            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body)
                    throws IOException {
                String receivedMessage = new String(body, "UTF-8");
                System.out.println("Consumer for security.*.info got " + receivedMessage);
            }
        };
        channel.basicConsume(securityInfoQueueName, true, securityInfoMessageConsumer);
    }
}
