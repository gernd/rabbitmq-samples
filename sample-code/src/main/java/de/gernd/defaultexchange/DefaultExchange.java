package de.gernd.defaultexchange;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class DefaultExchange {

    public static final String QUEUE_NAME = "defaultExchangeQueue";

    public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
        System.out.println("Continously sending messages until ENTER is pressed");

        // set up sending connection to rabbitmq broker
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection rabbitMqConnection = factory.newConnection();

        // declare queue
        Channel channel = rabbitMqConnection.createChannel();
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);

        // start sending random messages
        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(new RandomMessageSender
                (channel), 0, 5, TimeUnit.SECONDS);

        // set up message consumption for test queue
        // messages for direct exchanges (as e.g. the default exchange) are load balanced
        // amongst consumers and not queues. if we had an additional consumer here,
        // the messages would be delivered in a round-robin manner
        Consumer messageConsumer = new DefaultConsumer(channel) {

            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body)
                    throws IOException {
                String receivedMessage = new String(body, "UTF-8");
                System.out.println("Got message: " + receivedMessage);
            }

        };
        channel.basicConsume(QUEUE_NAME, true, messageConsumer);

        // wait until user presses ENTER
        System.in.read();

        // close send channel and connection
        System.out.println("Shutting down");
        channel.close();
        rabbitMqConnection.close();
    }
}
