package de.gernd.defaultexchange;

import com.rabbitmq.client.Channel;

import java.io.IOException;
import java.util.Random;

import static de.gernd.defaultexchange.DefaultExchange.QUEUE_NAME;

public class DefaultExchangeRandomMessageSender implements Runnable {

    private final Channel channel;

    private final Random random = new Random();

    public DefaultExchangeRandomMessageSender(Channel channel) {
        this.channel = channel;
    }

    public void run() {
        Long randomVal = random.nextLong();
        final String message = new StringBuilder()
                .append("Message[")
                .append(randomVal)
                .append("]")
                .toString();
        System.out.println("Sending message " + message);
        try {
            // send message
            // the message is sent using the broker with no exchange specified
            // -> default exchange is used
            // the routing key is the name of the queue that has been created before
            // the default queue has a special poperty: every queue, that is created,
            // is automatically bound to the default exchange with its name as the routing
            // key.
            channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
