package de.gernd.directexchange;

import com.rabbitmq.client.Channel;

import java.io.IOException;
import java.util.Random;

/**
 * sends random messages to the direct exchange using the
 */
public class DirectExchangeRandomMessageSender implements Runnable {

    private final Channel channel;

    private final Random random = new Random();

    public DirectExchangeRandomMessageSender(Channel channel) {
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
            channel.basicPublish(DirectExchangeDemo.DIRECT_EXCHANGE_NAME, DirectExchangeDemo.ROUTING_KEY, null, message.getBytes());
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
