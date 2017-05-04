package de.gernd.fanout;

import com.rabbitmq.client.Channel;

import java.io.IOException;
import java.util.Random;

/**
 * Sends random messages to the Fanout Exchange
 */
public class FanoutExchangeRandomMessageSender implements Runnable {

    private final Channel channel;
    private final Random random = new Random();

    public FanoutExchangeRandomMessageSender(Channel channel) {
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
            channel.basicPublish(FanoutExchangeDemo.FANOUT_EXCHANGE_NAME, "", null, message.getBytes());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
