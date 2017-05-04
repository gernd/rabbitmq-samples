package de.gernd.topic;

import com.rabbitmq.client.Channel;

import java.io.IOException;

/**
 * sends random messages with various routing keys to demonstrate the usage
 * of a topic exchange
 */
public class TopicExchangeRandomMessageSender implements Runnable {

    private final Channel channel;

    public TopicExchangeRandomMessageSender(Channel channel) {
        this.channel = channel;
    }

    public void run() {
        try {
            final String firstMessage = "this is a security.authentication.info message";
            System.out.println("Sending " + firstMessage);
            channel.basicPublish(TopicExchangeDemo.TOPIC_EXCHANGE_NAME, "security.authentication.info", null, firstMessage.getBytes());

            final String secondMessage = "this is a security.acl.debug message";
            System.out.println("Sending " + secondMessage);
            channel.basicPublish(TopicExchangeDemo.TOPIC_EXCHANGE_NAME, "security.acl.debug", null, secondMessage.getBytes());

            final String thirdMessage = "this is a video.compression.info message";
            System.out.println("Sending " + thirdMessage);
            channel.basicPublish(TopicExchangeDemo.TOPIC_EXCHANGE_NAME, "video.compression.info", null, thirdMessage.getBytes());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
