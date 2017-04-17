import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class DefaultExchange {

    private static final String QUEUE_NAME = "defaultExchangeQueue";

    public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
        // set up sending connection to rabbitmq broker
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection rabbitMqConnection = factory.newConnection();

        // declare queue
        Channel sendChannel = rabbitMqConnection.createChannel();
        sendChannel.queueDeclare(QUEUE_NAME, false, false, false, null);

        // send message
        // the message is sent using the broker with no exchange specified
        // -> default exchange is used
        // the routing key is the name of the queue that has been created before
        // the default queue has a special poperty: every queue, that is created,
        // is automatically bound to the default exchange with its name as the routing
        // key.
        final String myMessage = "this is a test message";
        sendChannel.basicPublish("", QUEUE_NAME, null, myMessage.getBytes());
        System.out.println("Send message:" + myMessage);

        // consume message
        Consumer messageConsumer = new DefaultConsumer(sendChannel) {

            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body)
                    throws IOException {
                String receivedMessage = new String(body, "UTF-8");
                System.out.println("Got message: " + receivedMessage);
            }

        };
        sendChannel.basicConsume(QUEUE_NAME, true, messageConsumer);

        // wait some time to make sure message has been received
        Thread.sleep(1000L);

        // close send channel and connection
        System.out.println("Shutting down");
        sendChannel.close();
        rabbitMqConnection.close();
    }
}
