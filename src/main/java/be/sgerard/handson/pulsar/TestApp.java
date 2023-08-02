package be.sgerard.handson.pulsar;

import org.apache.pulsar.client.api.*;

public class TestApp {

    public static void main(String[] args) throws InterruptedException {
        final String topic = "my-string-topic";

        final Thread sendingThread = Thread.ofVirtual().start(() -> sendMessages(topic));
        final Thread receivingThread = Thread.ofVirtual().start(() -> receiveMessages(topic));

        sendingThread.join();
        receivingThread.join();
    }

    @SuppressWarnings("SameParameterValue")
    private static void sendMessages(String topic) {
        try (PulsarClient client = initClient(); Producer<String> producer = createProducer(client, topic)) {
            System.out.println("Start sending messages");

            for (int i = 0; i < 100; i++) {
                producer.send("My message %s".formatted(i));
            }
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("SameParameterValue")
    private static void receiveMessages(String topic) {
        final String subscriptionName = "my-subscription";

        try (PulsarClient client = initClient(); Consumer<String> consumer = initSubscription(topic, subscriptionName, client)) {
            System.out.println("Start receiving message");

            while (true) {
                // Wait for a message
                Message<String> msg = consumer.receive();

                try {
                    // Do something with the message
                    System.out.println("Message received: " + msg.getValue());

                    // Acknowledge the message
                    consumer.acknowledge(msg);
                } catch (Exception e) {
                    // Message failed to process, redeliver later
                    consumer.negativeAcknowledge(msg);
                }
            }
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("SameParameterValue")
    private static Consumer<String> initSubscription(String topic, String subscriptionName, PulsarClient client) throws PulsarClientException {
        return client.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName(subscriptionName)
                .subscribe();
    }

    private static Producer<String> createProducer(PulsarClient client, String topic) throws PulsarClientException {
        return client.newProducer(Schema.STRING)
                .topic(topic)
                .create();
    }

    private static PulsarClient initClient() {
        try {
            return PulsarClient.builder()
                    .serviceUrl("pulsar://localhost:6650")
                    .build();
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
    }
}
