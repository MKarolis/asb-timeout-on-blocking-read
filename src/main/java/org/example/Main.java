package org.example;

import com.azure.core.amqp.AmqpRetryOptions;
import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusMessage;
import com.azure.messaging.servicebus.ServiceBusSenderClient;
import com.azure.messaging.servicebus.models.ServiceBusReceiveMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class Main {

    private static final String CONNECTION_STRING = Optional.ofNullable(System.getenv("CONNECTION_STRING"))
            .orElseThrow(() -> new IllegalArgumentException("CONNECTION_STRING is required"));
    private static final String QUEUE_NAME = Optional.ofNullable(System.getenv("QUEUE_NAME"))
            .orElseThrow(() -> new IllegalArgumentException("QUEUE_NAME is required"));

    private static final Random random = new Random();
    private static final Logger log = LoggerFactory.getLogger(Main.class);

    private static final boolean V2_ENABLED = Optional.ofNullable(System.getenv("V2_ENABLED"))
            .map(Boolean::parseBoolean)
            .orElse(false);
    private static final double SEND_LOG_PROBABILITY = Optional.ofNullable(System.getenv("SEND_LOG_PROBABILITY"))
            .map(Double::parseDouble)
            .orElse(0.05);
    private static final double RECEIVE_LOG_PROBABILITY = Optional.ofNullable(System.getenv("RECEIVE_LOG_PROBABILITY"))
            .map(Double::parseDouble)
            .orElse(0.05);
    private static final int TTL_MINUTES = Optional.ofNullable(System.getenv("TTL_MINUTES"))
            .map(Integer::parseInt)
            .orElse(1);

    private static boolean IS_IN_ERROR_LOOP = false;

    private static String randomString(int length) {
        int leftLimit = 48; // numeral '0'
        int rightLimit = 122; // letter 'z'

        return random.ints(leftLimit, rightLimit + 1)
                .filter(i -> (i <= 57 || i >= 65) && (i <= 90 || i >= 97))
                .limit(length)
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                .toString();
    }

    private static ServiceBusSenderClient buildClient() {
        return new ServiceBusClientBuilder()
                .fullyQualifiedNamespace("asb-debug.servicebus.windows.net")
                .retryOptions(new AmqpRetryOptions()
                        .setTryTimeout(Duration.ofSeconds(5))
                        .setMaxDelay(Duration.ofSeconds(10))
                        .setMaxRetries(2))
                .connectionString(CONNECTION_STRING)
                .sender()
                .queueName(QUEUE_NAME)
                .buildClient();
    }

    private static void schedulePublishing() {
        for (int i = 0; i < 5; i++) {
            final AtomicReference<ServiceBusSenderClient> senderClient = new AtomicReference<>(buildClient());

            ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
            executorService.scheduleWithFixedDelay(() -> {
                boolean shouldLog = Math.random() < SEND_LOG_PROBABILITY;

                if(shouldLog) {
                    System.out.println("Sending message at " + LocalDateTime.now() + ". " + senderClient.get().getIdentifier());
                }

                long start = System.currentTimeMillis();

                try {
                    senderClient.get().sendMessage(new ServiceBusMessage(randomString(131072)));
                    long end = System.currentTimeMillis();
                    if (Math.random() < SEND_LOG_PROBABILITY) {
                        System.out.println(LocalDateTime.now() + " | Message sent in " + (end - start) + "ms. " + senderClient.get().getIdentifier());
                    }
                } catch (Exception e) {
                    log.error("Failed to send message", e);

                    if (e.getMessage().contains("Timeout on blocking read") && !IS_IN_ERROR_LOOP) {
                        System.out.println("Blocking read error encountered with " + senderClient.get().getIdentifier() + ". No more restarts.");
                        IS_IN_ERROR_LOOP = true;
                    }

                    senderClient.get().close();
                    senderClient.set(buildClient());
                }
            }, i, 1, TimeUnit.SECONDS);
        }
    }

    private static void initListening() {
        for (int i = 0; i < 5; i++) {
            new ServiceBusClientBuilder()
                    .fullyQualifiedNamespace("asb-debug.servicebus.windows.net")
                    .retryOptions(new AmqpRetryOptions()
                            .setTryTimeout(Duration.ofSeconds(5))
                            .setMaxDelay(Duration.ofSeconds(10))
                            .setMaxRetries(2))
                    .connectionString(CONNECTION_STRING)
                    .processor()
                    .receiveMode(ServiceBusReceiveMode.PEEK_LOCK)
                    .disableAutoComplete()
                    .queueName(QUEUE_NAME)
                    .processMessage((m) -> {
                        if (Math.random() < RECEIVE_LOG_PROBABILITY) {
                            System.out.println("Received message: " + m.getMessage().getMessageId() + " at " + LocalDateTime.now());
                        }
                    })
                    .processError((e) -> {
                        System.out.println("Error occurred: " + e.getException().getMessage());
                    })
                    .buildProcessorClient()
                    .start();
        }
    }

    private static void initProperties() {
        if (!V2_ENABLED) {
            System.getProperties().put("com.azure.messaging.servicebus.sendAndManageRules.v2", "false");
        }

    }

    private static void scheduleDeath() {
        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        executorService.schedule(() -> {
            if (!IS_IN_ERROR_LOOP) {
                System.out.println("Scheduled exit. Godbyeeeeeeeeeeeeeee...");
                System.exit(1);
            }
        }, TTL_MINUTES, TimeUnit.MINUTES);

    }

    public static void main(String[] args) {
        System.out.println("Starting the application. Logging messages with probability " + SEND_LOG_PROBABILITY + "...");
        initProperties();

        schedulePublishing();
        initListening();

        scheduleDeath();
    }
}