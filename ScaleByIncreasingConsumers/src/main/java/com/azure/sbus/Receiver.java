package com.azure.sbus;

import com.azure.messaging.servicebus.*;
import reactor.core.Disposable;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/*
   This class is used to demonstrate how to scale out the processing of multiple messages from the Service Bus topics
   by increasing the number of consumers/receivers.

   If your business use case requires ordered processing of messages then that is something you would need to manage
   as part of your business logic

   You can further optimize it by batching the message during the retrieval or using pre-fetching technique

 */
public class Receiver {
    //
    private ServiceBusReceiverAsyncClient client;
    //
    private Disposable disposable;
    //
    int THREAD_COUNT = 20;
    //
    String CONNECTION_STRING = "connection string for the service bus";
    //
    String TOPIC_NAME = "topic name for the service bus";
    //
    String SUBSCRIPTION_NAME = "subscription name for the topic";

    public static void main(String... args) {
        Receiver receiver = new Receiver();
        receiver.initClient();
    }

    public void initClient() {

        // Runnable thread which would invoke method that would create new service bus client and start
        // processing the messages
        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                System.out.println("ThreadName "+Thread.currentThread().getName());
                try {
                    receiveMessages();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        };
        // Create executor service with the defined number of threads and spawn new thread in loop
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        for(int i = 1; i<= THREAD_COUNT; i++) {
            executor.execute(runnable);
        }
        while (!executor.isTerminated()) {
            // empty body
        }

    }

    // handles received messages
    private void receiveMessages() throws InterruptedException
    {
        // Create an instance of the processor through the ServiceBusClientBuilder
        ServiceBusProcessorClient processorClient = new ServiceBusClientBuilder()
                .connectionString(CONNECTION_STRING)
                .processor()
                .topicName(TOPIC_NAME)
                .subscriptionName(SUBSCRIPTION_NAME)
                .processMessage(Receiver::processMessage)
                .processError(context -> processError(context))
                .buildProcessorClient();

        System.out.println("Starting the processor");
        processorClient.start();
    }

    // handle error messages
    private void processError(ServiceBusErrorContext context) {
        System.out.printf("Error when receiving messages from namespace: '%s'. Entity: '%s'%n",
                context.getFullyQualifiedNamespace(), context.getEntityPath());

        if (!(context.getException() instanceof ServiceBusException)) {
            System.out.printf("Non-ServiceBusException occurred: %s%n", context.getException());
            return;
        }

        ServiceBusException exception = (ServiceBusException) context.getException();
        ServiceBusFailureReason reason = exception.getReason();
        System.out.println("reason "+ reason);
    }

    private static void processMessage(ServiceBusReceivedMessageContext context) {
        ServiceBusReceivedMessage message = context.getMessage();
        System.out.printf("Processing message. Thread: %s Session: %s, Sequence #: %s. Contents: %s%n",
                Thread.currentThread().getName(),
                message.getMessageId(),
                message.getSequenceNumber(),
                message.getBody());
    }

}
