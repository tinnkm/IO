package com.tinnkm.kafka.server;

import com.tinnkm.kafka.network.RequestChannel;

import java.util.concurrent.CountDownLatch;

public class KafkaRequestHandler implements Runnable{
    private RequestChannel requestChannel;
    private CountDownLatch shutdownComplete = new CountDownLatch(1);
    public KafkaRequestHandler(RequestChannel requestChannel){
        this.requestChannel = requestChannel;
    }

    @Override
    public void run() {

    }

    void initiateShutdown() throws InterruptedException {
        requestChannel.sendShutdownRequest();
    }

    void awaitShutdown() throws InterruptedException {
        shutdownComplete.await();
    }
}
