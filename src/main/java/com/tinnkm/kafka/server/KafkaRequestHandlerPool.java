package com.tinnkm.kafka.server;

import com.tinnkm.kafka.network.RequestChannel;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class KafkaRequestHandlerPool {
    private AtomicInteger threadPoolSize = new AtomicInteger(3);
    private List<KafkaRequestHandler> runnables = new ArrayList<>(8);

    public KafkaRequestHandlerPool(RequestChannel dataPlaneRequestChannel, KafkaApis dataPlaneRequestProcessor) {

    }

    public void shutdown() throws InterruptedException {
        for (KafkaRequestHandler handler : runnables) {
            handler.initiateShutdown();
            handler.awaitShutdown();
        }
    }
}
