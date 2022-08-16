package com.tinnkm.kafka.server;

import com.tinnkm.kafka.common.utils.KafkaThread;
import com.tinnkm.kafka.common.utils.Time;
import com.tinnkm.kafka.network.RequestChannel;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class KafkaRequestHandlerPool {
    private List<KafkaRequestHandler> runnables = new ArrayList<>(8);
    private int brokerId;
    private Time time;
    private RequestChannel requestChannel;
    private KafkaRequestHandler.ApiRequestHandler apis;

    public KafkaRequestHandlerPool(int brokerId, Time time, RequestChannel requestChannel, KafkaRequestHandler.ApiRequestHandler apis) {
        this.brokerId = brokerId;
        this.time = time;
        this.requestChannel = requestChannel;
        this.apis = apis;
        for (int i = 0; i < 8; i++) {
            createHandler(i);
        }
    }


    synchronized void createHandler(int id) {
        runnables.add(new KafkaRequestHandler(id, brokerId, requestChannel, apis, time));
        KafkaThread.daemon("kafka-request-handler-" + id, runnables.get(id)).start();
    }
    public void shutdown() throws InterruptedException {
        for (KafkaRequestHandler handler : runnables) {
            handler.initiateShutdown();
            handler.awaitShutdown();
        }
    }
}
