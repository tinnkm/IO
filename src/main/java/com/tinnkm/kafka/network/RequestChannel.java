package com.tinnkm.kafka.network;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

public class RequestChannel {
    private ArrayBlockingQueue<BaseRequest> requestQueue = new ArrayBlockingQueue<>(500);
    private ConcurrentHashMap<Integer, SocketServer.Processor> processors = new ConcurrentHashMap<Integer, SocketServer.Processor>();

    public void addProcessor(SocketServer.Processor processor) {
        processors.putIfAbsent(processor.id, processor);
    }

    public void clear() {
        this.requestQueue.clear();;
    }

    public void sendShutdownRequest() throws InterruptedException {
        requestQueue.put(new ShutdownRequest());
    }

    public void shutdown() {
        clear();
    }

    private interface BaseRequest{}
    private static class ShutdownRequest implements BaseRequest{}
}
