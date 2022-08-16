package com.tinnkm.kafka.server;

import com.tinnkm.kafka.common.utils.Time;
import com.tinnkm.kafka.network.RequestChannel;
import com.tinnkm.kafka.network.SocketServer;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaServer {
    private AtomicBoolean startupComplete = new AtomicBoolean(false);
    private AtomicBoolean isShuttingDown = new AtomicBoolean(false);
    private AtomicBoolean isStartingUp = new AtomicBoolean(false);
    private  CountDownLatch shutdownLatch = new CountDownLatch(1);
    private SocketServer socketServer = null;
    private KafkaApis dataPlaneRequestProcessor = null;
    private KafkaRequestHandlerPool dataPlaneRequestHandlerPool = null;
    private Time time = Time.SYSTEM;


    public void startup() throws InterruptedException {
        try {
            if (isShuttingDown.get())
                throw new IllegalStateException("Kafka server is still shutting down, cannot re-start!");

            if (startupComplete.get())
                return;
            boolean canStartup = isStartingUp.compareAndSet(false, true);
            if (canStartup){
                this.socketServer = new SocketServer(time);
                socketServer.startup(false);
                this.dataPlaneRequestProcessor = createKafkaApis(this.socketServer.dataPlaneRequestChannel);
                this.dataPlaneRequestHandlerPool = new KafkaRequestHandlerPool(-1,time,this.socketServer.dataPlaneRequestChannel,this.dataPlaneRequestProcessor);
                this.socketServer.startProcessingRequests();
                shutdownLatch = new CountDownLatch(1);
                startupComplete.set(true);
                isStartingUp.set(false);
            }
        } catch (Throwable e) {
           isStartingUp.set(false);
           shutdown();
           throw e;
        }
    }

    public void shutdown() throws InterruptedException {
        try {
            if (isStartingUp.get())
                throw new IllegalStateException("Kafka server is still starting up, cannot shut down!");
            if (shutdownLatch.getCount() > 0 && isShuttingDown.compareAndSet(false, true)) {


                // Stop socket server to stop accepting any more connections and requests.
                // Socket server will be shutdown towards the end of the sequence.
                if (socketServer != null)
                    socketServer.stopProcessingRequests();
                if (dataPlaneRequestHandlerPool != null)
                   dataPlaneRequestHandlerPool.shutdown();



                if (socketServer != null)
                   socketServer.shutdown();

                startupComplete.set(false);
                isShuttingDown.set(false);
                shutdownLatch.countDown();
            }
        } catch (Throwable e) {
            isShuttingDown.set(false);
            throw e;
        }
    }

    private KafkaApis createKafkaApis(RequestChannel requestChannel){
        return new KafkaApis(requestChannel);
    }

    public void awaitShutdown() throws InterruptedException {
        shutdownLatch.await();
    }
}
