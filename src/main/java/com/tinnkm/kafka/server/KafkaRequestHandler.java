package com.tinnkm.kafka.server;

import com.tinnkm.kafka.common.utils.Time;
import com.tinnkm.kafka.network.RequestChannel;

import java.util.concurrent.CountDownLatch;

public class KafkaRequestHandler implements Runnable{
    private RequestChannel requestChannel;
    private CountDownLatch shutdownComplete = new CountDownLatch(1);
    private volatile  boolean stopped = false;
    private final Time time;
    private final ApiRequestHandler apis;
    public KafkaRequestHandler(int id,int brokerId,RequestChannel requestChannel,ApiRequestHandler apis , Time time ){
        this.requestChannel = requestChannel;
        this.time = time;
        this.apis = apis;
    }

    @Override
    public void run() {
        while (!stopped) {
            // We use a single meter for aggregate idle percentage for the thread pool.
            // Since meter is calculated as total_recorded_value / time_window and
            // time_window is independent of the number of threads, each recorded idle
            // time should be discounted by # threads.
            long startSelectTime = time.nanoseconds();

            RequestChannel.BaseRequest req = null;
            try {
                // 拉取一个请求
                req = requestChannel.receiveRequest(Long.valueOf(300));
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            long endTime = time.nanoseconds();
            long idleTime = endTime - startSelectTime;
            switch (req){
                case  RequestChannel.ShutdownRequest r -> {
                    completeShutdown();
                }
                case RequestChannel.Request request ->{
                    try {
                        request.requestDequeueTimeNanos = endTime;
                        apis.handle(request);
                    } catch(Exception e) {
                        completeShutdown();
                    } finally {
                        request.releaseBuffer();
                    }
                }

                default -> throw new IllegalStateException("Unexpected value: " + req);
            }

        }
        completeShutdown();
    }

    private void completeShutdown(){
        shutdownComplete.countDown();
    }

    void initiateShutdown() throws InterruptedException {
        requestChannel.sendShutdownRequest();
    }

    void awaitShutdown() throws InterruptedException {
        shutdownComplete.await();
    }

    interface ApiRequestHandler{
        void handle(RequestChannel.Request request) throws InterruptedException;
    }
}
