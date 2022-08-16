package com.tinnkm.kafka.network;

import com.tinnkm.kafka.common.network.Send;
import com.tinnkm.kafka.common.utils.Time;
import com.tinnkm.kafka.memory.MemoryPool;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class RequestChannel {
    private ArrayBlockingQueue<BaseRequest> requestQueue = new ArrayBlockingQueue<>(500);
    private ConcurrentHashMap<Integer, SocketServer.Processor> processors = new ConcurrentHashMap<Integer, SocketServer.Processor>();

    private final Time time;
    public RequestChannel(Time time){
        this.time = time;
    }
    public void addProcessor(SocketServer.Processor processor) {
        processors.putIfAbsent(processor.id, processor);
    }

    public void clear() {
        this.requestQueue.clear();
        ;
    }

    public void sendShutdownRequest() throws InterruptedException {
        requestQueue.put(new ShutdownRequest());
    }

    public void shutdown() {
        clear();
    }

    public void sendRequest(Request req) throws InterruptedException {
        requestQueue.put(req);
    }
    void sendResponse(Response response) throws InterruptedException {

        if (response instanceof SendResponse ||
                response instanceof NoOpResponse ||
                response instanceof CloseConnectionResponse){
            Request request = response.request();
            long timeNanos = time.nanoseconds();
            request.responseCompleteTimeNanos = timeNanos;
            if (request.apiLocalCompleteTimeNanos == -1L)
                request.apiLocalCompleteTimeNanos = timeNanos;
        }

        SocketServer.Processor processor = processors.get(response.processor());
        // The processor may be null if it was shutdown. In this case, the connections
        // are closed, so the response is dropped.
        if (processor != null) {
            processor.enqueueResponse(response);
        }
    }

     public RequestChannel.BaseRequest receiveRequest(Long timeout) throws InterruptedException {
       return   requestQueue.poll(timeout, TimeUnit.MILLISECONDS);
     }



    public interface BaseRequest {
    }

    public static class ShutdownRequest implements BaseRequest {
    }

    public static class Request implements BaseRequest {
        String connectionId;
        public volatile long requestDequeueTimeNanos = -1L;
        volatile long apiLocalCompleteTimeNanos = -1L;
        volatile long responseCompleteTimeNanos = -1L;
        volatile long responseDequeueTimeNanos = -1L;
        volatile long messageConversionsTimeNanos = 0L;
        private int processor;
        private Optional<RequestChannel.Request> envelope;
        private ByteBuffer buffer;
        private MemoryPool memoryPool;

        public Request(int processor, Long startTimeNanos, MemoryPool memoryPool, ByteBuffer buffer, Optional<RequestChannel.Request> envelope) {
            this.processor = processor;
            this.envelope = envelope;
            this.memoryPool = memoryPool;
            this.buffer = buffer;
        }

        public void releaseBuffer() {
            if (envelope.isPresent()) {
                envelope.get().releaseBuffer();
            } else {
                memoryPool.release(buffer);
                buffer = null;
            }

        }


    }
    abstract static class Response {
        Request request;

        public Response(Request request) {
            this.request = request;
        }

        public int processor() {
            return request.processor;
        }

        public Request request() {
            return this.request;
        }

        public  void onComplete(Send send){};
    }

    class SendResponse extends Response {
        Send responseSend;
        Consumer<Send> onCompleteCallback;
        public SendResponse(Request request, Send responseSend,Consumer<Send> onCompleteCallback) {
            super(request);
            this.responseSend = responseSend;
            this.onCompleteCallback = onCompleteCallback;
        }

        @Override
        public void onComplete(Send send) {
            this.onCompleteCallback.accept(send);
        }
    }

    class NoOpResponse extends Response {

        public NoOpResponse(Request request) {
            super(request);
        }
    }

    class CloseConnectionResponse extends Response {

        public CloseConnectionResponse(Request request) {
            super(request);
        }
    }
}
