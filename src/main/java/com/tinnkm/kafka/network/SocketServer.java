package com.tinnkm.kafka.network;

import com.tinnkm.kafka.common.network.KafkaChannel;
import com.tinnkm.kafka.common.network.NetworkSend;
import com.tinnkm.kafka.common.network.Send;
import com.tinnkm.kafka.common.utils.KafkaThread;
import com.tinnkm.kafka.common.utils.Time;
import com.tinnkm.kafka.common.utils.Utils;
import com.tinnkm.kafka.memory.MemoryPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class SocketServer {
    private final static Logger logger = LoggerFactory.getLogger(SocketServer.class);
    public RequestChannel dataPlaneRequestChannel;
    private boolean startedProcessingRequests = false;
    private boolean stoppedProcessingRequests = false;
    private Acceptor dataPlaneAcceptor = null;
    private AtomicInteger nextProcessorId = new AtomicInteger(0);
    private Time time;
    private MemoryPool memoryPool = MemoryPool.NONE;

    public SocketServer(Time time) {
        this.time = time;
        dataPlaneRequestChannel = new RequestChannel(this.time);
    }

    int nextProcessorId() {
        return nextProcessorId.getAndIncrement();
    }


    public void startProcessingRequests() throws InterruptedException {
        System.out.println("Starting socket server acceptors and processors");
        synchronized (this) {
            if (!startedProcessingRequests) {
                startDataPlaneProcessorsAndAcceptors();
                startedProcessingRequests = true;
            } else {
                System.out.println("Socket server acceptors and processors already started");
            }
        }
        System.out.println("Started socket server acceptors and processors");
    }

    private void startDataPlaneProcessorsAndAcceptors() throws InterruptedException {
        dataPlaneAcceptor.startProcessors();
        if (!dataPlaneAcceptor.isStarted()) {
            KafkaThread.nonDaemon(
                    "kafka-socket-acceptor",
                    dataPlaneAcceptor
            ).start();
            dataPlaneAcceptor.awaitStartup();
        }
    }

    public void startup(boolean startProcessingRequests) throws InterruptedException {
        synchronized (this) {
            // 移除控制面板监听之后都是数据面板
            createDataPlaneAcceptorsAndProcessors();
            // 一开始不开启
            if (startProcessingRequests) {
                this.startProcessingRequests();
            }
        }
    }

    private void createDataPlaneAcceptorsAndProcessors() {
        // 创建数据面板线程默认开启了一个Selector
        DataPlaneAcceptor dataPlaneAcceptor = createDataPlaneAcceptor(dataPlaneRequestChannel);
        // 添加处理者
        dataPlaneAcceptor.configure();
        this.dataPlaneAcceptor = dataPlaneAcceptor;
    }

    protected DataPlaneAcceptor createDataPlaneAcceptor(RequestChannel requestChannel) {
        return new DataPlaneAcceptor(this, requestChannel, this.time, this.memoryPool);
    }

    public synchronized void stopProcessingRequests() throws InterruptedException {
        dataPlaneAcceptor.initiateShutdown();
        dataPlaneAcceptor.awaitShutdown();
        dataPlaneRequestChannel.clear();
        stoppedProcessingRequests = true;

    }

    public synchronized void shutdown() throws InterruptedException {
        if (!stoppedProcessingRequests)
            stopProcessingRequests();
        dataPlaneRequestChannel.shutdown();

    }

    private class DataPlaneAcceptor extends Acceptor {
        private DataPlaneAcceptor(SocketServer socketServer, RequestChannel requestChannel, Time time, MemoryPool memoryPool) {
            super(socketServer, requestChannel, time, memoryPool);
        }

        void configure() {
            addProcessors(3);
        }
    }


    class Processor extends AbstractServerThread {
        public int id;
        private Time time;
        private int nextConnectionIndex;
        private ArrayBlockingQueue<SocketChannel> newConnections = new ArrayBlockingQueue<SocketChannel>(20);
        private LinkedBlockingDeque<RequestChannel.Response> responseQueue = new LinkedBlockingDeque<>();
        private com.tinnkm.kafka.common.network.Selector selector;
        private Map<String, RequestChannel.Response> inflightResponses = new HashMap<>();
        private final int maxRequestSize;
        private final MemoryPool memoryPool;
        private final RequestChannel requestChannel;

        public Processor(int id, Time time, RequestChannel requestChannel, int maxRequestSize, MemoryPool memoryPool) {
            this.id = id;
            this.time = time;
            this.maxRequestSize = maxRequestSize;
            this.memoryPool = memoryPool;
            this.requestChannel = requestChannel;
            try {
                this.selector = createSelector();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private com.tinnkm.kafka.common.network.Selector createSelector() throws IOException {
            return new com.tinnkm.kafka.common.network.Selector(this.maxRequestSize, this.time, this.memoryPool);
        }

        @Override
        public void run() {
            // 启动完成
            startupComplete();
            try {
                while (isRunning()) {
                    try {
                        // setup any new connections that have been queued up
                        // 一开始不会执行，因为newConnections是空
                        // 循环第二次进来的时候，会注册OP_READ事件
                        configureNewConnections();
                        // register any new responses for writing
                        // 一开始不会执行因为responseQueue是空
                        processNewResponses();
                        // 一开始线程阻塞再这里，等待事件
                        // 一开始的时候会被nioSelector执行完OP_ACCEPT之后唤醒
                        poll();
                        // 处理接收到的消息，并且拼接响应
                        processCompletedReceives();
                        // 发送响应
                        processCompletedSends();
                        processDisconnected();
                        closeExcessConnections();
                    } catch (Throwable e) {
                        // We catch all the throwables here to prevent the processor thread from exiting. We do this because
                        // letting a processor exit might cause a bigger impact on the broker. This behavior might need to be
                        // reviewed if we see an exception that needs the entire broker to stop. Usually the exceptions thrown would
                        // be either associated with a specific socket channel or a bad request. These exceptions are caught and
                        // processed by the individual methods above which close the failing channel and continue processing other
                        // channels. So this catch block should only ever see ControlThrowables.
                        logger.error("Processor got uncaught exception.", e);
                    }
                }
            } finally {
                shutdownComplete();
            }
        }

        private void processDisconnected(){
            selector.disconnected().keySet().forEach(connectionId ->{
            try {
                inflightResponses.remove(connectionId);
            } catch(Throwable e) {
                logger.error("Exception while processing disconnection of {}",connectionId, e);
            }
            });
        }

        private void closeExcessConnections(){
            // 此方法是如果连接数大于brokerMaxConnections，并且不是被保护的监听，就关闭channel
            // 默认brokerMaxConnections是int的最大，所以不考虑关闭
        }
        private void processCompletedSends(){
            selector.completedSends().forEach(send -> {
                try {
                    RequestChannel.Response response = inflightResponses.remove(send.destinationId());
                    if (response == null){
                        throw new IllegalStateException("Send for"+send.destinationId()+" completed, but not in `inflightResponses`");
                    }
                    // Invoke send completion callback
                    response.onComplete(send);

                    // Try unmuting the channel. If there was no quota violation and the channel has not been throttled,
                    // it will be unmuted immediately. If the channel has been throttled, it will unmuted only if the throttling
                    // delay has already passed by now.
                    handleChannelMuteEvent(send.destinationId(), KafkaChannel.ChannelMuteEvent.RESPONSE_SENT);
                    tryUnmuteChannel(send.destinationId());
                } catch (Throwable e){
                   logger.error("Exception while processing completed send to {}",send.destinationId(), e);
                }
            });
            selector.clearCompletedSends();
        }

        private void processCompletedReceives(){
            selector.completedReceives().forEach( receive -> {
                try {
                    openOrClosingChannel(receive.source()).ifPresent(channel -> {
                        long nowNanos = time.nanoseconds();
                        if (channel.serverAuthenticationSessionExpired(nowNanos)) {
                            // be sure to decrease connection count and drop any in-flight responses
                            close(channel.id());
                        } else {
                            String connectionId = receive.source();
                            RequestChannel.Request req = new RequestChannel.Request(id,
                                    nowNanos, memoryPool, receive.payload(),null);

                            // KIP-511: ApiVersionsRequest is intercepted here to catch the client software name
                            // and version. It is done here to avoid wiring things up to the api layer.
                            try {
                                requestChannel.sendRequest(req);
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                            selector.mute(connectionId);
                            handleChannelMuteEvent(connectionId, KafkaChannel.ChannelMuteEvent.REQUEST_RECEIVED);
                        }
                    });
                } catch (Throwable e){
                    // note that even though we got an exception, we can assume that receive.source is valid.
                    // Issues with constructing a valid receive object were handled earlier
                    logger.error("Exception while processing request from {}", receive.source(),e);
                }
            });
            selector.clearCompletedReceives();
        }


        private void poll() {
            long pollTimeout = newConnections.isEmpty() ? 300 : 0;
            try {
                selector.poll(pollTimeout);
            } catch (IllegalStateException | IOException e) {
                logger.error("Processor {} poll failed", this.id, e);
            }
        }

        private void processNewResponses() {
            RequestChannel.Response currentResponse = null;
            do {
                currentResponse = dequeueResponse();
                String channelId = currentResponse.request().connectionId;
                try {
                    switch (currentResponse) {
                        case RequestChannel.NoOpResponse response -> {
                            handleChannelMuteEvent(channelId, KafkaChannel.ChannelMuteEvent.RESPONSE_SENT);
                            tryUnmuteChannel(channelId);
                        }
                        case RequestChannel.SendResponse response -> {
                            sendResponse(response, response.responseSend);
                        }
                        case RequestChannel.CloseConnectionResponse response -> {
                            close(channelId);
                        }
                        default ->
                                throw new IllegalArgumentException("Unknown response type: "+currentResponse.getClass());
                    }
                } catch (Throwable e) {
                    logger.error("Exception while processing response for {}", channelId, e);
                }
            } while ((currentResponse != null));
        }

        private void tryUnmuteChannel(String connectionId) {
            openOrClosingChannel(connectionId).ifPresent(c -> selector.unmute(c.id()));
        }

        private void handleChannelMuteEvent(String connectionId, KafkaChannel.ChannelMuteEvent event) {
            openOrClosingChannel(connectionId).ifPresent(c -> c.handleChannelMuteEvent(event));
        }

        protected void sendResponse(RequestChannel.Response response, Send responseSend) {
            String connectionId = response.request.connectionId;
            // Invoke send for closingChannel as well so that the send is failed and the channel closed properly and
            // removed from the Selector after discarding any pending staged receives.
            // `openOrClosingChannel` can be None if the selector closed the connection because it was idle for too long
            if (openOrClosingChannel(connectionId).isPresent()) {
                selector.send(new NetworkSend(connectionId, responseSend));
                inflightResponses.put(connectionId, response);
            }
        }

        private Optional<KafkaChannel> channel(String connectionId) {
            return Optional.of(selector.channel(connectionId));
        }


        private RequestChannel.Response dequeueResponse() {
            RequestChannel.Response response = responseQueue.poll();
            if (response != null)
                response.request().responseDequeueTimeNanos = Time.SYSTEM.nanoseconds();
            return response;
        }

        private void configureNewConnections() throws IOException {
            while (!newConnections.isEmpty()) {
                SocketChannel channel = newConnections.poll();
                try {
                    logger.debug("Processor {} listening to new connection from {}", this.id, channel.socket().getRemoteSocketAddress());
                    selector.register(connectionId(channel.socket()), channel);
                } catch (Throwable e) {
                    // We explicitly catch all exceptions and close the socket to avoid a socket leak.
                    SocketAddress remoteAddress = channel.socket().getRemoteSocketAddress();
                    // need to close the channel here to avoid a socket leak.
                    close(channel);
                    logger.error("Processor {} closed connection from {}", this.id, remoteAddress);
                }
            }
        }

        protected String connectionId(Socket socket) {
            String localHost = socket.getLocalAddress().getHostAddress();
            int localPort = socket.getLocalPort();
            String remoteHost = socket.getInetAddress().getHostAddress();
            int remotePort = socket.getPort();
            String connId = String.format("%s:%s-%st:%s-%s", localHost, localPort, remoteHost, remotePort, nextConnectionIndex);
            nextConnectionIndex = nextConnectionIndex == Integer.MAX_VALUE ? 0 : nextConnectionIndex + 1;
            return connId;
        }

        private void close(String connectionId) {
            selector.close(connectionId);
            inflightResponses.remove(connectionId);
        }

        public boolean accept(SocketChannel socketChannel, Boolean mayBlock, Time time) throws InterruptedException {
            boolean accepted = false;
            // 入队
            if (newConnections.offer(socketChannel))
                accepted = true;
            else if (mayBlock) {
                long startNs = time.nanoseconds();
                newConnections.put(socketChannel);
                accepted = true;
            }

            if (accepted) {
                // 成功后唤醒等待线程
                wakeup();
            }
            return accepted;
        }

        private Optional<KafkaChannel> openOrClosingChannel(String connectionId) {
            return Optional.of(Optional.of(selector.channel(connectionId)).orElse(selector.closingChannel(connectionId)));
        }

        @Override
        void wakeup() {
            selector.wakeup();
        }
        public void enqueueResponse(RequestChannel.Response response) throws InterruptedException {
            responseQueue.put(response);
            wakeup();
        }
    }


    private abstract class Acceptor extends AbstractServerThread {
        private static final Logger logger = LoggerFactory.getLogger(Acceptor.class);
        private java.nio.channels.Selector nioSelector = null;
        private AtomicBoolean processorsStarted = new AtomicBoolean();
        private List<Processor> processors = new ArrayList<>();
        private RequestChannel requestChannel = null;
        private SocketServer socketServer = null;
        private ServerSocketChannel serverChannel;
        protected Boolean isRunning = super.alive.get();
        private int currentProcessorIndex = 0;
        private Time time;
        private MemoryPool memoryPool;


        private Acceptor(SocketServer socketServer, RequestChannel requestChannel, Time time, MemoryPool memoryPool) {
            try {
                this.nioSelector = java.nio.channels.Selector.open();
                this.requestChannel = requestChannel;
                this.socketServer = socketServer;
                this.serverChannel = openServerSocket("127.0.0.1", 9092, 50);
                this.time = time;
                this.memoryPool = memoryPool;
            } catch (IOException e) {
                System.exit(0);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void run() {
            try {
                serverChannel.register(nioSelector, SelectionKey.OP_ACCEPT);
                startupComplete();
                while (isRunning) {
                    try {
                        acceptNewConnections();
                    } catch (Throwable e) {
                        System.out.println("Error occurred");
                    }
                }
            } catch (ClosedChannelException e) {
                throw new RuntimeException(e);
            } finally {
                try {
                    serverChannel.close();
                    nioSelector.close();
                } catch (IOException ignored) {

                }
                shutdownComplete();
            }
        }

        private ServerSocketChannel openServerSocket(String host, int port, int listenBacklogSize) throws Exception {
            InetSocketAddress socketAddress = Utils.isBlank(host) ? new InetSocketAddress(port) : new InetSocketAddress(host, port);
            // 开启ServerSocketChannel
            ServerSocketChannel serverChannel = ServerSocketChannel.open();
            serverChannel.configureBlocking(false);
            serverChannel.socket().setReceiveBufferSize(100 * 1024);

            try {
                serverChannel.socket().bind(socketAddress, listenBacklogSize);
            } catch (SocketException e) {
                throw new Exception("Socket server failed to bind to " + socketAddress.getHostString() + ":" + port + ": " + e.getMessage() + ".", e);
            }
            return serverChannel;
        }

        private void acceptNewConnections() throws IOException {
            // 线程阻塞
            int ready = nioSelector.select(500);
            if (ready > 0) {
                Set<SelectionKey> keys = nioSelector.selectedKeys();
                Iterator<SelectionKey> iter = keys.iterator();
                while (iter.hasNext() && isRunning) {
                    try {
                        SelectionKey key = iter.next();
                        iter.remove();

                        // 处理ACCEPT事件
                        if (key.isAcceptable()) {
                            accept(key).ifPresent(socketChannel -> {
                                int retriesLeft = 0;
                                synchronized (this) {
                                    retriesLeft = processors.size();
                                }
                                Processor processor = null;
                                do {
                                    retriesLeft -= 1;
                                    // 轮询取处理连接的processor
                                    synchronized (this) {
                                        // adjust the index (if necessary) and retrieve the processor atomically for
                                        // correct behaviour in case the number of processors is reduced dynamically
                                        // 这里类似轮询，对processors取模
                                        currentProcessorIndex = currentProcessorIndex % processors.size();
                                        processor = processors.get(currentProcessorIndex);
                                    }
                                    currentProcessorIndex += 1;
                                } while (!assignNewConnection(socketChannel, processor, retriesLeft == 0));
                            });

                        } else
                            throw new IllegalStateException("Unrecognized key state for acceptor thread.");
                    } catch (Throwable e) {
                        logger.error("Error while accepting connection", e);
                    }
                }
            }
        }

        private Optional<SocketChannel> accept(SelectionKey key) throws IOException {
            ServerSocketChannel serverSocketChannel = (ServerSocketChannel) key.channel();
            // accept连接
            SocketChannel socketChannel = serverSocketChannel.accept();
            try {
                configureAcceptedSocketChannel(socketChannel);
            } catch (Throwable e) {
                close(socketChannel);
            }
            return Optional.of(socketChannel);
        }

        protected void configureAcceptedSocketChannel(SocketChannel socketChannel) throws IOException {
            socketChannel.configureBlocking(false);
            socketChannel.socket().setTcpNoDelay(true);
            socketChannel.socket().setKeepAlive(true);
            socketChannel.socket().setSendBufferSize(100 * 1024);
        }

        private Boolean assignNewConnection(SocketChannel socketChannel, Processor processor, Boolean mayBlock) {
            try {
                return processor.accept(socketChannel, mayBlock, this.time);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        void wakeup() {
            nioSelector.wakeup();
        }

        public synchronized void startProcessors() {
            // 设置processorsStarted=true
            if (!processorsStarted.getAndSet(true)) {
                // 启动Processors
                startProcessors(processors);
            }
        }

        private synchronized void startProcessors(List<Processor> processors) {
            processors.forEach(processor ->
                    KafkaThread.nonDaemon(
                            "kafka-network-thread-processor",
                            processor
                    ).start());
        }

        void addProcessors(Integer toCreate) {
            List<Processor> listenerProcessors = new ArrayList<>();

            // 创建3个处理者
            for (int i = 0; i < toCreate; i++) {
                // socketServer.nextProcessorId()
                Processor processor = newProcessor(socketServer.nextProcessorId());
                listenerProcessors.add(processor);
                requestChannel.addProcessor(processor);
            }

            processors.addAll(listenerProcessors);
            // processorsStarted.get 默认false
            if (processorsStarted.get())
                startProcessors(listenerProcessors);
        }

        Processor newProcessor(int id) {
            return new Processor(id, this.time, requestChannel, 100 * 1024 * 1024, this.memoryPool);
        }
    }


    abstract class AbstractServerThread implements Runnable {
        private CountDownLatch startupLatch = new CountDownLatch(1);
        private volatile CountDownLatch shutdownLatch = new CountDownLatch(0);
        protected AtomicBoolean alive = new AtomicBoolean(true);

        abstract void wakeup();

        /**
         * Initiates a graceful shutdown by signaling to stop
         */
        void initiateShutdown() {
            if (alive.getAndSet(false))
                wakeup();
        }

        /**
         * Wait for the thread to completely shutdown
         */
        void awaitShutdown() throws InterruptedException {
            shutdownLatch.await();
        }

        /**
         * Returns true if the thread is completely started
         */
        Boolean isStarted() {
            return startupLatch.getCount() == 0;
        }

        /**
         * Wait for the thread to completely start up
         */
        void awaitStartup() throws InterruptedException {
            startupLatch.await();
        }

        /**
         * Record that the thread startup is complete
         */
        protected void startupComplete() {
            // Replace the open latch with a closed one
            shutdownLatch = new CountDownLatch(1);
            startupLatch.countDown();
        }

        /**
         * Record that the thread shutdown is complete
         */
        protected void shutdownComplete() {
            shutdownLatch.countDown();
        }

        /**
         * Is the server still running?
         */
        protected Boolean isRunning() {
            return alive.get();
        }

        /**
         * Close `channel` and decrement the connection count.
         */
        void close(SocketChannel channel) throws IOException {
            if (channel != null) {
                closeSocket(channel);
            }
        }

        protected void closeSocket(SocketChannel channel) throws IOException {
            channel.socket().close();
            channel.close();
        }
    }
}
