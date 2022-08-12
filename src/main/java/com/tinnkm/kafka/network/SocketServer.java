package com.tinnkm.kafka.network;

import com.tinnkm.kafka.common.utils.KafkaThread;
import com.tinnkm.kafka.common.utils.Time;
import com.tinnkm.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.channels.*;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class SocketServer {
    public RequestChannel dataPlaneRequestChannel = new RequestChannel();
    private boolean startedProcessingRequests = false;
    private boolean stoppedProcessingRequests = false;
    private Acceptor dataPlaneAcceptor = null;
    private AtomicInteger nextProcessorId = new AtomicInteger(0);
    private Time time;

    public SocketServer(Time time) {
        this.time = time;
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
        return new DataPlaneAcceptor(this, requestChannel, this.time);
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
        private DataPlaneAcceptor(SocketServer socketServer, RequestChannel requestChannel, Time time) {
            super(socketServer, requestChannel, time);
        }

        void configure() {
            addProcessors(3);
        }
    }


    class Processor extends AbstractServerThread {
        public int id;
        private ArrayBlockingQueue<SocketChannel> newConnections = new ArrayBlockingQueue<SocketChannel>(20);
        private Selector selector;

        public Processor(int id, RequestChannel requestChannel) {
            this.id = id;
        }

        @Override
        public void run() {

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

        @Override
        void wakeup() {
            selector.wakeup();
        }
    }


    private abstract class Acceptor extends AbstractServerThread {
        private static final Logger logger = LoggerFactory.getLogger(Acceptor.class);
        private Selector nioSelector = null;
        private AtomicBoolean processorsStarted = new AtomicBoolean();
        private List<Processor> processors = new ArrayList<>();
        private RequestChannel requestChannel = null;
        private SocketServer socketServer = null;
        private ServerSocketChannel serverChannel;
        protected Boolean isRunning = super.alive.get();
        private int currentProcessorIndex = 0;
        private Time time;


        private Acceptor(SocketServer socketServer, RequestChannel requestChannel, Time time) {
            try {
                this.nioSelector = Selector.open();
                this.requestChannel = requestChannel;
                this.socketServer = socketServer;
                this.serverChannel = openServerSocket("127.0.0.1", 9092, 50);
                this.time = time;
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
            return new Processor(id, requestChannel);
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
