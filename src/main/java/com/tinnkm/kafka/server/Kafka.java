package com.tinnkm.kafka.server;

public class Kafka {
    public static void main(String[] args) {
        try {
            KafkaServer server = new KafkaServer();
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    server.shutdown();
                }catch (Throwable e){
                    System.exit(1);
                }
            },"kafka-shutdown-hook"));
            try {
                server.startup();
            } catch (Exception e) {
                System.exit(1);
            }
            server.awaitShutdown();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(1);
    }
}
