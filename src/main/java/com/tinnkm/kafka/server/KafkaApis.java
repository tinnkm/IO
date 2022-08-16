package com.tinnkm.kafka.server;

import com.tinnkm.kafka.network.RequestChannel;

public class KafkaApis implements KafkaRequestHandler.ApiRequestHandler {
    private final RequestChannel requestChannel;
    public KafkaApis(RequestChannel requestChannel) {
        this.requestChannel= requestChannel;
    }

    @Override
    public void handle(RequestChannel.Request request) throws InterruptedException {
        // 根据header处理具体业务
        // 1. 根据header判断具体业务
        // 2. 组件response
        // 3. 存入Processor形成闭环
        requestChannel.sendRequest(request);
    }

}
