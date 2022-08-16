package com.tinnkm.kafka.common.network;
public class ChannelState {
    public enum State {
        NOT_CONNECTED,
        AUTHENTICATE,
        READY,
        EXPIRED,
        FAILED_SEND,
        AUTHENTICATION_FAILED,
        LOCAL_CLOSE
    }

    // AUTHENTICATION_FAILED has a custom exception. For other states,
    // create a reusable `ChannelState` instance per-state.
    public static final ChannelState NOT_CONNECTED = new ChannelState(State.NOT_CONNECTED);
    public static final ChannelState AUTHENTICATE = new ChannelState(State.AUTHENTICATE);
    public static final ChannelState READY = new ChannelState(State.READY);
    public static final ChannelState EXPIRED = new ChannelState(State.EXPIRED);
    public static final ChannelState FAILED_SEND = new ChannelState(State.FAILED_SEND);
    public static final ChannelState LOCAL_CLOSE = new ChannelState(State.LOCAL_CLOSE);

    private final State state;
    private final Exception exception;
    private final String remoteAddress;

    public ChannelState(State state) {
        this(state, null, null);
    }

    public ChannelState(State state, String remoteAddress) {
        this(state, null, remoteAddress);
    }

    public ChannelState(State state, Exception exception, String remoteAddress) {
        this.state = state;
        this.exception = exception;
        this.remoteAddress = remoteAddress;
    }

    public State state() {
        return state;
    }

    public Exception exception() {
        return exception;
    }

    public String remoteAddress() {
        return remoteAddress;
    }
}

