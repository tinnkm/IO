package com.tinnkm.kafka.common.network;

import com.tinnkm.kafka.common.errors.AuthenticationException;
import com.tinnkm.kafka.common.security.auth.KafkaPrincipal;
import com.tinnkm.kafka.common.security.auth.KafkaPrincipalSerde;
import com.tinnkm.kafka.memory.MemoryPool;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Optional;
import java.util.function.Supplier;

public class KafkaChannel {



    public enum ChannelMuteState {
        NOT_MUTED,
        MUTED,
        MUTED_AND_RESPONSE_PENDING,
        MUTED_AND_THROTTLED,
        MUTED_AND_THROTTLED_AND_RESPONSE_PENDING
    }
    public enum ChannelMuteEvent {
        REQUEST_RECEIVED,
        RESPONSE_SENT,
        THROTTLE_STARTED,
        THROTTLE_ENDED
    }

    private ChannelState state;
    private boolean disconnected;
    private SocketAddress remoteAddress;
    private final TransportLayer transportLayer;
    private String id;
    private NetworkReceive receive;
    private final int maxReceiveSize;
    private final MemoryPool memoryPool;
    private ChannelMuteState muteState;
    private NetworkSend send;
    private final Supplier<Authenticator> authenticatorCreator;
    private Authenticator authenticator;
    private int successfulAuthentications;
    private long networkThreadTimeNanos;

    private boolean midWrite;
   // private NetworkSend send;
    public KafkaChannel(String id,TransportLayer transportLayer,Supplier<Authenticator> authenticatorCreator,int maxReceiveSize, MemoryPool memoryPool) {
        this.id = id;
        this.disconnected = false;
        this.maxReceiveSize = maxReceiveSize;
        this.memoryPool = memoryPool;
        this.transportLayer = transportLayer;
        this.authenticatorCreator = authenticatorCreator;
        this.authenticator = authenticatorCreator.get();
    }

    public void state(ChannelState state) {
        this.state = state;
    }

    public ChannelState state() {
        return this.state;
    }

    public void disconnect() {
        disconnected = true;
        if (state == ChannelState.NOT_CONNECTED && remoteAddress != null) {
            //if we captured the remote address we can provide more information
            state = new ChannelState(ChannelState.State.NOT_CONNECTED, remoteAddress.toString());
        }
        transportLayer.disconnect();
    }
    public String id() {
        return id;
    }

    public SelectionKey selectionKey() {
        return transportLayer.selectionKey();
    }

    public void close() throws IOException {
        this.disconnected = true;
        transportLayer.close();
        receive.close();
        authenticator.close();
    }

    public KafkaPrincipal principal() {
        return authenticator.principal();
    }

    public Optional<KafkaPrincipalSerde> principalSerde() {
        return authenticator.principalSerde();
    }
    void mute() {
        if (muteState == ChannelMuteState.NOT_MUTED) {
            if (!disconnected) transportLayer.removeInterestOps(SelectionKey.OP_READ);
            muteState = ChannelMuteState.MUTED;
        }
    }

    public long read() throws IOException {
        if (receive == null) {
            receive = new NetworkReceive(maxReceiveSize, id, memoryPool);
        }

        long bytesReceived = receive(this.receive);

        if (this.receive.requiredMemoryAmountKnown() && !this.receive.memoryAllocated() && isInMutableState()) {
            //pool must be out of memory, mute ourselves.
            mute();
        }
        return bytesReceived;
    }

    public boolean isInMutableState() {
        //some requests do not require memory, so if we do not know what the current (or future) request is
        //(receive == null) we dont mute. we also dont mute if whatever memory required has already been
        //successfully allocated (if none is required for the currently-being-read request
        //receive.memoryAllocated() is expected to return true)
        if (receive == null || receive.memoryAllocated())
            return false;
        //also cannot mute if underlying transport is not in the ready state
        return transportLayer.ready();
    }

    private long receive(NetworkReceive receive) throws IOException {
        try {
            return receive.readFrom(transportLayer);
        } catch (Exception e) {
            // With TLSv1.3, post-handshake messages may throw SSLExceptions, which are
            // handled as authentication failures
            String remoteDesc = remoteAddress != null ? remoteAddress.toString() : null;
            state = new ChannelState(ChannelState.State.AUTHENTICATION_FAILED, e, remoteDesc);
            throw e;
        }
    }

    boolean maybeUnmute() {
        if (muteState == ChannelMuteState.MUTED) {
            if (!disconnected) transportLayer.addInterestOps(SelectionKey.OP_READ);
            muteState = ChannelMuteState.NOT_MUTED;
        }
        return muteState == ChannelMuteState.NOT_MUTED;
    }

    public boolean hasBytesBuffered() {
        return transportLayer.hasBytesBuffered();
    }


    public NetworkReceive maybeCompleteReceive() {
        // size和buffer都读完了，则认为这条消息读完了
        // size是消息长度
        // buffer是消息体
        if (receive != null && receive.complete()) {
            receive.payload().rewind();
            NetworkReceive result = receive;
            receive = null;
            return result;
        }
        return null;
    }

    public boolean isMuted() {
        return muteState != ChannelMuteState.NOT_MUTED;
    }

    public void handleChannelMuteEvent(ChannelMuteEvent event) {
        boolean stateChanged = false;
        switch (event) {
            case REQUEST_RECEIVED:
                if (muteState == ChannelMuteState.MUTED) {
                    muteState = ChannelMuteState.MUTED_AND_RESPONSE_PENDING;
                    stateChanged = true;
                }
                break;
            case RESPONSE_SENT:
                if (muteState == ChannelMuteState.MUTED_AND_RESPONSE_PENDING) {
                    muteState = ChannelMuteState.MUTED;
                    stateChanged = true;
                }
                if (muteState == ChannelMuteState.MUTED_AND_THROTTLED_AND_RESPONSE_PENDING) {
                    muteState = ChannelMuteState.MUTED_AND_THROTTLED;
                    stateChanged = true;
                }
                break;
            case THROTTLE_STARTED:
                if (muteState == ChannelMuteState.MUTED_AND_RESPONSE_PENDING) {
                    muteState = ChannelMuteState.MUTED_AND_THROTTLED_AND_RESPONSE_PENDING;
                    stateChanged = true;
                }
                break;
            case THROTTLE_ENDED:
                if (muteState == ChannelMuteState.MUTED_AND_THROTTLED) {
                    muteState = ChannelMuteState.MUTED;
                    stateChanged = true;
                }
                if (muteState == ChannelMuteState.MUTED_AND_THROTTLED_AND_RESPONSE_PENDING) {
                    muteState = ChannelMuteState.MUTED_AND_RESPONSE_PENDING;
                    stateChanged = true;
                }
        }
        if (!stateChanged) {
            throw new IllegalStateException("Cannot transition from " + muteState.name() + " for " + event.name());
        }
    }

    public void setSend(NetworkSend send) {
        if (this.send != null)
            throw new IllegalStateException("Attempt to begin a send operation with prior send operation still in progress, connection id is " + id);
        this.send = send;
        // 注册了一个OP_WRITE事件
        this.transportLayer.addInterestOps(SelectionKey.OP_WRITE);
    }

    void completeCloseOnAuthenticationFailure() throws IOException {
        transportLayer.addInterestOps(SelectionKey.OP_WRITE);
        authenticator.handleAuthenticationFailure();
    }

    public boolean finishConnect() throws IOException {
        //we need to grab remoteAddr before finishConnect() is called otherwise
        //it becomes inaccessible if the connection was refused.
        SocketChannel socketChannel = transportLayer.socketChannel();
        if (socketChannel != null) {
            remoteAddress = socketChannel.getRemoteAddress();
        }
        boolean connected = transportLayer.finishConnect();
        if (connected) {
            if (ready()) {
                state = ChannelState.READY;
            } else if (remoteAddress != null) {
                state = new ChannelState(ChannelState.State.AUTHENTICATE, remoteAddress.toString());
            } else {
                state = ChannelState.AUTHENTICATE;
            }
        }
        return connected;
    }

    public boolean ready() {
        return transportLayer.ready() && authenticator.complete();
    }


    public boolean isConnected() {
        return transportLayer.isConnected();
    }

    public void prepare() throws AuthenticationException, IOException {
        boolean authenticating = false;
        try {
            if (!transportLayer.ready())
                transportLayer.handshake();
            if (transportLayer.ready() && !authenticator.complete()) {
                authenticating = true;
                authenticator.authenticate();
            }
        } catch (AuthenticationException e) {
            // Clients are notified of authentication exceptions to enable operations to be terminated
            // without retries. Other errors are handled as network exceptions in Selector.
            String remoteDesc = remoteAddress != null ? remoteAddress.toString() : null;
            state = new ChannelState(ChannelState.State.AUTHENTICATION_FAILED, e, remoteDesc);
            if (authenticating) {
                delayCloseOnAuthenticationFailure();
                throw new DelayedResponseAuthenticationException(e);
            }
            throw e;
        }
        if (ready()) {
            ++successfulAuthentications;
            state = ChannelState.READY;
        }
    }

    private void delayCloseOnAuthenticationFailure() {
        transportLayer.removeInterestOps(SelectionKey.OP_WRITE);
    }

    public int successfulAuthentications() {
        return successfulAuthentications;
    }

    public Long reauthenticationLatencyMs() {
        return authenticator.reauthenticationLatencyMs();
    }

    boolean connectedClientSupportsReauthentication() {
        return authenticator.connectedClientSupportsReauthentication();
    }

    public Optional<NetworkReceive> pollResponseReceivedDuringReauthentication() {
        return authenticator.pollResponseReceivedDuringReauthentication();
    }

    public String socketDescription() {
        Socket socket = transportLayer.socketChannel().socket();
        if (socket.getInetAddress() == null)
            return socket.getLocalAddress().toString();
        return socket.getInetAddress().toString();
    }

    /**
     * Accumulates network thread time for this channel.
     */
    public void addNetworkThreadTimeNanos(long nanos) {
        networkThreadTimeNanos += nanos;
    }

    public boolean hasSend() {
        return send != null;
    }

    public boolean maybeBeginClientReauthentication(Supplier<Long> nowNanosSupplier)
            throws AuthenticationException, IOException {
        if (!ready())
            throw new IllegalStateException(
                    "KafkaChannel should always be \"ready\" when it is checked for possible re-authentication");
        if (muteState != ChannelMuteState.NOT_MUTED || midWrite
                || authenticator.clientSessionReauthenticationTimeNanos() == null)
            return false;
        /*
         * We've delayed getting the time as long as possible in case we don't need it,
         * but at this point we need it -- so get it now.
         */
        long nowNanos = nowNanosSupplier.get();
        if (nowNanos < authenticator.clientSessionReauthenticationTimeNanos())
            return false;
        swapAuthenticatorsAndBeginReauthentication(new ReauthenticationContext(authenticator, receive, nowNanos));
        receive = null;
        return true;
    }

    private void swapAuthenticatorsAndBeginReauthentication(ReauthenticationContext reauthenticationContext)
            throws IOException {
        // it is up to the new authenticator to close the old one
        // replace with a new one and begin the process of re-authenticating
        authenticator = authenticatorCreator.get();
        authenticator.reauthenticate(reauthenticationContext);
    }


    public long write() throws IOException {
        if (send == null)
            return 0;

        midWrite = true;
        return send.writeTo(transportLayer);
    }

    public NetworkSend maybeCompleteSend() {
        if (send != null && send.completed()) {
            midWrite = false;
            transportLayer.removeInterestOps(SelectionKey.OP_WRITE);
            NetworkSend result = send;
            send = null;
            return result;
        }
        return null;
    }
    public boolean serverAuthenticationSessionExpired(long nowNanos) {
        Long serverSessionExpirationTimeNanos = authenticator.serverSessionExpirationTimeNanos();
        return serverSessionExpirationTimeNanos != null && nowNanos - serverSessionExpirationTimeNanos > 0;
    }
}
