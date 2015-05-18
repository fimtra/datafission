/*
 * Copyright (c) 2013 Ramon Servadei 
 *  
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *    
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.fimtra.tcpchannel;

import java.io.IOException;
import java.net.ConnectException;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import com.fimtra.channel.ChannelUtils;
import com.fimtra.channel.IReceiver;
import com.fimtra.channel.ITransportChannel;
import com.fimtra.util.Log;
import com.fimtra.util.ObjectUtils;

/**
 * Provides the ability to send and receive data over TCP to a peer.
 * <p>
 * TCP socket options are configured by system properties and are applied by the
 * {@link TcpChannelUtils#setOptions(SocketChannel)} method.
 * 
 * @author Ramon Servadei
 */
public class TcpChannel implements ITransportChannel
{
    /** Expresses the encoding format for the data frames */
    public static enum FrameEncodingFormatEnum
    {
        /**
         * Length-based frame encoding, in the following format (ABNF notation):
         * 
         * <pre>
         *  stream = 1*frame
         *  frame = length data
         * 
         *  length = 4OCTET ; big-endian integer indicating the length of the data
         *  data = 1*OCTET ; the data
         * </pre>
         */
        LENGTH_BASED,

        /**
         * Terminator-based frame encoding, in the following format (ABNF notation):
         * 
         * <pre>
         * stream = 1*frame
         * 
         * frame = data terminator
         * data = 1*OCTET ; the data
         * 
         * terminator = OCTET OCTET ; an ASCII control code, possibly 0x3 for ETX (end of text)
         * </pre>
         */
        TERMINATOR_BASED;

        /**
         * @param tcpChannel
         *            the channel to create a frame reader-writer for
         * @return the reader-writer that supports this frame format
         */
        public IFrameReaderWriter getFrameReaderWriter(TcpChannel tcpChannel)
        {
            switch(this)
            {
                case LENGTH_BASED:
                    return new LengthBasedWriter(tcpChannel);
                case TERMINATOR_BASED:
                    return new TerminatorBasedReaderWriter(tcpChannel);
                default :
                    throw new IllegalStateException("No support for " + this);
            }
        }
    }

    private static final String TCP_CHANNEL_CLOSED = "TcpChannel [closed ";
    private static final String TCP_CHANNEL_PENDING = "TcpChannel [pending ";
    private static final String TCP_CHANNEL_CONNECTED = "TcpChannel [connected ";

    /** Represents the ASCII code for CRLF */
    static final byte[] TERMINATOR = { 0xd, 0xa };

    int rxData;
    final IReceiver receiver;
    final ByteBuffer rxFrames;
    final Queue<byte[]> txFrames;
    final SocketChannel socketChannel;
    final IFrameReaderWriter readerWriter;
    final ByteArrayFragmentResolver byteArrayFragmentResolver;
    /** The details of the end-point socket connection */
    final String endPointSocketDescription;
    /** The short-hand description of the end-point connections */
    final String shortSocketDescription;
    /**
     * Tracks if the {@link IReceiver#onChannelConnected(ITcpChannel)} has been called. Ensures its
     * only called once.
     */
    private boolean onChannelConnectedCalled;
    /**
     * Tracks if the {@link IReceiver#onChannelClosed(ITcpChannel)} has been called. Ensures its
     * only called once.
     */
    private final AtomicBoolean onChannelClosedCalled;

    /**
     * Construct a {@link TcpChannel} with a default receive buffer size and default frame encoding
     * format.
     * 
     * @see TcpChannelProperties#FRAME_ENCODING
     * @see TcpChannelProperties#RX_BUFFER_SIZE
     * @see #TcpChannel(String, int, IReceiver, int, FrameEncodingFormatEnum)
     */
    public TcpChannel(String serverHost, int serverPort, IReceiver receiver) throws ConnectException
    {
        this(serverHost, serverPort, receiver, TcpChannelProperties.RX_BUFFER_SIZE);
    }

    /**
     * Construct a {@link TcpChannel} with a specific receive buffer size and default frame encoding
     * format.
     * 
     * @see TcpChannelProperties#FRAME_ENCODING
     * @see #TcpChannel(String, int, IReceiver, int, FrameEncodingFormatEnum)
     */
    public TcpChannel(String serverHost, int serverPort, IReceiver receiver, int rxBufferSize) throws ConnectException
    {
        this(serverHost, serverPort, receiver, rxBufferSize, TcpChannelProperties.FRAME_ENCODING);
    }

    /**
     * Construct a {@link TcpChannel} with a default receive buffer size and specific frame encoding
     * format.
     * 
     * @see TcpChannelProperties#RX_BUFFER_SIZE
     * @see #TcpChannel(String, int, IReceiver, int, FrameEncodingFormatEnum)
     */
    public TcpChannel(String serverHost, int serverPort, IReceiver receiver, FrameEncodingFormatEnum frameEncodingFormat)
        throws ConnectException
    {
        this(serverHost, serverPort, receiver, TcpChannelProperties.RX_BUFFER_SIZE, frameEncodingFormat);
    }

    /**
     * Construct a client {@link TcpChannel} that connects to a TCP server. This establishes a TCP
     * connection to the indicated host and port. After the constructor completes, the channel is
     * ready for use however the underlying {@link SocketChannel} may not yet be connected. This
     * does not stop the channel from being used; any data queued up to send via the
     * {@link #sendAsync(byte[])} method will be sent when the connection completes.
     * 
     * @param serverHost
     *            the target host for the TCP connection
     * @param port
     *            the target port for the TCP connection
     * @param receiver
     *            the object that will receive all the communication data from the target host
     * @param rxBufferSize
     *            the size of the receive buffer in bytes
     * @param frameEncodingFormat
     *            the enum for the frame format for this channel
     * @throws ConnectException
     *             if the TCP connection could not be established
     */
    public TcpChannel(final String serverHost, final int serverPort, final IReceiver receiver, int rxBufferSize,
        FrameEncodingFormatEnum frameEncodingFormat) throws ConnectException
    {
        this.onChannelClosedCalled = new AtomicBoolean();
        this.rxFrames = ByteBuffer.wrap(new byte[rxBufferSize]);
        this.txFrames = new ConcurrentLinkedQueue<byte[]>();
        this.byteArrayFragmentResolver = ByteArrayFragmentResolver.newInstance(frameEncodingFormat);
        this.receiver = receiver;
        this.readerWriter = frameEncodingFormat.getFrameReaderWriter(this);
        this.endPointSocketDescription = serverHost + ":" + serverPort;
        this.socketChannel = TcpChannelUtils.createAndConnectNonBlockingSocketChannel(serverHost, serverPort);
        this.shortSocketDescription =
            this.socketChannel.socket().getLocalSocketAddress() + "->" + getEndPointDescription();
        finishConstruction();
    }

    /** Internally used constructor for server-side channels */
    TcpChannel(SocketChannel socketChannel, IReceiver receiver, int rxBufferSize,
        FrameEncodingFormatEnum frameEncodingFormat) throws ConnectException
    {
        this.onChannelClosedCalled = new AtomicBoolean();
        this.socketChannel = socketChannel;
        this.rxFrames = ByteBuffer.wrap(new byte[rxBufferSize]);
        this.txFrames = new ConcurrentLinkedQueue<byte[]>();
        this.byteArrayFragmentResolver = ByteArrayFragmentResolver.newInstance(frameEncodingFormat);
        this.receiver = receiver;
        this.readerWriter = frameEncodingFormat.getFrameReaderWriter(this);

        final Socket socket = this.socketChannel.socket();
        this.endPointSocketDescription = socket.getInetAddress().getHostAddress() + ":" + socket.getPort();
        this.shortSocketDescription = socket.getLocalSocketAddress() + "->" + getEndPointDescription();

        finishConstruction();
    }

    private void finishConstruction() throws ConnectException
    {
        // this can be overridden by system properties
        try
        {
            this.socketChannel.socket().setTcpNoDelay(true);
        }
        catch (SocketException e1)
        {
            Log.log(TcpChannel.this, "Could not set TCP_NODELAY option on " + ObjectUtils.safeToString(this), e1);
        }

        TcpChannelUtils.setOptions(this.socketChannel);

        try
        {
            TcpChannelUtils.WRITER.register(this.socketChannel, new Runnable()
            {
                @Override
                public void run()
                {
                    writeFrames();
                }
            });

            TcpChannelUtils.WRITER.resetInterest(this.socketChannel);
        }
        catch (Exception e)
        {
            String message = this + " could not register for write operations";
            Log.log(this, message, e);
            throw new ConnectException(message);
        }
        try
        {
            TcpChannelUtils.READER.register(this.socketChannel, new Runnable()
            {
                @Override
                public void run()
                {
                    readFrames();
                }
            });
        }
        catch (Exception e)
        {
            String message = this + " could not register for read operations";
            Log.log(this, message, e);
            throw new ConnectException(message);
        }

        ChannelUtils.WATCHDOG.addChannel(this);

        Log.log(this, "Constructed ", ObjectUtils.safeToString(this));
    }

    @Override
    public boolean sendAsync(byte[] toSend)
    {
        try
        {
            final List<byte[]> byteFragmentsToSend =
                this.byteArrayFragmentResolver.getByteFragmentsToSend(toSend, TcpChannelProperties.TX_SEND_SIZE);
            for (byte[] byteFragment : byteFragmentsToSend)
            {
                this.txFrames.add(byteFragment);
            }
            TcpChannelUtils.WRITER.setInterest(this.socketChannel);
            return true;
        }
        catch (Exception e)
        {
            destroy("Could not send data", e);
            return false;
        }
    }

    @Override
    public String toString()
    {
        return (this.onChannelConnectedCalled ? (this.onChannelClosedCalled.get() ? TCP_CHANNEL_CLOSED
            : TCP_CHANNEL_CONNECTED) : TCP_CHANNEL_PENDING) + getDescription() + "]";
    }

    @Override
    protected void finalize() throws Throwable
    {
        super.finalize();
        destroy("finalize");
    }

    void readFrames()
    {
        try
        {
            if (this.socketChannel.read(this.rxFrames) == -1)
            {
                destroy("End-of-stream reached");
                return;
            }
            this.rxData++;
            final List<byte[]> readFrames = this.readerWriter.readFrames();
            final List<byte[]> resolvedFrames = new ArrayList<byte[]>(readFrames.size());
            byte[] data = null;
            for (byte[] byteArrayFragmentTxData : readFrames)
            {
                if ((data = this.byteArrayFragmentResolver.resolve(byteArrayFragmentTxData)) != null)
                {
                    resolvedFrames.add(data);
                }
            }

            for (byte[] frame : resolvedFrames)
            {
                switch(frame.length)
                {
                    case 1:
                        if (ChannelUtils.isHeartbeatSignal(frame))
                        {
                            ChannelUtils.WATCHDOG.onHeartbeat(TcpChannel.this);
                            if (!this.onChannelConnectedCalled)
                            {
                                this.onChannelConnectedCalled = true;
                                Log.log(this, "Connected ", ObjectUtils.safeToString(this.socketChannel));
                                this.receiver.onChannelConnected(TcpChannel.this);
                            }
                            break;
                        }
                        //$FALL-THROUGH$
                    default :
                        try
                        {
                            this.receiver.onDataReceived(frame, this);
                        }
                        catch (Exception e)
                        {
                            Log.log(this,
                                ObjectUtils.safeToString(this) + " receiver " + ObjectUtils.safeToString(this.receiver)
                                    + " could not handle data", e);
                        }
                }
            }
        }
        catch (IOException e)
        {
            destroy("Could not read from socket (" + e.toString() + ")");
        }
    }

    void writeFrames()
    {
        try
        {
            TcpChannelUtils.WRITER.resetInterest(TcpChannel.this.socketChannel);
        }
        catch (CancelledKeyException e)
        {
            destroy("Socket has been closed", e);
            return;
        }
        this.readerWriter.writeFrames();
    }

    @Override
    public boolean isConnected()
    {
        try
        {
            return this.socketChannel.isConnected() && this.socketChannel.isOpen();
        }
        catch (Exception e)
        {
            Log.log(this, "Could not determine connected state", e);
            return false;
        }
    }

    @Override
    public void destroy(String reason, Exception... e)
    {
        if (this.onChannelClosedCalled.getAndSet(true))
        {
            return;
        }

        if (e == null || e.length == 0)
        {
            Log.log(this, reason, " ", ObjectUtils.safeToString(this));
        }
        else
        {
            Log.log(this, reason + " " + ObjectUtils.safeToString(this), e[0]);
        }

        try
        {
            if (this.socketChannel != null)
            {
                TcpChannelUtils.closeChannel(this.socketChannel);
                TcpChannelUtils.READER.cancel(this.socketChannel);
                TcpChannelUtils.WRITER.cancel(this.socketChannel);
            }
            this.txFrames.clear();
            this.rxFrames.clear();
            this.receiver.onChannelClosed(this);
        }
        catch (Exception e1)
        {
            Log.log(this, "Could not destroy " + ObjectUtils.safeToString(this), e1);
        }
    }

    @Override
    public String getEndPointDescription()
    {
        return this.endPointSocketDescription;
    }

    @Override
    public String getDescription()
    {
        return this.shortSocketDescription;
    }

    @Override
    public boolean hasRxData()
    {
        final boolean hasRxData = this.rxData > 0;
        this.rxData = 0;
        return hasRxData;
    }
}

/**
 * Interface for the component that reads and writes transmission frames for a {@link TcpChannel}
 * 
 * @author Ramon Servadei
 */
interface IFrameReaderWriter
{
    /**
     * @return the frames from the {@link TcpChannel#rxFrames}
     */
    List<byte[]> readFrames();

    /**
     * Write the frames held in the {@link TcpChannel#txFrames}
     */
    void writeFrames();
}

/**
 * Base class for {@link IFrameReaderWriter} implementations. This class handles partial writing of
 * data buffers to a socket. The {@link #writeBuffersToSocket()} method will continually attempt to
 * write the buffers to the socket until all data has been sent.
 * 
 * @author Ramon Servadei
 */
abstract class AbstractFrameReaderWriter implements IFrameReaderWriter
{
    int txLength;
    int bytesWritten;
    final ByteBuffer[] buffers = new ByteBuffer[2];
    final TcpChannel tcpChannel;

    AbstractFrameReaderWriter(TcpChannel tcpChannel)
    {
        super();
        this.tcpChannel = tcpChannel;
    }

    /**
     * Write the internal {@link #buffers} to the socket. This will continually try to write the
     * buffer to the socket until all the data has been written.
     * 
     * @throws IOException
     */
    void writeBuffersToSocket() throws IOException
    {
        while (this.bytesWritten != this.txLength)
        {
            this.bytesWritten += this.tcpChannel.socketChannel.write(this.buffers);
        }
    }

    @Override
    public final void writeFrames()
    {
        try
        {
            doWriteFrames();
        }
        catch (IOException e)
        {
            this.tcpChannel.destroy("Could not write frames (" + e.toString() + ")");
        }
        catch (Exception e)
        {
            this.tcpChannel.destroy("Could not write frames", e);
        }
    }

    abstract void doWriteFrames() throws Exception;
}

/**
 * Handles frame encoding using a terminator field after each frame.
 * 
 * @see TcpChannelUtils#decodeUsingTerminator(ByteBuffer, byte[])
 * @author Ramon Servadei
 */
class TerminatorBasedReaderWriter extends AbstractFrameReaderWriter
{
    private final ByteBuffer terminatorByteBuffer = ByteBuffer.wrap(TcpChannel.TERMINATOR);

    TerminatorBasedReaderWriter(TcpChannel tcpChannel)
    {
        super(tcpChannel);
    }

    @Override
    public List<byte[]> readFrames()
    {
        return TcpChannelUtils.decodeUsingTerminator(this.tcpChannel.rxFrames, TcpChannel.TERMINATOR);
    }

    @Override
    void doWriteFrames() throws Exception
    {
        byte[] data = null;
        this.buffers[1] = this.terminatorByteBuffer;
        while ((data = this.tcpChannel.txFrames.poll()) != null)
        {
            this.bytesWritten = 0;
            this.txLength = data.length + TcpChannel.TERMINATOR.length;
            this.buffers[0] = ByteBuffer.wrap(data);
            writeBuffersToSocket();
            this.terminatorByteBuffer.flip();
        }
    }
}

/**
 * Handles frame encoding with a 4-byte length field before each frame.
 * 
 * @see TcpChannelUtils#decode(ByteBuffer)
 * @author Ramon Servadei
 */
class LengthBasedWriter extends AbstractFrameReaderWriter
{
    private final ByteBuffer txLengthBuffer = ByteBuffer.wrap(new byte[4]);

    LengthBasedWriter(TcpChannel tcpChannel)
    {
        super(tcpChannel);
    }

    @Override
    public List<byte[]> readFrames()
    {
        return TcpChannelUtils.decode(this.tcpChannel.rxFrames);
    }

    @Override
    void doWriteFrames() throws Exception
    {
        byte[] data = null;
        this.buffers[0] = this.txLengthBuffer;
        while ((data = this.tcpChannel.txFrames.poll()) != null)
        {
            this.bytesWritten = 0;
            this.txLength = data.length + 4;
            this.txLengthBuffer.putInt(0, data.length);
            this.buffers[1] = ByteBuffer.wrap(data);
            writeBuffersToSocket();
            this.txLengthBuffer.flip();
        }
    }
}