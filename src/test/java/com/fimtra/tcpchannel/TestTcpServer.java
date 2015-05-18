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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.ConnectException;
import java.nio.channels.SelectionKey;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.fimtra.channel.ChannelUtils;
import com.fimtra.channel.IReceiver;
import com.fimtra.channel.ITransportChannel;
import com.fimtra.tcpchannel.TcpChannel;
import com.fimtra.tcpchannel.TcpChannelUtils;
import com.fimtra.tcpchannel.TcpServer;
import com.fimtra.tcpchannel.TcpChannel.FrameEncodingFormatEnum;
import com.fimtra.util.Log;

/**
 * Tests the {@link TcpServer} and {@link TcpChannel}
 * 
 * @author Ramon Servadei
 */
@SuppressWarnings({ "boxing", "unused", "unchecked" })
public class TestTcpServer
{
    public static class EchoReceiver implements IReceiver
    {
        final CountDownLatch channelClosedLatch = new CountDownLatch(1);

        public EchoReceiver()
        {
        }

        @Override
        public void onDataReceived(byte[] data, ITransportChannel source)
        {
            source.sendAsync(data);
        }

        @Override
        public void onChannelClosed(ITransportChannel tcpChannel)
        {
            this.channelClosedLatch.countDown();
        }

        @Override
        public void onChannelConnected(ITransportChannel tcpChannel)
        {
        }
    }

    public static class NoopReceiver implements IReceiver
    {
        public NoopReceiver()
        {
        }

        @Override
        public void onDataReceived(byte[] data, ITransportChannel source)
        {
        }

        @Override
        public void onChannelClosed(ITransportChannel tcpChannel)
        {
        }

        @Override
        public void onChannelConnected(ITransportChannel tcpChannel)
        {
        }
    }

    static byte[] toBytes(Serializable s) throws IOException
    {
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(byteStream);
        oos.writeObject(s);
        return byteStream.toByteArray();
    }

    static <T extends Serializable> T fromBytes(byte[] bytes) throws IOException, ClassNotFoundException
    {
        ByteArrayInputStream byteStream = new ByteArrayInputStream(bytes);
        ObjectInputStream ois = new ObjectInputStream(byteStream);
        return (T) ois.readObject();
    }

    private final static int STD_TIMEOUT = 10;
    private static final String LOCALHOST = "localhost";
    static int PORT = 12000;

    final static IReceiver noopReceiver = new NoopReceiver();

    TcpServer server;

    FrameEncodingFormatEnum frameEncodingFormat;

    public TestTcpServer()
    {
        this.frameEncodingFormat = FrameEncodingFormatEnum.TERMINATOR_BASED;
    }

    @BeforeClass
    public static void init()
    {
        ChannelUtils.WATCHDOG.configure(100, 20);
    }

    @Before
    public void setUp() throws Exception
    {
        PORT += 1;
        PORT = TcpChannelUtils.getNextFreeTcpServerPort(null, PORT, PORT + 100);
        Log.log(this, "port=" + PORT);
    }

    @After
    public void tearDown() throws Exception
    {
        this.server.destroy();
    }

    @Test
    public void testTcpServerShutdownAndRestartOnSamePort() throws IOException, InterruptedException
    {
        this.server = new TcpServer(LOCALHOST, PORT, new EchoReceiver(), this.frameEncodingFormat);
        this.server.destroy();
        Thread.sleep(100);
        this.server = new TcpServer(LOCALHOST, PORT, new EchoReceiver(), this.frameEncodingFormat);
        this.server.destroy();
        Thread.sleep(100);
        this.server = new TcpServer(LOCALHOST, PORT, new EchoReceiver(), this.frameEncodingFormat);
    }

    @Test
    public void testTcpServerShutdown() throws IOException, InterruptedException
    {
        final CountDownLatch channelConnectedLatch = new CountDownLatch(2);
        final CountDownLatch channelClosedLatch = new CountDownLatch(2);
        this.server = new TcpServer(LOCALHOST, PORT, new EchoReceiver(), this.frameEncodingFormat);

        final TcpChannel client = new TcpChannel(LOCALHOST, PORT, new NoopReceiver()
        {
            @Override
            public void onChannelConnected(ITransportChannel tcpChannel)
            {
                channelConnectedLatch.countDown();
            }

            @Override
            public void onChannelClosed(ITransportChannel tcpChannel)
            {
                channelClosedLatch.countDown();
            }
        }, this.frameEncodingFormat);
        final TcpChannel client2 = new TcpChannel(LOCALHOST, PORT, new NoopReceiver()
        {
            @Override
            public void onChannelConnected(ITransportChannel tcpChannel)
            {
                channelConnectedLatch.countDown();
            }

            @Override
            public void onChannelClosed(ITransportChannel tcpChannel)
            {
                channelClosedLatch.countDown();
            }
        }, this.frameEncodingFormat);

        assertTrue("Channel connected callbacks invoked " + (2 - channelConnectedLatch.getCount()) + " times",
            channelConnectedLatch.await(STD_TIMEOUT, TimeUnit.SECONDS));

        this.server.destroy();

        assertTrue("Channel closed callbacks invoked " + (2 - channelClosedLatch.getCount()) + " times",
            channelClosedLatch.await(STD_TIMEOUT, TimeUnit.SECONDS));

        assertFalse(client.sendAsync("sdf3".getBytes()));
        assertFalse(client2.sendAsync("sdf3".getBytes()));
    }

    @Test(expected = ConnectException.class)
    public void testAttemptConnectionWhenServerIsShutDown() throws IOException, InterruptedException
    {
        this.server = new TcpServer(LOCALHOST, PORT, new EchoReceiver(), this.frameEncodingFormat);
        this.server.destroy();
        Thread.sleep(200);
        final TcpChannel client = new TcpChannel(LOCALHOST, PORT, new NoopReceiver());
    }

    @Test
    public void testSimpleClientServerMessageSending() throws IOException, InterruptedException
    {
        final CountDownLatch latch = new CountDownLatch(4);
        final List<String> expected1 = new ArrayList<String>();
        final List<String> received1 = new ArrayList<String>();
        final List<String> expected2 = new ArrayList<String>();
        final List<String> received2 = new ArrayList<String>();
        final String message1 = "hello1";
        final String message2 = "hello2";
        expected1.add(message1);
        expected1.add(message2);
        expected2.add(message1);
        expected2.add(message2);
        this.server = new TcpServer(LOCALHOST, PORT, new EchoReceiver(), this.frameEncodingFormat);

        final TcpChannel client = new TcpChannel(LOCALHOST, PORT, new NoopReceiver()
        {
            @Override
            public void onDataReceived(byte[] data, ITransportChannel source)
            {
                received1.add(new String(data));
                latch.countDown();
            }
        }, this.frameEncodingFormat);

        final TcpChannel client2 = new TcpChannel(LOCALHOST, PORT, new NoopReceiver()
        {
            @Override
            public void onDataReceived(byte[] data, ITransportChannel source)
            {
                received2.add(new String(data));
                latch.countDown();
            }
        }, this.frameEncodingFormat);
        assertTrue(client.sendAsync(message1.getBytes()));
        assertTrue(client.sendAsync(message2.getBytes()));
        assertTrue(client2.sendAsync(message1.getBytes()));
        assertTrue(client2.sendAsync(message2.getBytes()));
        assertTrue("onDataReceived only called " + (4 - latch.getCount()) + " times",
            latch.await(STD_TIMEOUT, TimeUnit.SECONDS));
        assertEquals(expected1, received1);
        assertEquals(expected2, received2);
    }

    @Test
    public void testBigMessageClientServerMessageSending() throws IOException, InterruptedException
    {
        final CountDownLatch latch = new CountDownLatch(4);
        final List<String> expected1 = new ArrayList<String>();
        final List<String> received1 = new ArrayList<String>();
        final List<String> expected2 = new ArrayList<String>();
        final List<String> received2 = new ArrayList<String>();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 1000; i++)
        {
            sb.append("hello").append(i);
        }
        final String message1 = sb.toString();
        final String message2 = "hello2";
        expected1.add(message1);
        expected1.add(message2);
        expected2.add(message1);
        expected2.add(message2);
        this.server = new TcpServer(LOCALHOST, PORT, new EchoReceiver(), this.frameEncodingFormat);

        final TcpChannel client = new TcpChannel(LOCALHOST, PORT, new NoopReceiver()
        {
            @Override
            public void onDataReceived(byte[] data, ITransportChannel source)
            {
                received1.add(new String(data));
                latch.countDown();
            }
        }, this.frameEncodingFormat);

        final TcpChannel client2 = new TcpChannel(LOCALHOST, PORT, new NoopReceiver()
        {
            @Override
            public void onDataReceived(byte[] data, ITransportChannel source)
            {
                received2.add(new String(data));
                latch.countDown();
            }
        }, this.frameEncodingFormat);
        assertTrue(client.sendAsync(message1.getBytes()));
        assertTrue(client.sendAsync(message2.getBytes()));
        assertTrue(client2.sendAsync(message1.getBytes()));
        assertTrue(client2.sendAsync(message2.getBytes()));
        assertTrue("onDataReceived only called " + (4 - latch.getCount()) + " times",
            latch.await(STD_TIMEOUT, TimeUnit.SECONDS));
        assertEquals(expected1, received1);
        assertEquals(expected2, received2);
    }

    @Test
    public void testHighThroughputClient() throws IOException, InterruptedException
    {
        final int messageCount = 5000;
        final int[] last = new int[] { 0 };
        final CountDownLatch latch = new CountDownLatch(messageCount);

        this.server = new TcpServer(LOCALHOST, PORT, new NoopReceiver()
        {
            @Override
            public void onDataReceived(byte[] data, ITransportChannel source)
            {
                int now = Integer.valueOf(new String(data)).intValue();
                if (last[0] + 1 != now)
                {
                    throw new RuntimeException("Invalid sequence: last=" + last[0] + ", now=" + now);
                }
                last[0] = now;
                latch.countDown();
            }
        }, this.frameEncodingFormat);

        final TcpChannel client = new TcpChannel(LOCALHOST, PORT, noopReceiver, this.frameEncodingFormat);
        int i = 0;
        while (i < messageCount)
        {
            client.sendAsync(("" + ++i).getBytes());
        }
        assertTrue("Only received " + (messageCount - latch.getCount()) + " correct messages",
            latch.await(STD_TIMEOUT, TimeUnit.SECONDS));
    }

    @Test
    public void testMultipleConnections() throws IOException, InterruptedException
    {
        this.server = new TcpServer(LOCALHOST, PORT, new EchoReceiver(), this.frameEncodingFormat);
        final int clientCount = 50;
        final CountDownLatch channelConnectedLatch = new CountDownLatch(clientCount);
        final CountDownLatch closedLatch = new CountDownLatch(clientCount);
        List<TcpChannel> clients = new ArrayList<TcpChannel>(clientCount);
        for (int i = 0; i < clientCount; i++)
        {
            clients.add(new TcpChannel(LOCALHOST, PORT, new NoopReceiver()
            {
                @Override
                public void onChannelConnected(ITransportChannel tcpChannel)
                {
                    channelConnectedLatch.countDown();
                }

                @Override
                public void onChannelClosed(ITransportChannel tcpChannel)
                {
                    closedLatch.countDown();
                }
            }, this.frameEncodingFormat));
        }
        assertTrue("Only connected " + ((clientCount) - channelConnectedLatch.getCount()) + " clients",
            channelConnectedLatch.await(STD_TIMEOUT, TimeUnit.SECONDS));
        assertTrue(closedLatch.getCount() == clientCount);
        this.server.destroy();
        assertTrue("Only closed " + ((clientCount) - closedLatch.getCount()) + " clients",
            closedLatch.await(STD_TIMEOUT, TimeUnit.SECONDS));

        // now loop around until we get our callback invoked
        final int MAX_TRIES = 10;
        for (int j = 0; j <= MAX_TRIES; j++)
        {
            boolean connected = false;
            for (int i = 0; i < clientCount; i++)
            {
                final TcpChannel tcpChannel = clients.get(i);
                connected |= tcpChannel.isConnected();
                if (j == MAX_TRIES)
                {
                    assertFalse("Not CLOSED at index " + i + ", " + tcpChannel.toString(), connected);
                }
            }
            if (!connected)
            {
                break;
            }
            Thread.sleep(100);
        }
    }

    @Test
    public void testMultipleConnectionsAndSending() throws IOException, InterruptedException
    {
        final int messageCount = 200;
        final int clientCount = 50;
        final CountDownLatch latch = new CountDownLatch(messageCount * clientCount);

        this.server = new TcpServer(LOCALHOST, PORT, new NoopReceiver()
        {
            Map<ITransportChannel, Integer> lastValue = new HashMap<ITransportChannel, Integer>();

            @Override
            public void onDataReceived(byte[] data, ITransportChannel source)
            {
                Integer now = Integer.valueOf(new String(data));
                Integer then = this.lastValue.get(source);
                if (then == null)
                {
                    then = Integer.valueOf(-1);
                }
                assertEquals(then + 1, now.intValue());
                this.lastValue.put(source, now);
                latch.countDown();
            }
        }, this.frameEncodingFormat);

        List<TcpChannel> clients = new ArrayList<TcpChannel>(clientCount);
        for (int i = 0; i < clientCount; i++)
        {
            clients.add(new TcpChannel(LOCALHOST, PORT, noopReceiver, this.frameEncodingFormat));
        }

        for (int j = 0; j < messageCount; j++)
        {
            for (int i = 0; i < clientCount; i++)
            {
                clients.get(i).sendAsync(("" + j).getBytes());
            }
        }
        assertTrue("Only received " + ((messageCount * clientCount) - latch.getCount()) + " correct messages",
            latch.await(STD_TIMEOUT, TimeUnit.SECONDS));
    }

    @Test
    public void testChannelClose() throws IOException, InterruptedException
    {
        final CountDownLatch channelConnectedLatch = new CountDownLatch(1);
        final CountDownLatch closedLatch = new CountDownLatch(1);
        this.server = new TcpServer(LOCALHOST, PORT, new EchoReceiver(), this.frameEncodingFormat);
        TcpChannel client = new TcpChannel(LOCALHOST, PORT, new NoopReceiver()
        {
            @Override
            public void onChannelConnected(ITransportChannel tcpChannel)
            {
                channelConnectedLatch.countDown();
            }

            @Override
            public void onChannelClosed(ITransportChannel tcpChannel)
            {
                closedLatch.countDown();
            }
        }, this.frameEncodingFormat);
        assertTrue("channel was not connected", channelConnectedLatch.await(STD_TIMEOUT, TimeUnit.SECONDS));
        client.destroy("unit test");
        assertFalse("channel is still connected", client.isConnected());

        SelectionKey keyFor = client.socketChannel.keyFor(TcpChannelUtils.READER.selector);
        if (keyFor != null)
        {
            assertFalse("key should be invalid", keyFor.isValid());
        }
        assertTrue("channel was not closed", closedLatch.await(STD_TIMEOUT, TimeUnit.SECONDS));
    }

    @Test
    public void testClientSocketCloseIsDetected() throws IOException, InterruptedException
    {
        final CountDownLatch channelConnectedLatch = new CountDownLatch(1);
        final CountDownLatch closedLatch = new CountDownLatch(1);
        this.server = new TcpServer(LOCALHOST, PORT, new EchoReceiver(), this.frameEncodingFormat);
        TcpChannel client = new TcpChannel(LOCALHOST, PORT, new NoopReceiver()
        {
            @Override
            public void onChannelConnected(ITransportChannel tcpChannel)
            {
                channelConnectedLatch.countDown();
            }

            @Override
            public void onChannelClosed(ITransportChannel tcpChannel)
            {
                closedLatch.countDown();
            }
        }, this.frameEncodingFormat);
        assertTrue("channel was not connected", channelConnectedLatch.await(STD_TIMEOUT, TimeUnit.SECONDS));
        client.socketChannel.close();
        SelectionKey keyFor = client.socketChannel.keyFor(TcpChannelUtils.READER.selector);
        if (keyFor != null)
        {
            assertFalse("key should be invalid", keyFor.isValid());
        }
        assertTrue("channel was not closed", closedLatch.await(STD_TIMEOUT, TimeUnit.SECONDS));
    }

    @Test(expected = ConnectException.class)
    public void testClientSocketConnectToNullServer() throws IOException
    {
        final CountDownLatch closedLatch = new CountDownLatch(1);
        this.server = new TcpServer(LOCALHOST, PORT, new EchoReceiver(), this.frameEncodingFormat);
        TcpChannel client = new TcpChannel(LOCALHOST, 12345, new NoopReceiver()
        {
            @Override
            public void onChannelClosed(ITransportChannel tcpChannel)
            {
                closedLatch.countDown();
            }
        }, this.frameEncodingFormat);
    }

    @Test
    public void testClientReferencesRemovedWhenSocketCloses() throws IOException, InterruptedException
    {
        final CountDownLatch channelConnectedLatch = new CountDownLatch(1);
        final CountDownLatch closedLatch = new CountDownLatch(1);
        EchoReceiver clientSocketReceiver = new EchoReceiver();
        this.server = new TcpServer(LOCALHOST, PORT, clientSocketReceiver, this.frameEncodingFormat);
        TcpChannel client = new TcpChannel(LOCALHOST, PORT, new NoopReceiver()
        {
            @Override
            public void onChannelConnected(ITransportChannel tcpChannel)
            {
                channelConnectedLatch.countDown();
            }

            @Override
            public void onChannelClosed(ITransportChannel tcpChannel)
            {
                closedLatch.countDown();
            }
        }, this.frameEncodingFormat);
        assertTrue("channel was not connected", channelConnectedLatch.await(STD_TIMEOUT, TimeUnit.SECONDS));
        assertEquals(1, this.server.clients.size());
        client.socketChannel.close();
        SelectionKey keyFor = client.socketChannel.keyFor(TcpChannelUtils.READER.selector);
        if (keyFor != null)
        {
            assertFalse("key should be invalid", keyFor.isValid());
        }
        assertTrue("channel was not closed", closedLatch.await(STD_TIMEOUT, TimeUnit.SECONDS));
        assertTrue(clientSocketReceiver.channelClosedLatch.await(STD_TIMEOUT, TimeUnit.SECONDS));
        assertEquals(0, this.server.clients.size());
    }

    @Test
    public void testMassiveMessage() throws IOException, InterruptedException
    {
        ChannelUtils.WATCHDOG.configure(1000);
        final CountDownLatch channelConnectedLatch = new CountDownLatch(1);
        final CountDownLatch dataLatch = new CountDownLatch(1);
        final AtomicReference<byte[]> dataRef = new AtomicReference<byte[]>();
        EchoReceiver clientSocketReceiver = new EchoReceiver();
        this.server = new TcpServer(LOCALHOST, PORT, clientSocketReceiver, this.frameEncodingFormat);
        TcpChannel client = new TcpChannel(LOCALHOST, PORT, new NoopReceiver()
        {
            @Override
            public void onChannelConnected(ITransportChannel tcpChannel)
            {
                channelConnectedLatch.countDown();
            }

            @Override
            public void onDataReceived(byte[] data, ITransportChannel source)
            {
                dataRef.set(data);
                dataLatch.countDown();
            }

        }, this.frameEncodingFormat);
        assertTrue("channel was not connected", channelConnectedLatch.await(STD_TIMEOUT, TimeUnit.SECONDS));
        client.sendAsync(generateMassiveMessage(65539));
        final int timeout = 1;
        switch(this.frameEncodingFormat)
        {
            case LENGTH_BASED:
                assertTrue(dataLatch.await(timeout, TimeUnit.SECONDS));
                assertEquals("data rx", 65539, dataRef.get().length);
                break;
            case TERMINATOR_BASED:
                assertTrue(dataLatch.await(timeout, TimeUnit.SECONDS));
                assertEquals("data rx", 65539, dataRef.get().length);
                break;
        }
    }

    private static byte[] generateMassiveMessage(int i)
    {
        final byte[] data = new byte[i];
        Arrays.fill(data, (byte) '0');
        return data;
    }
}