import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import com.fimtra.infra.channel.EndPointAddress;
import com.fimtra.infra.channel.StaticEndPointAddressFactory;
import com.fimtra.infra.datafission.ICodec;
import com.fimtra.infra.datafission.IRecord;
import com.fimtra.infra.datafission.IRecordChange;
import com.fimtra.infra.datafission.IRecordListener;
import com.fimtra.infra.datafission.IRpcInstance.TimeOutException;
import com.fimtra.infra.datafission.core.ContextUtils;
import com.fimtra.infra.datafission.core.HybridProtocolCodec;
import com.fimtra.infra.datafission.core.ProxyContext;
import com.fimtra.infra.datafission.field.LongValue;
import com.fimtra.infra.tcpchannel.TcpChannelBuilderFactory;
import com.fimtra.infra.tcpchannel.TcpChannelUtils;

/*
 * Copyright (c) 2015 Ramon Servadei, Fimtra
 * All rights reserved.
 * 
 * This file is subject to the terms and conditions defined in 
 * file 'LICENSE.txt', which is part of this source code package. 
 * The terms and conditions can also be found at http://fimtra.com/LICENSE.txt.
 */

/**
 * @author Ramon Servadei
 */
public class BenchmarkSubscriber
{

    public static void main(String[] args) throws IOException, InterruptedException, TimeOutException
    {
        final ICodec proxyCodec = new HybridProtocolCodec();
        final TcpChannelBuilderFactory channelBuilderFactory =
            new TcpChannelBuilderFactory(proxyCodec.getFrameEncodingFormat(), new StaticEndPointAddressFactory(
                new EndPointAddress(TcpChannelUtils.LOCALHOST_IP, 22222)));
        final ProxyContext proxyContext =
            new ProxyContext("BenchmarkSubscriber", proxyCodec, channelBuilderFactory);

        // subscribe for the 15 other data records
        for (int i = 1; i < 16; i++)
        {
            proxyContext.addObserver(new IRecordListener()
            {
                @Override
                public void onChange(IRecord imageValidInCallingThreadOnly, IRecordChange atomicChange)
                {
                    // noop
                }
            }, "BenchmarkRecord-" + i);
        }

        ContextUtils.getRpc(proxyContext, 2000, "runComplete");
        
        final CountDownLatch finished = new CountDownLatch(1);
        proxyContext.addObserver(new IRecordListener()
        {
            long startNanos;

            @Override
            public void onChange(IRecord imageValidInCallingThreadOnly, IRecordChange atomicChange)
            {
                if (imageValidInCallingThreadOnly.keySet().size() == 0)
                {
                    return;
                }

                final long updateNumber = imageValidInCallingThreadOnly.get("updateNumber").longValue();
                if (updateNumber == 1)
                {
                    startNanos = System.nanoTime();
                }
                else
                {
                    long maxUpdates = imageValidInCallingThreadOnly.get("maxUpdates").longValue();
                    if (updateNumber == maxUpdates)
                    {
                        long diffMicros = (System.nanoTime() - startNanos) / (1000 * maxUpdates);
                        long recordCount = imageValidInCallingThreadOnly.get("concurrentRecordCount").longValue();
                        System.err.println(recordCount + "," + diffMicros);
                        try
                        {
                            proxyContext.getRpc("runComplete").execute(LongValue.valueOf(diffMicros));
                        }
                        catch (Exception e)
                        {
                            e.printStackTrace();
                        }
                        
                        if (recordCount == imageValidInCallingThreadOnly.get("maxRecordCount").longValue())
                        {
                            finished.countDown();                     
                        }
                    }

                }
            }
        }, "BenchmarkRecord-0");

        finished.await();
        System.err.println("Finished");
    }

}
