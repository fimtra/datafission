/*
 * Copyright (c) 2015 Ramon Servadei 
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
import com.fimtra.infra.datafission.core.ProxyContext;
import com.fimtra.infra.datafission.core.StringProtocolCodec;
import com.fimtra.infra.tcpchannel.TcpChannelBuilderFactory;
import com.fimtra.infra.tcpchannel.TcpChannelUtils;

/**
 * Benchmark subscriber. Run after starting a {@link BenchmarkPublisher}
 * 
 * @author Ramon Servadei
 */
public class BenchmarkSubscriber
{

    public static void main(String[] args) throws IOException, InterruptedException, TimeOutException
    {
        final ICodec proxyCodec = new StringProtocolCodec();
        final TcpChannelBuilderFactory channelBuilderFactory =
            new TcpChannelBuilderFactory(proxyCodec.getFrameEncodingFormat(), new StaticEndPointAddressFactory(
                new EndPointAddress(args.length == 0 ? TcpChannelUtils.LOOPBACK : args[0], 22222)));
        final ProxyContext proxyContext = new ProxyContext("BenchmarkSubscriber", proxyCodec, channelBuilderFactory);

        ContextUtils.getRpc(proxyContext, 2000, "runComplete");

        final CountDownLatch finished = new CountDownLatch(1);
        IRecordListener listener = new IRecordListener()
        {
            int runCount = 0;

            @Override
            public void onChange(IRecord imageValidInCallingThreadOnly, IRecordChange atomicChange)
            {
                if (imageValidInCallingThreadOnly.keySet().size() == 0)
                {
                    return;
                }

                final long updateNumber = imageValidInCallingThreadOnly.get("updateNumber").longValue();
                long maxUpdates = imageValidInCallingThreadOnly.get("maxUpdates").longValue();
                if (updateNumber == maxUpdates)
                {
                    try
                    {
                        proxyContext.getRpc("runComplete").execute();

                        // first run is a warmup, hence 2 *
                        if (++this.runCount == 2 * imageValidInCallingThreadOnly.get("maxRecordCount").longValue())
                        {
                            finished.countDown();
                        }
                    }
                    catch (Exception e)
                    {
                        e.printStackTrace();
                    }
                }
            }
        };
        // subscribe for the 15 other data records
        for (int i = 1; i < 16; i++)
        {
            proxyContext.addObserver(listener, "BenchmarkRecord-" + i);
        }
        // this subscription triggers the test
        proxyContext.addObserver(listener, "BenchmarkRecord-0");

        finished.await();
        System.err.println("Finished");
    }

}
