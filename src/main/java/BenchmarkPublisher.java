import java.io.IOException;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.fimtra.infra.datafission.IObserverContext.ISystemRecordNames;
import com.fimtra.infra.datafission.IRpcInstance.ExecutionException;
import com.fimtra.infra.datafission.IRpcInstance.TimeOutException;
import com.fimtra.infra.datafission.IRecord;
import com.fimtra.infra.datafission.IRecordChange;
import com.fimtra.infra.datafission.IRecordListener;
import com.fimtra.infra.datafission.IValue;
import com.fimtra.infra.datafission.IValue.TypeEnum;
import com.fimtra.infra.datafission.core.Context;
import com.fimtra.infra.datafission.core.Publisher;
import com.fimtra.infra.datafission.core.RpcInstance;
import com.fimtra.infra.datafission.core.RpcInstance.IRpcExecutionHandler;
import com.fimtra.infra.datafission.core.StringProtocolCodec;
import com.fimtra.infra.datafission.field.DoubleValue;
import com.fimtra.infra.datafission.field.LongValue;
import com.fimtra.infra.datafission.field.TextValue;
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
public class BenchmarkPublisher
{
    public static void main(String[] args) throws IOException, InterruptedException
    {
        // create the context that will hold the record(s)
        Context context = new Context("BenchmarkPublisher");

        // enable remote access to the context, this opens a TCP server socket on localhost:222222
        Publisher publisher = new Publisher(context, new StringProtocolCodec(), TcpChannelUtils.LOCALHOST_IP, 22222);

        final AtomicLong runTimerEnd = new AtomicLong();
        final AtomicReference<CountDownLatch> runLatch = new AtomicReference<CountDownLatch>();

        // this RPC is called by the subscriber after each run completes
        context.createRpc(new RpcInstance(new IRpcExecutionHandler()
        {
            @Override
            public IValue execute(IValue... args) throws TimeOutException, ExecutionException
            {
                runTimerEnd.set(args[0].longValue());
                runLatch.get().countDown();
                return null;
            }
        }, TypeEnum.TEXT, "runComplete", TypeEnum.LONG));

        IRecord record = context.getOrCreateRecord("BenchmarkRecord-0");

        // wait for subscribers
        final CountDownLatch start = new CountDownLatch(1);
        context.addObserver(new IRecordListener()
        {
            @Override
            public void onChange(IRecord imageValidInCallingThreadOnly, IRecordChange atomicChange)
            {
                if (imageValidInCallingThreadOnly.keySet().contains("BenchmarkRecord-0"))
                {
                    start.countDown();
                }
            }
        }, ISystemRecordNames.CONTEXT_SUBSCRIPTIONS);

        System.err.println("Waiting for subscriber...");
        start.await();
        System.err.println("Subscriber available...");

        final int maxUpdates = 1000;
        final int maxRecordCount = 32;
        final Random rnd = new Random();
        long runStartNanos, runLatencyNanos, publishCount;
        for (int recordCount = 1; recordCount < maxRecordCount; recordCount++)
        {
            System.err.println("Updating " + recordCount + " concurrent records...");
            runLatch.set(new CountDownLatch(1));
            publishCount = 0;
            runStartNanos = System.nanoTime();
            for (int updateNumber = 1; updateNumber <= maxUpdates; updateNumber++)
            {
                // get each record and update
                for (int k = 0; k < recordCount; k++)
                {
                    record = context.getOrCreateRecord("BenchmarkRecord-" + k);
                    record.put("maxRecordCount", LongValue.valueOf(maxRecordCount));
                    record.put("concurrentRecordCount", LongValue.valueOf(recordCount));
                    record.put("maxUpdates", LongValue.valueOf(maxUpdates));
                    record.put("updateNumber", LongValue.valueOf(updateNumber));
                    record.put("data1", LongValue.valueOf(rnd.nextLong()));
                    record.put("data2", DoubleValue.valueOf(rnd.nextDouble()));
                    record.put("data3", TextValue.valueOf("" + rnd.nextLong()));
                    context.publishAtomicChange(record);
                    publishCount++;
                }
            }

            // wait for the subscriber to acknowledge this run
            System.err.println("Waiting for run to complete...");
            runLatch.get().await();
            runLatencyNanos = (System.nanoTime() - runStartNanos) / 1000;
            System.err.println("For " + recordCount + ", avg latency=" + (runLatencyNanos / (publishCount)));
        }
        System.err.println("Finished");
        System.in.read();
    }
}
