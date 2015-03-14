import java.io.IOException;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.fimtra.infra.datafission.IObserverContext.ISystemRecordNames;
import com.fimtra.infra.datafission.IRecord;
import com.fimtra.infra.datafission.IRecordChange;
import com.fimtra.infra.datafission.IRecordListener;
import com.fimtra.infra.datafission.IRpcInstance.ExecutionException;
import com.fimtra.infra.datafission.IRpcInstance.TimeOutException;
import com.fimtra.infra.datafission.IValue;
import com.fimtra.infra.datafission.IValue.TypeEnum;
import com.fimtra.infra.datafission.core.Context;
import com.fimtra.infra.datafission.core.HybridProtocolCodec;
import com.fimtra.infra.datafission.core.Publisher;
import com.fimtra.infra.datafission.core.RpcInstance;
import com.fimtra.infra.datafission.core.RpcInstance.IRpcExecutionHandler;
import com.fimtra.infra.datafission.field.DoubleValue;
import com.fimtra.infra.datafission.field.LongValue;
import com.fimtra.infra.datafission.field.TextValue;
import com.fimtra.infra.tcpchannel.TcpChannelUtils;
import com.fimtra.infra.util.SystemUtils;

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
        Publisher publisher = new Publisher(context, new HybridProtocolCodec(), TcpChannelUtils.LOCALHOST_IP, 22222);

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

        System.err.print("Waiting for subscriber...");
        start.await();
        System.err.println("done");

        // warmup
        doTest(context, runLatch);

        StringBuilder results = doTest(context, runLatch);

        results.append("CPU count: " + Runtime.getRuntime().availableProcessors()).append(SystemUtils.lineSeparator());
        results.append("JVM version: " + System.getProperty("java.version")).append(SystemUtils.lineSeparator());
        System.err.println(results);

        System.err.println("Finished");
        System.in.read();
    }

    static StringBuilder doTest(Context context, final AtomicReference<CountDownLatch> runLatch)
        throws InterruptedException
    {
        StringBuilder sb = new StringBuilder();
        sb.append("Concurrent record count, avg latency (uSec)").append(SystemUtils.lineSeparator());

        IRecord record;
        final int maxUpdates = 10000;
        final int maxRecordCount = 32;
        final Random rnd = new Random();
        long runStartNanos, runLatencyMicros, publishCount;
        int maxUpdatesForRecordCount;
        for (int recordCount = 1; recordCount <= maxRecordCount; recordCount++)
        {
            maxUpdatesForRecordCount = (int) ((double) maxUpdates / recordCount);
            System.err.print("Updating " + recordCount + " concurrent records, total update count per record will be "
                + maxUpdatesForRecordCount + "...");
            runLatch.set(new CountDownLatch(1));
            publishCount = 0;
            runStartNanos = System.nanoTime();
            for (int updateNumber = 1; updateNumber <= maxUpdatesForRecordCount; updateNumber++)
            {
                // get each record and update - go backwards so the 0th one is always the last one
                for (int k = recordCount - 1; k > -1; k--)
                {
                    record = context.getOrCreateRecord("BenchmarkRecord-" + k);
                    record.put("concurrentRecordCount", LongValue.valueOf(recordCount));
                    record.put("maxUpdates", LongValue.valueOf(maxUpdatesForRecordCount));
                    record.put("updateNumber", LongValue.valueOf(updateNumber));
                    record.put("data1", LongValue.valueOf(rnd.nextLong()));
                    record.put("data2", DoubleValue.valueOf(rnd.nextDouble()));
                    record.put("data3", TextValue.valueOf("" + rnd.nextLong()));
                    context.publishAtomicChange(record);
                    publishCount++;
                }
            }

            // wait for the subscriber to acknowledge this run
            System.err.print("waiting for run to complete...");
            runLatch.get().await();
            runLatencyMicros = (System.nanoTime() - runStartNanos) / 1000;
            System.err.println("completed.");
            sb.append(recordCount).append(",").append((runLatencyMicros / (publishCount))).append(
                SystemUtils.lineSeparator());
        }
        return sb;
    }
}
