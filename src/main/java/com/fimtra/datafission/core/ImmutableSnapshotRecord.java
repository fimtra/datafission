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
package com.fimtra.datafission.core;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;

import com.fimtra.datafission.IRecord;
import com.fimtra.datafission.IValue;

/**
 * A snapshot of a record that is also immutable.
 * 
 * @author Ramon Servadei
 */
public class ImmutableSnapshotRecord extends ImmutableRecord
{

    /**
     * Create a snapshot of the {@link IRecord} as the source for a new {@link ImmutableRecord}
     * instance.
     */
    public static ImmutableSnapshotRecord create(IRecord template)
    {
        if (template instanceof ImmutableRecord)
        {
            final ImmutableRecord immutable = (ImmutableRecord) template;
            final Record clone =
                new Record(immutable.name, immutable.data, null, new ConcurrentHashMap<String, Map<String, IValue>>(
                    immutable.subMaps)).clone();
            return new ImmutableSnapshotRecord(clone.getName(), immutable.getContextName(), template.getSequence(),
                clone.data, clone.subMaps, clone.writeLock);
        }
        else
        {
            final Record clone = ((Record) template).clone();
            return new ImmutableSnapshotRecord(clone.getName(), clone.getContextName(), template.getSequence(),
                clone.data, clone.subMaps, clone.writeLock);
        }
    }

    /**
     * Used to construct an immutable record from a snapshot of a record.
     */
    ImmutableSnapshotRecord(String name, String contextName, long sequence, Map<String, IValue> data,
        Map<String, Map<String, IValue>> subMaps, Lock writeLock)
    {
        super(name, contextName, new AtomicLong(sequence), data, subMaps, writeLock);
    }

    @Override
    public String toString()
    {
        return Record.toString("(ImmutableSnapshot)" + this.contextName, this.name, this.sequence.longValue(),
            this.data, this.subMaps);
    }
}
