/*
 * Copyright (c) 2013 Ramon 
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

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;

import com.fimtra.datafission.IRecord;
import com.fimtra.datafission.IValue;
import com.fimtra.util.is;

/**
 * An immutable {@link IRecord}.
 * <p>
 * Immutable here means the contents of the record cannot be changed by this object. <b>However, the
 * contents of the record can change if the immutable record is created with a LIVE record backing
 * it. Changes made to the live instance will be seen by this immutable instance.</b>
 * 
 * @author Ramon Servadei
 */
public class ImmutableRecord implements IRecord
{
    final String name;
    final AtomicLong sequence;
    final String contextName;
    final Map<String, IValue> data;
    final Map<String, Map<String, IValue>> subMaps;
    final Lock writeLock;

    /**
     * A record constructs its sub-maps lazily, this accessor provides access to the actual instance
     * being used at any point in time.
     * 
     * @author Ramon Servadei
     */
    private static final class RecordSubMapAccessor implements Map<String, Map<String, IValue>>
    {
        final Record liveRecord;

        RecordSubMapAccessor(Record liveRecord)
        {
            super();
            this.liveRecord = liveRecord;
        }

        @Override
        public int size()
        {
            return this.liveRecord.subMaps.size();
        }

        @Override
        public boolean isEmpty()
        {
            return this.liveRecord.subMaps.isEmpty();
        }

        @Override
        public boolean containsKey(Object key)
        {
            return this.liveRecord.subMaps.containsKey(key);
        }

        @Override
        public boolean containsValue(Object value)
        {
            return this.liveRecord.subMaps.containsValue(value);
        }

        @Override
        public Map<String, IValue> get(Object key)
        {
            return this.liveRecord.subMaps.get(key);
        }

        @Override
        public Map<String, IValue> put(String key, Map<String, IValue> value)
        {
            return this.liveRecord.subMaps.put(key, value);
        }

        @Override
        public Map<String, IValue> remove(Object key)
        {
            return this.liveRecord.subMaps.remove(key);
        }

        @Override
        public void putAll(Map<? extends String, ? extends Map<String, IValue>> m)
        {
            this.liveRecord.subMaps.putAll(m);
        }

        @Override
        public void clear()
        {
            this.liveRecord.subMaps.clear();
        }

        @Override
        public Set<String> keySet()
        {
            return this.liveRecord.subMaps.keySet();
        }

        @Override
        public Collection<Map<String, IValue>> values()
        {
            return this.liveRecord.subMaps.values();
        }

        @Override
        public Set<java.util.Map.Entry<String, Map<String, IValue>>> entrySet()
        {
            return this.liveRecord.subMaps.entrySet();
        }

        @Override
        public boolean equals(Object o)
        {
            return this.liveRecord.subMaps.equals(o);
        }

        @Override
        public int hashCode()
        {
            return this.liveRecord.subMaps.hashCode();
        }
    }

    /**
     * Create an {@link ImmutableRecord} instance that is backed by a live {@link Record} instance.
     * Changes in the record will be visible to the immutable instance.
     * 
     * @deprecated to be removed
     */
    @Deprecated
    static ImmutableRecord liveImage(Record template)
    {
        return new ImmutableRecord(template);
    }

    /**
     * Create a snapshot of the {@link IRecord} as the source for a new {@link ImmutableRecord}
     * instance.
     * <p>
     * This is needed if the template is a 'live' instance that was created via
     * {@link #liveImage(Record)}. Calling this snapshot method creates a snapshot of this 'live'
     * immutable image.
     */
    public static ImmutableRecord snapshot(IRecord template)
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
    ImmutableRecord(String name, String contextName, AtomicLong sequence, Map<String, IValue> data,
        Map<String, Map<String, IValue>> subMaps, Lock writeLock)
    {
        super();
        this.name = name;
        this.sequence = sequence;
        this.contextName = contextName;
        this.data = data;
        this.subMaps = subMaps;
        this.writeLock = writeLock;
    }

    /**
     * Construct an immutable record backed by a record. Changes made to the backing record are
     * visible via the immutable instance.
     */
    ImmutableRecord(Record template)
    {
        this(template.getName(), template.getContextName(), template.sequence, template.data, new RecordSubMapAccessor(
            template), template.getWriteLock());
    }

    @Override
    public int size()
    {
        return this.data.size();
    }

    @Override
    public boolean isEmpty()
    {
        return this.data.isEmpty();
    }

    @Override
    public boolean containsKey(Object key)
    {
        return this.data.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value)
    {
        return this.data.containsValue(value);
    }

    @Override
    public IValue get(Object key)
    {
        return this.data.get(key);
    }

    @Override
    public IValue put(String key, IValue value)
    {
        throw new UnsupportedOperationException("Cannot call on immutable record " + this.contextName + ":" + this.name);
    }

    @Override
    public IValue remove(Object key)
    {
        throw new UnsupportedOperationException("Cannot call on immutable record " + this.contextName + ":" + this.name);
    }

    @Override
    public void putAll(Map<? extends String, ? extends IValue> m)
    {
        throw new UnsupportedOperationException("Cannot call on immutable record " + this.contextName + ":" + this.name);
    }

    @Override
    public void clear()
    {
        throw new UnsupportedOperationException("Cannot call on immutable record " + this.contextName + ":" + this.name);
    }

    @Override
    public Set<String> keySet()
    {
        return Collections.unmodifiableSet(this.data.keySet());
    }

    @Override
    public Collection<IValue> values()
    {
        return Collections.unmodifiableCollection(this.data.values());
    }

    @Override
    public Set<java.util.Map.Entry<String, IValue>> entrySet()
    {
        return Collections.unmodifiableSet(this.data.entrySet());
    }

    @Override
    public boolean equals(Object o)
    {
        if (is.same(o, this))
        {
            return true;
        }
        if (!(o instanceof IRecord))
        {
            return false;
        }
        if (o instanceof ImmutableRecord)
        {
            ImmutableRecord other = (ImmutableRecord) o;
            return is.eq(this.name, other.name) && is.eq(this.contextName, other.contextName)
                && is.eq(this.data, other.data) && is.eq(this.subMaps, other.subMaps);
        }
        else
        {
            Record other = (Record) o;
            return is.eq(this.name, other.getName()) && is.eq(this.contextName, other.getContextName())
                && is.eq(this.data, other.data) && is.eq(this.subMaps, other.subMaps);
        }
    }

    @Override
    public int hashCode()
    {
        return this.name.hashCode();
    }

    @Override
    public String getName()
    {
        return this.name;
    }

    @Override
    public String getContextName()
    {
        return this.contextName;
    }

    @Override
    public IValue put(String key, long value)
    {
        throw new UnsupportedOperationException("Cannot call on immutable record " + this.contextName + ":" + this.name);
    }

    @Override
    public IValue put(String key, double value)
    {
        throw new UnsupportedOperationException("Cannot call on immutable record " + this.contextName + ":" + this.name);
    }

    @Override
    public IValue put(String key, String value)
    {
        throw new UnsupportedOperationException("Cannot call on immutable record " + this.contextName + ":" + this.name);
    }

    @Override
    public Set<String> getSubMapKeys()
    {
        return Collections.unmodifiableSet(this.subMaps.keySet());
    }

    @Override
    public Map<String, IValue> getOrCreateSubMap(String subMapKey)
    {
        final Map<String, IValue> subMap = this.subMaps.get(subMapKey);
        if (subMap == null)
        {
            return ContextUtils.EMPTY_MAP;
        }
        return Collections.unmodifiableMap(subMap);
    }

    @Override
    public Map<String, IValue> removeSubMap(String subMapKey)
    {
        throw new UnsupportedOperationException("Cannot call on immutable record " + this.contextName + ":" + this.name);
    }

    @Override
    public String toString()
    {
        return Record.toString("(Immutable)" + this.contextName, this.name, this.sequence.longValue(), this.data,
            this.subMaps);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends IValue> T get(String key)
    {
        return (T) this.data.get(key);
    }

    @Override
    public void resolveFromStream(Reader reader) throws IOException
    {
        throw new UnsupportedOperationException("Cannot resolve immutable record from a stream " + this.contextName
            + ":" + this.name);
    }

    @Override
    public void serializeToStream(Writer writer) throws IOException
    {
        ContextUtils.serializeRecordMapToStream(writer, asFlattenedMap());
    }

    @Override
    public Lock getWriteLock()
    {
        return this.writeLock;
    }

    @Override
    public IRecord getImmutableInstance()
    {
        return this;
    }

    @Override
    public Map<String, IValue> asFlattenedMap()
    {
        return ContextUtils.mergeMaps(this.data, this.subMaps);
    }

    @Override
    public long getSequence()
    {
        return this.sequence.longValue();
    }
}
