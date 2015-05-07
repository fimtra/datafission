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
package com.fimtra.util;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This holds {@link ReentrantLock} objects against a string
 * 
 * @author Ramon Servadei
 */
public final class Locks
{
    private final static Locks staticLocks = new Locks();

    /**
     * Get a lock that instance mapped to the name, creating it if necessary. This lock can be
     * accessed by any code in the runtime.
     * 
     * @param name
     *            the name of the lock to obtain, <b>MUST NOT BE NULL</b>
     * @return the runtime lock for this name
     * @throws NullPointerException
     *             if the name is <code>null</code>
     */
    public static Lock getRuntimeLock(String name)
    {
        return staticLocks.getLock(name);
    }

    private final ConcurrentMap<String, Lock> locks;
    private final Lock lock;

    public Locks()
    {
		this.locks = new ConcurrentHashMap<String, Lock>(2);
        this.lock = new ReentrantLock();
    }

    /**
     * Get a lock instance mapped to the name, creating it if necessary
     * 
     * @param name
     *            the name of the lock to obtain, <b>MUST NOT BE NULL</b>
     * @return the lock for this name
     * @throws NullPointerException
     *             if the name is <code>null</code>
     */
    public Lock getLock(String name)
    {
        final Lock namedLock = this.locks.get(name);
        if (namedLock != null)
        {
            return namedLock;
        }
        this.lock.lock();
        try
        {
            final ReentrantLock newLock = new ReentrantLock();
            final Lock existingLock = this.locks.putIfAbsent(name, newLock);
            return existingLock == null ? newLock : existingLock;
        }
        finally
        {
            this.lock.unlock();
        }
    }

    /**
     * Remove the lock held against this name. This method should be called when the lock will no
     * longer be needed by any other code.
     * 
     * @param name
     *            the name of the lock to remove.
     */
    public void removeLock(String name)
    {
        this.locks.remove(name);
    }
}
