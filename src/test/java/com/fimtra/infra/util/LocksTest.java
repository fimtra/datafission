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
package com.fimtra.infra.util;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

import java.util.concurrent.locks.Lock;

import org.junit.Before;
import org.junit.Test;

/**
 * Tests for {@link Locks}
 * 
 * @author Ramon Servadei
 */
public class LocksTest
{
    Locks candidate;

    @Before
    public void setUp() throws Exception
    {
        this.candidate = new Locks();
    }

    @Test
    public void testGetRuntimeLock()
    {
        Lock lock1 = Locks.getRuntimeLock("lock1");
        Lock lock2 = Locks.getRuntimeLock("lock1");
        Lock lock3 = Locks.getRuntimeLock("lock2");
        assertNotNull(lock1);
        assertNotNull(lock2);
        assertNotNull(lock3);
        assertSame(lock1, lock2);
        assertNotSame(lock1, lock3);
    }

    @Test(expected = NullPointerException.class)
    public void testGetRuntimeLockNull()
    {
        Locks.getRuntimeLock(null);
    }

    @Test
    public void testGetLock()
    {
        Lock lock1 = this.candidate.getLock("lock1");
        Lock lock2 = this.candidate.getLock("lock1");
        Lock lock3 = this.candidate.getLock("lock3");
        assertNotNull(lock1);
        assertNotNull(lock2);
        assertNotNull(lock3);
        assertSame(lock1, lock2);
        assertNotSame(lock1, lock3);
    }

    @Test
    public void testGetLockAcrossInstances()
    {
        Lock lock1 = this.candidate.getLock("lock1");
        Lock lock2 = new Locks().getLock("lock1");
        assertNotNull(lock1);
        assertNotNull(lock2);
        assertNotSame(lock1, lock2);
    }

    @Test(expected = NullPointerException.class)
    public void testGetLockNull()
    {
        this.candidate.getLock(null);
    }

    @Test
    public void testRemoveLock()
    {
        Lock lock1 = this.candidate.getLock("lock1");
        assertNotNull(lock1);
        this.candidate.removeLock("lock1");
        Lock lock1_2 = this.candidate.getLock("lock1");
        assertNotNull(lock1);
        assertNotNull(lock1_2);
        assertNotSame(lock1, lock1_2);
    }

}
