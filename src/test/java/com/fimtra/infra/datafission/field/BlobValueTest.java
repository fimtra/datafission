/*
 * Copyright (c) 2014 Ramon Servadei 
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
package com.fimtra.infra.datafission.field;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.junit.Before;
import org.junit.Test;

import com.fimtra.infra.datafission.IValue.TypeEnum;

/**
 * Tests for the {@link BlobValue}
 * 
 * @author Ramon Servadei
 */
public class BlobValueTest
{

    private static final String _1AF3416 = "1a0f3416";
    BlobValue candidate;
    byte[] bytes = new byte[] { 0x1a, 0xf, 0x34, 0x16 };

    @Before
    public void setUp() throws Exception
    {
        this.candidate = new BlobValue(this.bytes);
        // candidate = new BlobValue("a);sldkfnjmq;sloedj;lwkejr;oai3wjr;lakejfs;lj!".getBytes());
    }

    @Test
    public void testObjectToFromBlob()
    {
        assertNotNull(BlobValue.toBlob(null));
        assertNull(BlobValue.fromBlob(null));
        String s = "Lasers";
        assertEquals(s, BlobValue.fromBlob(BlobValue.toBlob(s)));
        assertNull(BlobValue.fromBlob(BlobValue.toBlob(null)));
    }

    @Test
    public void testEquals()
    {
        assertEquals(new BlobValue(this.bytes), new BlobValue(this.bytes));
        assertFalse(new BlobValue().equals(new BlobValue(this.bytes)));
    }

    @Test
    public void testFromString()
    {
        final BlobValue other = new BlobValue();
        other.fromString(_1AF3416);
        assertEquals(other, this.candidate);
    }

    @Test
    public void testFromChar()
    {
        final BlobValue other = new BlobValue();
        char[] charArray = _1AF3416.toCharArray();
        other.fromChars(charArray, 0, charArray.length);
        assertEquals(other, this.candidate);
    }

    @Test
    public void testGetType()
    {
        assertEquals(TypeEnum.BLOB, this.candidate.getType());
    }

    @Test
    public void testLongValue()
    {
        assertEquals(4, this.candidate.longValue());
    }

    @Test
    public void testDoubleValue()
    {
        assertEquals(4, this.candidate.doubleValue(), 1.0);
    }

    @Test
    public void testTextValue()
    {
        assertEquals(_1AF3416, this.candidate.textValue());
    }

    @Test
    public void testSmallToFromString() throws Exception
    {
        final byte[] bytes = new byte[] { 0xf, 0x0, 0x1 };
        this.candidate = new BlobValue(bytes);
        BlobValue other = new BlobValue();
        other.fromString(this.candidate.textValue());
        assertEquals(this.candidate, other);
    }

    @Test
    public void testFullByteRangeToFromString() throws Exception
    {
        final byte[] bytes = new byte[256];
        int i = 0;
        for (int v = -128; v < 128; v++)
        {
            bytes[i++] = (byte) v;
        }
        this.candidate = new BlobValue(bytes);
        BlobValue other = new BlobValue();
        other.fromString(this.candidate.textValue());
        assertEquals(this.candidate, other);
    }
}
