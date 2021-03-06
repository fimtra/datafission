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
package com.fimtra.datafission.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.fimtra.datafission.ICodec;
import com.fimtra.datafission.IRecordChange;
import com.fimtra.datafission.IValue;
import com.fimtra.datafission.core.AtomicChange;
import com.fimtra.datafission.core.RpcInstance;
import com.fimtra.datafission.core.StringProtocolCodec;
import com.fimtra.datafission.field.LongValue;
import com.fimtra.datafission.field.TextValue;

/**
 * Tests for the {@link StringProtocolCodec}
 * 
 * @author Ramon Servadei
 */
public class StringProtocolCodecTest extends CodecBaseTest
{
    String name = "myName";

    @Override
    ICodec<?> constructCandidate()
    {
        return new StringProtocolCodec();
    }

    @Test
    public void testEscapeUnescape()
    {
        String value = "some value \\|| with | delimiters \\/ |\\ |/";
        StringBuilder sb = new StringBuilder();
        StringProtocolCodec.escape(value, sb);
        String unescape = StringProtocolCodec.stringFromCharBuffer(sb.toString().toCharArray());
        assertEquals(value, unescape);
    }

    @Test
    public void testEscapeUnescapeLength()
    {
        String value = "||||||||";
        StringBuilder sb = new StringBuilder();
        StringProtocolCodec.escape(value, sb);
        String unescape = StringProtocolCodec.stringFromCharBuffer(sb.toString().toCharArray());
        assertEquals(value, unescape);
    }

    @Test
    public void testEscapeUnescapeEndingInSpecialChar()
    {
        String value = "special char ending \\";
        StringBuilder sb = new StringBuilder();
        StringProtocolCodec.escape(value, sb);
        String unescape = StringProtocolCodec.stringFromCharBuffer(sb.toString().toCharArray());
        assertEquals(value, unescape);
    }

    @Test
    public void testEscapeUnescapeCRLF()
    {
        String value = "some value \\|| with \r\n | delimiters \\/ |\\ |/";
        StringBuilder sb = new StringBuilder();
        StringProtocolCodec.escape(value, sb);
        String escaped = sb.toString();
        assertFalse(escaped.contains("\r"));
        assertFalse(escaped.contains("\n"));
        String unescape = StringProtocolCodec.stringFromCharBuffer(escaped.toString().toCharArray());
        assertEquals(value, unescape);
    }

    @Test
    public void testStringWithEscapedCRLF()
    {
        String value = "some value \\|| with \\r\\n | delimiters \\/ |\\ |/";
        StringBuilder sb = new StringBuilder();
        StringProtocolCodec.escape(value, sb);
        String escaped = sb.toString();
        String unescape = StringProtocolCodec.stringFromCharBuffer(escaped.toString().toCharArray());
        assertEquals(value, unescape);
    }

    @Test
    public void testEncodeDecodeValue()
    {
        char[] chars = StringProtocolCodec.encodeValue(null).toString().toCharArray();
        IValue decodeValue = StringProtocolCodec.decodeValue(chars, 0, chars.length);
        assertNull("got: " + decodeValue, decodeValue);
    }

    @Test
    public void testEncodeDecodeValueWithTextValueUsingSpecialChar()
    {
        TextValue value = new TextValue(StringProtocolCodec.NULL_VALUE);
        char[] chars = StringProtocolCodec.encodeValue(value).toString().toCharArray();
        IValue decodeValue = StringProtocolCodec.decodeValue(chars, 0, chars.length);
        assertEquals(value, decodeValue);
    }

    @Test
    public void testEncodeDecodeValueWithTextValue()
    {
        TextValue value = new TextValue("");
        char[] chars = StringProtocolCodec.encodeValue(value).toString().toCharArray();
        IValue decodeValue = StringProtocolCodec.decodeValue(chars, 0, chars.length);
        assertEquals(value, decodeValue);
    }

    @Test
    public void testGetCommandMessageForRecordNames()
    {
        String[] args = new String[] { "one", "two", "three", "|\\|\\||special" };
        List<String> result =
            StringProtocolCodec.getNamesFromCommandMessage(StringProtocolCodec.getEncodedNamesForCommandMessage(
                StringProtocolCodec.SUBSCRIBE_COMMAND, args).toCharArray());
        assertEquals(Arrays.toString(args), Arrays.toString(result.toArray(new String[result.size()])));
    }

    @Test
    public void testEncodeDecodeAtomicChange()
    {
        final String k1 = "one";
        final String k2 = "two";
        final String k3 = "three";
        final String k4 = "four";
        final LongValue v1 = LongValue.valueOf(1);
        final LongValue v2 = LongValue.valueOf(2);
        final LongValue v3 = LongValue.valueOf(3);
        final TextValue v4 = TextValue.valueOf("0123456789-10-0123456789-20-0123456789-30-0123456789-40-0123456789");

        AtomicChange change = new AtomicChange("change");
        change.mergeEntryUpdatedChange(k1, v1, null);
        change.mergeEntryUpdatedChange(k2, v2, null);
        change.mergeEntryRemovedChange(k3, v3);
        change.mergeEntryRemovedChange(k4, v4);
        change.mergeSubMapEntryUpdatedChange("subMap1", k1, v1, null);
        change.mergeSubMapEntryUpdatedChange("subMap1", k2, v2, null);
        change.mergeSubMapEntryRemovedChange("subMap1", k3, v3);
        change.mergeSubMapEntryRemovedChange("subMap1", k4, v4);

        IRecordChange result =
            StringProtocolCodec.decodeAtomicChange(new String(StringProtocolCodec.encodeAtomicChange("|", change)).toCharArray());

        assertEquals(change.toString(), result.toString());
    }

    @Test
    public void testEncodeDecodeAtomicChange_preamble()
    {
        final String k1 = "one";
        final String k2 = "two";
        final String k3 = "three";
        final String k4 = "four";
        final LongValue v1 = LongValue.valueOf(1);
        final LongValue v2 = LongValue.valueOf(2);
        final LongValue v3 = LongValue.valueOf(3);
        final TextValue v4 = TextValue.valueOf("0123456789-10-0123456789-20-0123456789-30-0123456789-40-0123456789");

        AtomicChange change = new AtomicChange("change");
        change.mergeEntryUpdatedChange(k1, v1, null);
        change.mergeEntryUpdatedChange(k2, v2, null);
        change.mergeEntryRemovedChange(k3, v3);
        change.mergeEntryRemovedChange(k4, v4);
        change.mergeSubMapEntryUpdatedChange("subMap1", k1, v1, null);
        change.mergeSubMapEntryUpdatedChange("subMap1", k2, v2, null);
        change.mergeSubMapEntryRemovedChange("subMap1", k3, v3);
        change.mergeSubMapEntryRemovedChange("subMap1", k4, v4);

        IRecordChange result =
            StringProtocolCodec.decodeAtomicChange(
                new String(StringProtocolCodec.encodeAtomicChange(StringProtocolCodec.RPC_COMMAND, change)).toCharArray());

        assertEquals(change, result);
    }

    @Test
    public void testEncodeDecodeAtomicChange_preamble_noArgs()
    {
        byte[] txMessageForRpc =
            this.candidate.getTxMessageForRpc("rpcException", new IValue[0],
                "_RPC_rpcException:304189752:1405541086282:1");
        IRecordChange atomicChangeFromRxMessage = this.candidate.getAtomicChangeFromRxMessage(txMessageForRpc);
        System.err.println(atomicChangeFromRxMessage);
        assertEquals("_RPC_rpcException:304189752:1405541086282:1",
            atomicChangeFromRxMessage.getPutEntries().get(RpcInstance.Remote.RESULT_RECORD_NAME).textValue());
    }

    @Test
    public void testPerformanceOfIndexAndBulkBuffer()
    {
        StringBuilder sb = new StringBuilder();
        String valueToSend = "0123456789";

        int MAX = 10000;
        for (int i = 0; i < MAX; i++)
        {
            dobulk(sb, valueToSend);
            doswitch(sb, valueToSend);
        }

        long indexThenBulkAdd = 0;
        long bulkStart = System.nanoTime();
        for (int i = 0; i < MAX; i++)
        {
            dobulk(sb, valueToSend);
        }
        System.err.println("Bulk=" + (indexThenBulkAdd += (System.nanoTime() - bulkStart)));

        long scanAndSwitch = 0;
        long swStart = System.nanoTime();
        for (int i = 0; i < MAX; i++)
        {
            doswitch(sb, valueToSend);
        }
        System.err.println("Scan=" + (scanAndSwitch += (System.nanoTime() - swStart)));

        bulkStart = System.nanoTime();
        for (int i = 0; i < MAX; i++)
        {
            dobulk(sb, valueToSend);
        }
        System.err.println("Bulk=" + (indexThenBulkAdd += (System.nanoTime() - bulkStart)));

        swStart = System.nanoTime();
        for (int i = 0; i < MAX; i++)
        {
            doswitch(sb, valueToSend);
        }
        System.err.println("Scan=" + (scanAndSwitch += (System.nanoTime() - swStart)));
    }

    void doswitch(StringBuilder sb, String valueToSend)
    {
        sb.setLength(0);
        final char[] chars = valueToSend.toCharArray();
        final int length = chars.length;
        char charAt;
        int last = 0;
        for (int i = 0; i < length; i++)
        {
            charAt = chars[i];
            switch(charAt)
            {
                case '\r':
                    sb.append(chars, last, i - last);
                    sb.append('\\');
                    sb.append('r');
                    last = i + 1;
                    break;
                case '\n':
                    sb.append(chars, last, i - last);
                    sb.append('\\');
                    sb.append('n');
                    last = i + 1;
                    break;
                case '\\':
                case '|':
                case '=':
                    sb.append(chars, last, i - last);
                    sb.append('\\');
                    sb.append(charAt);
                    last = i + 1;
                    break;
                default :
            }
        }
        sb.append(chars, last, length - last);
    }

    void dobulk(StringBuilder sb, String valueToSend)
    {
        sb.setLength(0);
        if ((valueToSend.indexOf('\r', 0) == -1) && (valueToSend.indexOf('\n', 0) == -1)
            && (valueToSend.indexOf('\\', 0) == -1) && (valueToSend.indexOf('|', 0) == -1)
            && (valueToSend.indexOf('=', 0) == -1)

        )
        {
            sb.append(valueToSend);
            return;
        }
    }
}