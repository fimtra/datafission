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

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests the {@link RollingFileAppender}
 * 
 * @author Ramon Servadei
 */
public class RollingFileAppenderTest
{
    private static final String FILENAME = "RollingFileAppenderTest.log";
    final static File file = new File(FILENAME);
    RollingFileAppender candidate;

    @Before
    public void setUp() throws Exception
    {
        deleteLogged();
        this.candidate = new RollingFileAppender(file, 10, TimeUnit.HOURS, 1, "RollingFileAppenderTest");
    }

    @After
    public void tearDown() throws Exception
    {
        deleteLogged();
    }

    static void deleteLogged()
    {
        new File(FILENAME).delete();
        final File[] files = FileUtils.readFiles(new File("."), new FileUtils.ExtensionFileFilter("logged"));
        for (File file : files)
        {
            file.delete();
        }
    }

    @Test
    public void testAppendCharSequence() throws IOException
    {
        this.candidate.append("hello").append(" this is ").append(" some text").append('\n').append(
            "This is some more text", 13, 17).append("This is some more text", 17, 22);
        this.candidate.flush();

        final File[] files = FileUtils.readFiles(new File("."), new FileUtils.ExtensionFileFilter("logged"));
        assertEquals(4, files.length);
    }
}
