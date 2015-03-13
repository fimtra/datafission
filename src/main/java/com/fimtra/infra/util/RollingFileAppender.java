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

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.FileFilter;
import java.io.FileWriter;
import java.io.Flushable;
import java.io.IOException;
import java.io.Writer;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * An {@link Appendable} implementation that writes to a {@link File} and will roll the file when
 * the allotted number of characters has been written. The rolling convention is to name the current
 * file:
 * 
 * <pre>
 * {filename}.{counter}.logged
 * </pre>
 * 
 * and then create a new file called {filename}. This all occurs in the same directory as the
 * original file.
 * <p>
 * Each time a file is rolled, any files older than the specified number of time units and that
 * start with the specified prefix are deleted.
 * <p>
 * <b>This is not thread safe</b>
 * 
 * @author Ramon Servadei
 */
public final class RollingFileAppender implements Appendable, Closeable, Flushable
{
    final static Executor DELETE_EXECUTOR = ThreadUtils.newSingleThreadExecutorService("RollingFileAppender-delete");

    /**
     * Create a standard {@link RollingFileAppender} allowing 1M per file, deleting older than 1
     * day.
     */
    public static RollingFileAppender createStandardRollingFileAppender(String fileIdentity, String directory)
    {
        final File fileDirectory = new File(directory);
        fileDirectory.mkdir();
        String yyyyMMddHHmmssSSS = new FastDateFormat().yyyyMMddHHmmssSSS(System.currentTimeMillis());
        yyyyMMddHHmmssSSS = yyyyMMddHHmmssSSS.replace(":", "");
        yyyyMMddHHmmssSSS = yyyyMMddHHmmssSSS.replace("-", "_");
        yyyyMMddHHmmssSSS = yyyyMMddHHmmssSSS.substring(0, 15);
        final String filePrefix = ThreadUtils.getMainMethodClassSimpleName() + "-" + fileIdentity;
        final File file = new File(fileDirectory, filePrefix + "_" + yyyyMMddHHmmssSSS + ".log");
        RollingFileAppender temp = null;
        try
        {
            temp = new RollingFileAppender(file, 1024 * 1024, TimeUnit.DAYS, 1, filePrefix);
        }
        catch (IOException e)
        {
            System.err.println("Could not create file: " + file);
            e.printStackTrace();
            System.exit(101);
        }
        return temp;
    }

    private static void checkFileWriteable(File file) throws IOException
    {
        if (!file.exists() && !file.createNewFile())
        {
            throw new IOException("Could not create file: " + file);
        }
        if (!file.canWrite())
        {
            throw new IOException("Cannot write to file: " + file);
        }
    }

    private static void deleteOldLogFiles(final File logFile, final TimeUnit olderThanTimeUnit,
        final int olderThanTimeUnitNumber, final String prefixToMatchWhenDeleting)
    {
        DELETE_EXECUTOR.execute(new Runnable()
        {
            @Override
            public void run()
            {
                File[] toDelete = FileUtils.readFiles(logFile.getAbsoluteFile().getParentFile(), new FileFilter()
                {
                    @Override
                    public boolean accept(File file)
                    {
                        final long oneDayAgo =
                            System.currentTimeMillis() - olderThanTimeUnit.toMillis(olderThanTimeUnitNumber);
                        if (file.isFile() && file.lastModified() < oneDayAgo
                            && file.getName().startsWith(prefixToMatchWhenDeleting))
                        {
                            return true;
                        }
                        return false;
                    }
                });
                for (File file : toDelete)
                {
                    Log.log(RollingFileAppender.class, "DELETING ", ObjectUtils.safeToString(file));
                    try
                    {
                        file.delete();
                    }
                    catch (Exception e)
                    {
                        Log.log(RollingFileAppender.class, "ERROR DELETING " + file, e);
                    }
                }
            }
        });
    }

    private final String prefixToMatchWhenDeleting;
    private final int olderThanTimeUnitNumber;
    private final TimeUnit olderThanTimeUnit;
    private int currentCharCount;
    private final int maxChars;
    private File currentFile;
    private int rollCount;
    private Writer writer;

    /**
     * Construct an instance writing to the given file.
     * 
     * @param file
     *            the file to write to
     * @param maximumCharacters
     *            the maximum number of characters to write to the file before rolling to a new file
     * @param olderThanTimeUnit
     *            the time unit for identifying files to delete
     * @param olderThanTimeUnitNumber
     *            the number of time units the last-modified time of a file must exceed to be
     *            deleted
     * @param prefixToMatchWhenDeleting
     *            the prefix to match files in the same directory as the log file when deleting
     * @throws IOException
     */
    public RollingFileAppender(File file, int maximumCharacters, final TimeUnit olderThanTimeUnit,
        final int olderThanTimeUnitNumber, String prefixToMatchWhenDeleting) throws IOException
    {
        if (maximumCharacters <= 0)
        {
            throw new IOException("Cannot have negative or 0 maximum characters");
        }
        this.currentFile = file;
        this.prefixToMatchWhenDeleting = prefixToMatchWhenDeleting;
        this.olderThanTimeUnit = olderThanTimeUnit;
        this.olderThanTimeUnitNumber = olderThanTimeUnitNumber;
        checkFileWriteable(this.currentFile);
        deleteOldLogFiles(this.currentFile, this.olderThanTimeUnit, this.olderThanTimeUnitNumber,
            this.prefixToMatchWhenDeleting);
        this.maxChars = maximumCharacters;
        this.writer = new BufferedWriter(new FileWriter(file));
    }

    @Override
    public String toString()
    {
        try
        {
            return this.currentFile.getCanonicalPath();
        }
        catch (IOException e)
        {
            return this.currentFile.toString();
        }
    }

    @Override
    public Appendable append(CharSequence csq) throws IOException
    {
        checkSize(csq.length());
        this.writer.append(csq);
        return this;
    }

    @Override
    public Appendable append(CharSequence csq, int start, int end) throws IOException
    {
        checkSize(end - start);
        this.writer.append(csq, start, end);
        return this;
    }

    @Override
    public Appendable append(char c) throws IOException
    {
        checkSize(1);
        this.writer.append(c);
        return this;
    }

    @Override
    public void flush() throws IOException
    {
        this.writer.flush();
    }

    @Override
    public void close() throws IOException
    {
        this.writer.close();
    }

    private void checkSize(int charCount) throws IOException
    {
        this.currentCharCount += charCount;

        if (this.currentCharCount >= this.maxChars)
        {
            this.writer.flush();
            this.writer.close();
            // java7
            // Files.move(this.currentFile.toPath(), new File(this.currentFile.getParent(),
            // this.currentFile.getName()
            // + "." + this.rollCount++ + ".logged").toPath(), StandardCopyOption.ATOMIC_MOVE);

            final String name = this.currentFile.getName();
            this.currentFile.renameTo(new File(this.currentFile.getParent(), name + "." + this.rollCount++ + ".logged"));
            this.currentCharCount = charCount;
            this.currentFile = new File(this.currentFile.getParent(), name);
            checkFileWriteable(this.currentFile);
            deleteOldLogFiles(this.currentFile, this.olderThanTimeUnit, this.olderThanTimeUnitNumber,
                this.prefixToMatchWhenDeleting);
            this.writer = new BufferedWriter(new FileWriter(this.currentFile));
        }
    }
}
