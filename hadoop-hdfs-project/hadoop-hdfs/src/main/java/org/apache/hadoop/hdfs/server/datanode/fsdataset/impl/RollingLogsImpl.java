/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hdfs.server.datanode.DataBlockScanner;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.RollingLogs;

import com.google.common.base.Charsets;

class RollingLogsImpl implements RollingLogs {
  private static final String CURR_SUFFIX = ".curr";
  private static final String PREV_SUFFIX = ".prev";

  static boolean isFilePresent(String dir, String filePrefix) {
    return new File(dir, filePrefix + CURR_SUFFIX).exists() ||
           new File(dir, filePrefix + PREV_SUFFIX).exists();
  }

  private final File curr;
  private final File prev;
  private PrintWriter out; //require synchronized access

  private final Appender appender = new Appender() {
    @Override
    public Appendable append(CharSequence csq) {
      synchronized(RollingLogsImpl.this) {
        if (out == null) {
          throw new IllegalStateException(RollingLogsImpl.this
              + " is not yet opened.");
        }
        out.print(csq);
        out.flush();
      }
      return this;
    }

    @Override
    public Appendable append(char c) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Appendable append(CharSequence csq, int start, int end) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
      synchronized(RollingLogsImpl.this) {
        if (out != null) {
          out.close();
          out = null;
        }
      }
    }
  };


  private final AtomicInteger numReaders = new AtomicInteger();

  RollingLogsImpl(String dir, String filePrefix) throws FileNotFoundException{
    curr = new File(dir, filePrefix + CURR_SUFFIX);
    prev = new File(dir, filePrefix + PREV_SUFFIX);
    out = new PrintWriter(new OutputStreamWriter(new FileOutputStream(
        curr, true), Charsets.UTF_8));
  }

  @Override
  public Reader iterator(boolean skipPrevFile) throws IOException {
    numReaders.incrementAndGet(); 
    return new Reader(skipPrevFile);
  }

  @Override
  public Appender appender() {
    return appender;
  }

  @Override
  public boolean roll() throws IOException {
    if (numReaders.get() > 0) {
      return false;
    }
    if (!prev.delete() && prev.exists()) {
      throw new IOException("Failed to delete " + prev);
    }

    synchronized(this) {
      appender.close();
      final boolean renamed = curr.renameTo(prev);
      out = new PrintWriter(new OutputStreamWriter(new FileOutputStream(
          curr, true), Charsets.UTF_8));
      if (!renamed) {
        throw new IOException("Failed to rename " + curr + " to " + prev);
      }
    }
    return true;
  }

  @Override
  public String toString() {
    return curr.toString();
  }
  
  /**
   * This is used to read the lines in order.
   * If the data is not read completely (i.e, untill hasNext() returns
   * false), it needs to be explicitly 
   */
  private class Reader implements RollingLogs.LineIterator {
    private File file;
    private File lastReadFile;
    private BufferedReader reader;
    private String line;
    private boolean closed = false;
    
    private Reader(boolean skipPrevFile) throws IOException {
      reader = null;
      file = skipPrevFile? curr : prev;
      readNext();        
    }

    @Override
    public boolean isPrevious() {
      return file == prev;
    }

    @Override
    public boolean isLastReadFromPrevious() {
      return lastReadFile == prev;
    }

    private boolean openFile() throws IOException {

      for(int i=0; i<2; i++) {
        if (reader != null || i > 0) {
          // move to next file
          file = isPrevious()? curr : null;
        }
        if (file == null) {
          return false;
        }
        if (file.exists()) {
          break;
        }
      }
      
      if (reader != null ) {
        reader.close();
        reader = null;
      }
      
      reader = new BufferedReader(new InputStreamReader(new FileInputStream(
          file), Charsets.UTF_8));
      return true;
    }
    
    // read next line if possible.
    private void readNext() throws IOException {
      line = null;
      try {
        if (reader != null && (line = reader.readLine()) != null) {
          return;
        }
        // move to the next file.
        if (openFile()) {
          readNext();
        }
      } finally {
        if (!hasNext()) {
          close();
        }
      }
    }
    
    @Override
    public boolean hasNext() {
      return line != null;
    }

    @Override
    public String next() {
      String curLine = line;
      try {
        lastReadFile = file;
        readNext();
      } catch (IOException e) {
        DataBlockScanner.LOG.warn("Failed to read next line.", e);
      }
      return curLine;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {
      if (!closed) {
        try {
          if (reader != null) {
            reader.close();
          }
        } finally {
          file = null;
          reader = null;
          closed = true;
          final int n = numReaders.decrementAndGet();
          assert(n >= 0);
        }
      }
    }
  }
}