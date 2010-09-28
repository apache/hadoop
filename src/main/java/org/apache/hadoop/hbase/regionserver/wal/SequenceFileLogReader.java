/**
 * Copyright 2010 The Apache Software Foundation
 *
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

package org.apache.hadoop.hbase.regionserver.wal;

import java.io.FilterInputStream;
import java.io.IOException;
import java.lang.Class;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
 
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;

public class SequenceFileLogReader implements HLog.Reader {
  private static final Log LOG = LogFactory.getLog(SequenceFileLogReader.class);

  /**
   * Hack just to set the correct file length up in SequenceFile.Reader.
   * See HADOOP-6307.  The below is all about setting the right length on the
   * file we are reading.  fs.getFileStatus(file).getLen() is passed down to
   * a private SequenceFile.Reader constructor.  This won't work.  Need to do
   * the available on the stream.  The below is ugly.  It makes getPos, the
   * first time its called, return length of the file -- i.e. tell a lie -- just
   * so this line up in SF.Reader's constructor ends up with right answer:
   *
   *         this.end = in.getPos() + length;
   *
   */
  private static class WALReader extends SequenceFile.Reader {

    WALReader(final FileSystem fs, final Path p, final Configuration c)
    throws IOException {
      super(fs, p, c);

    }

    @Override
    protected FSDataInputStream openFile(FileSystem fs, Path file,
      int bufferSize, long length)
    throws IOException {
      return new WALReaderFSDataInputStream(super.openFile(fs, file,
        bufferSize, length), length);
    }

    /**
     * Override just so can intercept first call to getPos.
     */
    static class WALReaderFSDataInputStream extends FSDataInputStream {
      private boolean firstGetPosInvocation = true;
      private long length;

      WALReaderFSDataInputStream(final FSDataInputStream is, final long l)
      throws IOException {
        super(is);
        this.length = l;
      }

      // This section can be confusing.  It is specific to how HDFS works.
      // Let me try to break it down.  This is the problem:
      //
      //  1. HDFS DataNodes update the NameNode about a filename's length 
      //     on block boundaries or when a file is closed. Therefore, 
      //     if an RS dies, then the NN's fs.getLength() can be out of date
      //  2. this.in.available() would work, but it returns int &
      //     therefore breaks for files > 2GB (happens on big clusters)
      //  3. DFSInputStream.getFileLength() gets the actual length from the DNs
      //  4. DFSInputStream is wrapped 2 levels deep : this.in.in
      //
      // So, here we adjust getPos() using getFileLength() so the
      // SequenceFile.Reader constructor (aka: first invocation) comes out 
      // with the correct end of the file:
      //         this.end = in.getPos() + length;
      @Override
      public long getPos() throws IOException {
        if (this.firstGetPosInvocation) {
          this.firstGetPosInvocation = false;
          long adjust = 0;

          try {
            Field fIn = FilterInputStream.class.getDeclaredField("in");
            fIn.setAccessible(true);
            Object realIn = fIn.get(this.in);
            Method getFileLength = realIn.getClass().
              getMethod("getFileLength", new Class<?> []{});
            getFileLength.setAccessible(true);
            long realLength = ((Long)getFileLength.
              invoke(realIn, new Object []{})).longValue();
            assert(realLength >= this.length);
            adjust = realLength - this.length;
          } catch(Exception e) {
            SequenceFileLogReader.LOG.warn(
              "Error while trying to get accurate file length.  " +
              "Truncation / data loss may occur if RegionServers die.", e);
          }

          return adjust + super.getPos();
        }
        return super.getPos();
      }
    }
  }

  Configuration conf;
  WALReader reader;
  // Needed logging exceptions
  Path path;
  int edit = 0;
  long entryStart = 0;

  private Class<? extends HLogKey> keyClass;

  /**
   * Default constructor.
   */
  public SequenceFileLogReader() {
  }

  /**
   * This constructor allows a specific HLogKey implementation to override that
   * which would otherwise be chosen via configuration property.
   * 
   * @param keyClass
   */
  public SequenceFileLogReader(Class<? extends HLogKey> keyClass) {
    this.keyClass = keyClass;
  }


  @Override
  public void init(FileSystem fs, Path path, Configuration conf)
      throws IOException {
    this.conf = conf;
    this.path = path;
    reader = new WALReader(fs, path, conf);
  }

  @Override
  public void close() throws IOException {
    try {
      reader.close();
    } catch (IOException ioe) {
      throw addFileInfoToException(ioe);
    }
  }

  @Override
  public HLog.Entry next() throws IOException {
    return next(null);
  }

  @Override
  public HLog.Entry next(HLog.Entry reuse) throws IOException {
    this.entryStart = this.reader.getPosition();
    HLog.Entry e = reuse;
    if (e == null) {
      HLogKey key;
      if (keyClass == null) {
        key = HLog.newKey(conf);
      } else {
        try {
          key = keyClass.newInstance();
        } catch (InstantiationException ie) {
          throw new IOException(ie);
        } catch (IllegalAccessException iae) {
          throw new IOException(iae);
        }
      }
      
      WALEdit val = new WALEdit();
      e = new HLog.Entry(key, val);
    }
    boolean b = false;
    try {
      b = this.reader.next(e.getKey(), e.getEdit());
    } catch (IOException ioe) {
      throw addFileInfoToException(ioe);
    }
    edit++;
    return b? e: null;
  }

  @Override
  public void seek(long pos) throws IOException {
    try {
      reader.seek(pos);
    } catch (IOException ioe) {
      throw addFileInfoToException(ioe);
    }
  }

  @Override
  public long getPosition() throws IOException {
    return reader.getPosition();
  }

  private IOException addFileInfoToException(final IOException ioe)
  throws IOException {
    long pos = -1;
    try {
      pos = getPosition();
    } catch (IOException e) {
      LOG.warn("Failed getting position to add to throw", e);
    }

    // See what SequenceFile.Reader thinks is the end of the file
    long end = Long.MAX_VALUE;
    try {
      Field fEnd = SequenceFile.Reader.class.getDeclaredField("end");
      fEnd.setAccessible(true);
      end = fEnd.getLong(this.reader);
    } catch(Exception e) { /* reflection fail. keep going */ }

    String msg = (this.path == null? "": this.path.toString()) +
      ", entryStart=" + entryStart + ", pos=" + pos + 
      ((end == Long.MAX_VALUE) ? "" : ", end=" + end) + 
      ", edit=" + this.edit;

    // Enhance via reflection so we don't change the original class type
    try {
      return (IOException) ioe.getClass()
        .getConstructor(String.class)
        .newInstance(msg)
        .initCause(ioe);
    } catch(Exception e) { /* reflection fail. keep going */ }
    
    return ioe;
  }
}
