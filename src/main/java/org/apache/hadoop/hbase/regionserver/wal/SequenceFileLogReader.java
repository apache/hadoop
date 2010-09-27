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

import java.io.EOFException;
import java.io.IOException;
import java.lang.Class;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.mortbay.log.Log;

public class SequenceFileLogReader implements HLog.Reader {

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

      @Override
      public long getPos() throws IOException {
        if (this.firstGetPosInvocation) {
          this.firstGetPosInvocation = false;
          // Tell a lie.  We're doing this just so that this line up in
          // SequenceFile.Reader constructor comes out with the correct length
          // on the file:
          //         this.end = in.getPos() + length;
          long available = this.in.available();
          // Length gets added up in the SF.Reader constructor so subtract the
          // difference.  If available < this.length, then return this.length.
          return available >= this.length? available - this.length: this.length;
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
      Log.warn("Failed getting position to add to throw", e);
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
