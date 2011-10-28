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

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Metadata;
import org.apache.hadoop.io.compress.DefaultCodec;

/**
 * Implementation of {@link HLog.Writer} that delegates to
 * SequenceFile.Writer.
 */
public class SequenceFileLogWriter implements HLog.Writer {
  private final Log LOG = LogFactory.getLog(this.getClass());
  // The sequence file we delegate to.
  private SequenceFile.Writer writer;
  // This is the FSDataOutputStream instance that is the 'out' instance
  // in the SequenceFile.Writer 'writer' instance above.
  private FSDataOutputStream writer_out;

  private Class<? extends HLogKey> keyClass;

  private Method syncFs = null;
  private Method hflush = null;

  /**
   * Default constructor.
   */
  public SequenceFileLogWriter() {
    super();
  }

  /**
   * This constructor allows a specific HLogKey implementation to override that
   * which would otherwise be chosen via configuration property.
   * 
   * @param keyClass
   */
  public SequenceFileLogWriter(Class<? extends HLogKey> keyClass) {
    this.keyClass = keyClass;
  }

  @Override
  public void init(FileSystem fs, Path path, Configuration conf)
  throws IOException {

    if (null == keyClass) {
      keyClass = HLog.getKeyClass(conf);
    }

    // Create a SF.Writer instance.
    this.writer = SequenceFile.createWriter(fs, conf, path,
      keyClass, WALEdit.class,
      fs.getConf().getInt("io.file.buffer.size", 4096),
      (short) conf.getInt("hbase.regionserver.hlog.replication",
      fs.getDefaultReplication()),
      conf.getLong("hbase.regionserver.hlog.blocksize",
      fs.getDefaultBlockSize()),
      SequenceFile.CompressionType.NONE,
      new DefaultCodec(),
      null,
      new Metadata());
    
    this.writer_out = getSequenceFilePrivateFSDataOutputStreamAccessible();
    this.syncFs = getSyncFs();
    this.hflush = getHFlush();
    String msg = "Path=" + path +
      ", syncFs=" + (this.syncFs != null) +
      ", hflush=" + (this.hflush != null);
    if (this.syncFs != null || this.hflush != null) {
      LOG.debug(msg);
    } else {
      LOG.warn("No sync support! " + msg);
    }
  }

  /**
   * Now do dirty work to see if syncFs is available on the backing this.writer.
   * It will be available in branch-0.20-append and in CDH3.
   * @return The syncFs method or null if not available.
   * @throws IOException
   */
  private Method getSyncFs()
  throws IOException {
    Method m = null;
    try {
      // function pointer to writer.syncFs() method; present when sync is hdfs-200.
      m = this.writer.getClass().getMethod("syncFs", new Class<?> []{});
    } catch (SecurityException e) {
      throw new IOException("Failed test for syncfs", e);
    } catch (NoSuchMethodException e) {
      // Not available
    }
    return m;
  }

  /**
   * See if hflush (0.21 and 0.22 hadoop) is available.
   * @return The hflush method or null if not available.
   * @throws IOException
   */
  private Method getHFlush()
  throws IOException {
    Method m = null;
    try {
      Class<? extends OutputStream> c = getWriterFSDataOutputStream().getClass();
      m = c.getMethod("hflush", new Class<?> []{});
    } catch (SecurityException e) {
      throw new IOException("Failed test for hflush", e);
    } catch (NoSuchMethodException e) {
      // Ignore
    }
    return m;
  }

  // Get at the private FSDataOutputStream inside in SequenceFile so we can
  // call sync on it.  Make it accessible.
  private FSDataOutputStream getSequenceFilePrivateFSDataOutputStreamAccessible()
  throws IOException {
    FSDataOutputStream out = null;
    final Field fields [] = this.writer.getClass().getDeclaredFields();
    final String fieldName = "out";
    for (int i = 0; i < fields.length; ++i) {
      if (fieldName.equals(fields[i].getName())) {
        try {
          // Make the 'out' field up in SF.Writer accessible.
          fields[i].setAccessible(true);
          out = (FSDataOutputStream)fields[i].get(this.writer);
          break;
        } catch (IllegalAccessException ex) {
          throw new IOException("Accessing " + fieldName, ex);
        } catch (SecurityException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
    }
    return out;
  }

  @Override
  public void append(HLog.Entry entry) throws IOException {
    this.writer.append(entry.getKey(), entry.getEdit());
  }

  @Override
  public void close() throws IOException {
    if (this.writer != null) {
      this.writer.close();
      this.writer = null;
    }
  }

  @Override
  public void sync() throws IOException {
    if (this.syncFs != null) {
      try {
       this.syncFs.invoke(this.writer, HLog.NO_ARGS);
      } catch (Exception e) {
        throw new IOException("Reflection", e);
      }
    } else if (this.hflush != null) {
      try {
        this.hflush.invoke(getWriterFSDataOutputStream(), HLog.NO_ARGS);
      } catch (Exception e) {
        throw new IOException("Reflection", e);
      }
    }
  }

  @Override
  public long getLength() throws IOException {
    return this.writer.getLength();
  }

  /**
   * @return The dfsclient out stream up inside SF.Writer made accessible, or
   * null if not available.
   */
  public FSDataOutputStream getWriterFSDataOutputStream() {
    return this.writer_out;
  }
}
