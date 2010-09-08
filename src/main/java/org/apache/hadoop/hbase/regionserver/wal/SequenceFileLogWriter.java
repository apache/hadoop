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
 * {@link SequenceFile.Writer}.
 */
public class SequenceFileLogWriter implements HLog.Writer {
  private final Log LOG = LogFactory.getLog(this.getClass());
  // The sequence file we delegate to.
  private SequenceFile.Writer writer;
  // The dfsclient out stream gotten made accessible or null if not available.
  private OutputStream dfsClient_out;
  // The syncFs method from hdfs-200 or null if not available.
  private Method syncFs;

  public SequenceFileLogWriter() {
    super();
  }

  @Override
  public void init(FileSystem fs, Path path, Configuration conf)
      throws IOException {
    // Create a SF.Writer instance.
    this.writer = SequenceFile.createWriter(fs, conf, path,
      HLog.getKeyClass(conf), WALEdit.class,
      fs.getConf().getInt("io.file.buffer.size", 4096),
      (short) conf.getInt("hbase.regionserver.hlog.replication",
        fs.getDefaultReplication()),
      conf.getLong("hbase.regionserver.hlog.blocksize",
        fs.getDefaultBlockSize()),
      SequenceFile.CompressionType.NONE,
      new DefaultCodec(),
      null,
      new Metadata());

    // Get at the private FSDataOutputStream inside in SequenceFile so we can
    // call sync on it.  Make it accessible.  Stash it aside for call up in
    // the sync method.
    final Field fields [] = this.writer.getClass().getDeclaredFields();
    final String fieldName = "out";
    for (int i = 0; i < fields.length; ++i) {
      if (fieldName.equals(fields[i].getName())) {
        try {
          // Make the 'out' field up in SF.Writer accessible.
          fields[i].setAccessible(true);
          FSDataOutputStream out =
            (FSDataOutputStream)fields[i].get(this.writer);
          this.dfsClient_out = out.getWrappedStream();
          break;
        } catch (IllegalAccessException ex) {
          throw new IOException("Accessing " + fieldName, ex);
        }
      }
    }

    // Now do dirty work to see if syncFs is available.
    // Test if syncfs is available.
    Method m = null;
    boolean append = conf.getBoolean("dfs.support.append", false);
    if (append) {
      try {
        // function pointer to writer.syncFs()
        m = this.writer.getClass().getMethod("syncFs", new Class<?> []{});
      } catch (SecurityException e) {
        throw new IOException("Failed test for syncfs", e);
      } catch (NoSuchMethodException e) {
        // Not available
      }
    }
    this.syncFs = m;
    LOG.info((this.syncFs != null)?
      "Using syncFs -- HDFS-200":
      ("syncFs -- HDFS-200 -- not available, dfs.support.append=" + append));
  }

  @Override
  public void append(HLog.Entry entry) throws IOException {
    this.writer.append(entry.getKey(), entry.getEdit());
  }

  @Override
  public void close() throws IOException {
    this.writer.close();
  }

  @Override
  public void sync() throws IOException {
    if (this.syncFs != null) {
      try {
       this.syncFs.invoke(this.writer, HLog.NO_ARGS);
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
  public OutputStream getDFSCOutputStream() {
    return this.dfsClient_out;
  }
}
