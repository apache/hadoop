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
import java.lang.reflect.Field;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Metadata;
import org.apache.hadoop.io.compress.DefaultCodec;

public class SequenceFileLogWriter implements HLog.Writer {

  SequenceFile.Writer writer;
  FSDataOutputStream writer_out;

  public SequenceFileLogWriter() { }

  @Override
  public void init(FileSystem fs, Path path, Configuration conf)
      throws IOException {
    writer = SequenceFile.createWriter(fs, conf, path,
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
    final Field fields[] = writer.getClass().getDeclaredFields();
    final String fieldName = "out";
    for (int i = 0; i < fields.length; ++i) {
      if (fieldName.equals(fields[i].getName())) {
        try {
          fields[i].setAccessible(true);
          this.writer_out = (FSDataOutputStream)fields[i].get(writer);
          break;
        } catch (IllegalAccessException ex) {
          throw new IOException("Accessing " + fieldName, ex);
        }
      }
    }
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
    this.writer.sync();

    this.writer.syncFs();
  }

  @Override
  public long getLength() throws IOException {
    return this.writer.getLength();
  }
}
