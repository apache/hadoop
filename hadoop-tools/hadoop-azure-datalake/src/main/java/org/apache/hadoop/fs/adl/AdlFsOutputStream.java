/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.adl;

import com.microsoft.azure.datalake.store.ADLFileOutputStream;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Syncable;

import java.io.IOException;
import java.io.OutputStream;

import static org.apache.hadoop.fs.adl.AdlConfKeys
    .DEFAULT_WRITE_AHEAD_BUFFER_SIZE;
import static org.apache.hadoop.fs.adl.AdlConfKeys.WRITE_BUFFER_SIZE_KEY;

/**
 * Wraps {@link com.microsoft.azure.datalake.store.ADLFileOutputStream}
 * implementation.
 *
 * Flush semantics.
 * no-op, since some parts of hadoop ecosystem call flush(), expecting it to
 * have no perf impact. In hadoop filesystems, flush() itself guarantees no
 * durability: that is achieved by calling hflush() or hsync()
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class AdlFsOutputStream extends OutputStream implements Syncable {
  private final ADLFileOutputStream out;

  public AdlFsOutputStream(ADLFileOutputStream out, Configuration configuration)
      throws IOException {
    this.out = out;
    out.setBufferSize(configuration
        .getInt(WRITE_BUFFER_SIZE_KEY, DEFAULT_WRITE_AHEAD_BUFFER_SIZE));
  }

  @Override
  public synchronized void write(int b) throws IOException {
    out.write(b);
  }

  @Override
  public synchronized void write(byte[] b, int off, int len)
      throws IOException {
    out.write(b, off, len);
  }

  @Override
  public synchronized void close() throws IOException {
    out.close();
  }

  public synchronized void sync() throws IOException {
    out.flush();
  }

  public synchronized void hflush() throws IOException {
    out.flush();
  }

  public synchronized void hsync() throws IOException {
    out.flush();
  }
}
