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

package org.apache.hadoop.fs.azure;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.fs.Syncable;

/**
 * Support the Syncable interface on top of a DataOutputStream.
 * This allows passing the sync/hflush/hsync calls through to the
 * wrapped stream passed in to the constructor. This is required
 * for HBase when wrapping a PageBlobOutputStream used as a write-ahead log.
 */
public class SyncableDataOutputStream extends DataOutputStream implements Syncable {

  public SyncableDataOutputStream(OutputStream out) {
    super(out);
  }

  @Override
  @Deprecated
  public void sync() throws IOException {
    hflush();
  }

  @Override
  public void hflush() throws IOException {
    if (out instanceof Syncable) {
      ((Syncable) out).hflush();
    } else {
      out.flush();
    }
  }

  @Override
  public void hsync() throws IOException {
    if (out instanceof Syncable) {
      ((Syncable) out).hsync();
    } else {
      out.flush();
    }
  }
}
