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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.fs.Syncable;
import org.apache.hadoop.fs.impl.StoreImplementationUtils;

/**
 * Support the Syncable interface on top of a DataOutputStream.
 * This allows passing the sync/hflush/hsync calls through to the
 * wrapped stream passed in to the constructor. This is required
 * for HBase when wrapping a PageBlobOutputStream used as a write-ahead log.
 */
public class SyncableDataOutputStream extends DataOutputStream
    implements Syncable, StreamCapabilities {

  private static final Logger LOG = LoggerFactory.getLogger(SyncableDataOutputStream.class);

  public SyncableDataOutputStream(OutputStream out) {
    super(out);
  }

  /**
   * Get a reference to the wrapped output stream.
   *
   * @return the underlying output stream
   */
  @InterfaceAudience.LimitedPrivate({"HDFS"})
  public OutputStream getOutStream() {
    return out;
  }

  @Override
  public boolean hasCapability(String capability) {
    return StoreImplementationUtils.hasCapability(out, capability);
  }

  @Override
  public void hflush() throws IOException {
    if (out instanceof Syncable) {
      ((Syncable) out).hflush();
    }
  }

  @Override
  public void hsync() throws IOException {
    if (out instanceof Syncable) {
      ((Syncable) out).hsync();
    }
  }

  @Override
  public void close() throws IOException {
    IOException ioeFromFlush = null;
    try {
      flush();
    } catch (IOException e) {
      ioeFromFlush = e;
      throw e;
    } finally {
      try {
        this.out.close();
      } catch (IOException e) {
        // If there was an Exception during flush(), the Azure SDK will throw back the
        // same when we call close on the same stream. When try and finally both throw
        // Exception, Java will use Throwable#addSuppressed for one of the Exception so
        // that the caller will get one exception back. When within this, if both
        // Exceptions are equal, it will throw back IllegalStateException. This makes us
        // to throw back a non IOE. The below special handling is to avoid this.
        if (ioeFromFlush == e) {
          // Do nothing..
          // The close() call gave back the same IOE which flush() gave. Just swallow it
          LOG.debug("flush() and close() throwing back same Exception. Just swallowing the latter", e);
        } else {
          // Let Java handle 2 different Exceptions been thrown from try and finally.
          throw e;
        }
      }
    }
  }
}
