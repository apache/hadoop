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
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import java.io.Closeable;
import java.io.IOException;

/**
 * A generic abstract class to support reading edits log data from 
 * persistent storage.
 * 
 * It should stream bytes from the storage exactly as they were written
 * into the #{@link EditLogOutputStream}.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public abstract class EditLogInputStream implements Closeable {
  private FSEditLogOp cachedOp = null; 
  
  /** 
   * @return the name of the EditLogInputStream
   */
  public abstract String getName();
  
  /** 
   * @return the first transaction which will be found in this stream
   */
  public abstract long getFirstTxId();
  
  /** 
   * @return the last transaction which will be found in this stream
   */
  public abstract long getLastTxId();


  /**
   * Close the stream.
   * @throws IOException if an error occurred while closing
   */
  @Override
  public abstract void close() throws IOException;

  /** 
   * Read an operation from the stream
   * @return an operation from the stream or null if at end of stream
   * @throws IOException if there is an error reading from the stream
   */
  public FSEditLogOp readOp() throws IOException {
    FSEditLogOp ret;
    if (cachedOp != null) {
      ret = cachedOp;
      cachedOp = null;
      return ret;
    }
    return nextOp();
  }
  
  /** 
   * Position the stream so that a valid operation can be read from it with
   * readOp().
   * 
   * This method can be used to skip over corrupted sections of edit logs.
   */
  public void resync() {
    if (cachedOp != null) {
      return;
    }
    cachedOp = nextValidOp();
  }
  
  /** 
   * Get the next operation from the stream storage.
   * 
   * @return an operation from the stream or null if at end of stream
   * @throws IOException if there is an error reading from the stream
   */
  protected abstract FSEditLogOp nextOp() throws IOException;
  
  /** 
   * Get the next valid operation from the stream storage.
   * 
   * This is exactly like nextOp, except that we attempt to skip over damaged
   * parts of the edit log
   * 
   * @return an operation from the stream or null if at end of stream
   */
  protected FSEditLogOp nextValidOp() {
    // This is a trivial implementation which just assumes that any errors mean
    // that there is nothing more of value in the log.  Subclasses that support
    // error recovery will want to override this.
    try {
      return nextOp();
    } catch (Throwable e) {
      return null;
    }
  }
  
  /** 
   * Skip edit log operations up to a given transaction ID, or until the
   * end of the edit log is reached.
   *
   * After this function returns, the next call to readOp will return either
   * end-of-file (null) or a transaction with a txid equal to or higher than
   * the one we asked for.
   *
   * @param txid    The transaction ID to read up until.
   * @return        Returns true if we found a transaction ID greater than
   *                or equal to 'txid' in the log.
   */
  public boolean skipUntil(long txid) throws IOException {
    while (true) {
      FSEditLogOp op = readOp();
      if (op == null) {
        return false;
      }
      if (op.getTransactionId() >= txid) {
        cachedOp = op;
        return true;
      }
    }
  }
  
  /** 
   * Get the layout version of the data in the stream.
   * @return the layout version of the ops in the stream.
   * @throws IOException if there is an error reading the version
   */
  public abstract int getVersion() throws IOException;

  /**
   * Get the "position" of in the stream. This is useful for 
   * debugging and operational purposes.
   *
   * Different stream types can have a different meaning for 
   * what the position is. For file streams it means the byte offset
   * from the start of the file.
   *
   * @return the position in the stream
   */
  public abstract long getPosition();

  /**
   * Return the size of the current edits log.
   */
  public abstract long length() throws IOException;
  
  /**
   * Return true if this stream is in progress, false if it is finalized.
   */
  public abstract boolean isInProgress();
  
  /**
   * Set the maximum opcode size in bytes.
   */
  public abstract void setMaxOpSize(int maxOpSize);
}
