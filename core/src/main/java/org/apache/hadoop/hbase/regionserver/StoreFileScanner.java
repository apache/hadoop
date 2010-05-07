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

package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;

import java.io.IOException;

/**
 * A KeyValue scanner that iterates over a single HFile
 */
class StoreFileScanner implements KeyValueScanner {

  private HFileScanner hfs;
  private KeyValue cur = null;

  /**
   * Implements a {@link KeyValueScanner} on top of the specified {@link HFileScanner}
   * @param hfs HFile scanner
   */
  public StoreFileScanner(HFileScanner hfs) {
    this.hfs = hfs;
  }

  public String toString() {
    return "StoreFileScanner[" + hfs.toString() + ", cur=" + cur + "]";
  }

  public KeyValue peek() {
    return cur;
  }

  public KeyValue next() {
    KeyValue retKey = cur;
    cur = hfs.getKeyValue();
    try {
      // only seek if we arent at the end. cur == null implies 'end'.
      if (cur != null)
        hfs.next();
    } catch(IOException e) {
      // Turn checked exception into runtime exception.
      throw new RuntimeException(e);
    }
    return retKey;
  }

  public boolean seek(KeyValue key) {
    try {
      if(!seekAtOrAfter(hfs, key)) {
        close();
        return false;
      }
      cur = hfs.getKeyValue();
      hfs.next();
      return true;
    } catch(IOException ioe) {
      close();
      return false;
    }
  }

  public void close() {
    // Nothing to close on HFileScanner?
    cur = null;
  }

  /**
   *
   * @param s
   * @param k
   * @return
   * @throws IOException
   */
  public static boolean seekAtOrAfter(HFileScanner s, KeyValue k)
  throws IOException {
    int result = s.seekTo(k.getBuffer(), k.getKeyOffset(), k.getKeyLength());
    if(result < 0) {
      // Passed KV is smaller than first KV in file, work from start of file
      return s.seekTo();
    } else if(result > 0) {
      // Passed KV is larger than current KV in file, if there is a next
      // it is the "after", if not then this scanner is done.
      return s.next();
    }
    // Seeked to the exact key
    return true;
  }
}