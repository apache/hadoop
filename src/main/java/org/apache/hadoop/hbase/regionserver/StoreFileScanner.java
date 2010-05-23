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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.io.hfile.HFile.Reader;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * A KeyValue scanner that iterates over a single HFile
 */
class StoreFileScanner implements KeyValueScanner {
  static final Log LOG = LogFactory.getLog(Store.class);

  private HFileScanner hfs;
  private KeyValue cur = null;

  /**
   * Implements a {@link KeyValueScanner} on top of the specified {@link HFileScanner}
   * @param hfs HFile scanner
   */
  private StoreFileScanner(HFileScanner hfs) {
    this.hfs = hfs;
  }

  /**
   * Return an array of scanners corresponding to the given
   * set of store files.
   */
  public static List<StoreFileScanner> getScannersForStoreFiles(
      Collection<StoreFile> filesToCompact,
      boolean cacheBlocks,
      boolean usePread) throws IOException {
    List<StoreFileScanner> scanners =
      new ArrayList<StoreFileScanner>(filesToCompact.size());
    for (StoreFile file : filesToCompact) {
      Reader r = file.createReader();
      scanners.add(new StoreFileScanner(r.getScanner(cacheBlocks, usePread)));
    }
    return scanners;
  }
  
  public HFileScanner getHFileScanner() {
    return this.hfs;
  }
  
  public String toString() {
    return "StoreFileScanner[" + hfs.toString() + ", cur=" + cur + "]";
  }

  public KeyValue peek() {
    return cur;
  }

  public KeyValue next() throws IOException {
    KeyValue retKey = cur;
    cur = hfs.getKeyValue();
    try {
      // only seek if we arent at the end. cur == null implies 'end'.
      if (cur != null)
        hfs.next();
    } catch(IOException e) {
      throw new IOException("Could not iterate " + this, e);
    }
    return retKey;
  }

  public boolean seek(KeyValue key) throws IOException {
    try {
      if(!seekAtOrAfter(hfs, key)) {
        close();
        return false;
      }
      cur = hfs.getKeyValue();
      hfs.next();
      return true;
    } catch(IOException ioe) {
      throw new IOException("Could not seek " + this, ioe);
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
