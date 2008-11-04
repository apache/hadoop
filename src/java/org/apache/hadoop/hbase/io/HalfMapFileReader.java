/**
 * Copyright 2008 The Apache Software Foundation
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
package org.apache.hadoop.hbase.io;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HStoreKey;
import org.apache.hadoop.hbase.io.Reference.Range;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * A facade for a {@link MapFile.Reader} that serves up either the top or
 * bottom half of a MapFile where 'bottom' is the first half of the file
 * containing the keys that sort lowest and 'top' is the second half of the
 * file with keys that sort greater than those of the bottom half.  The top
 * includes the split files midkey, of the key that follows if it does not
 * exist in the file.
 * 
 * <p>This type works in tandem with the {@link Reference} type.  This class
 * is used reading while Reference is used writing.
 * 
 * <p>This file is not splitable.  Calls to {@link #midKey()} return null.
 */
public class HalfMapFileReader extends BloomFilterMapFile.Reader {
  private final boolean top;
  private final HStoreKey midkey;
  private boolean firstNextCall = true;
  private final WritableComparable<HStoreKey> firstKey;
  private final WritableComparable<HStoreKey> finalKey;
  
  public HalfMapFileReader(final FileSystem fs, final String dirName, 
      final Configuration conf, final Range r,
      final WritableComparable<HStoreKey> mk,
      final HRegionInfo hri)
  throws IOException {
    this(fs, dirName, conf, r, mk, false, false, hri);
  }
  
  @SuppressWarnings("unchecked")
  public HalfMapFileReader(final FileSystem fs, final String dirName, 
      final Configuration conf, final Range r,
      final WritableComparable<HStoreKey> mk, final boolean filter,
      final boolean blockCacheEnabled,
      final HRegionInfo hri)
  throws IOException {
    super(fs, dirName, conf, filter, blockCacheEnabled, hri);
    // This is not actual midkey for this half-file; its just border
    // around which we split top and bottom.  Have to look in files to find
    // actual last and first keys for bottom and top halves.  Half-files don't
    // have an actual midkey themselves. No midkey is how we indicate file is
    // not splittable.
    this.midkey = new HStoreKey((HStoreKey)mk);
    this.midkey.setHRegionInfo(hri);
    // Is it top or bottom half?
    this.top = Reference.isTopFileRegion(r);
    // Firstkey comes back null if passed midkey is lower than first key in file
    // and its a bottom half HalfMapFileReader, OR, if midkey is higher than
    // the final key in the backing file.  In either case, it means this half
    // file is empty.
    this.firstKey = this.top?
      super.getClosest(this.midkey, new ImmutableBytesWritable()):
      super.getFirstKey().compareTo(this.midkey) > 0?
        null: super.getFirstKey();
    this.finalKey = this.top?
      super.getFinalKey():
      super.getClosest(new HStoreKey.BeforeThisStoreKey(this.midkey),
          new ImmutableBytesWritable(), true);
  }
  
  /*
   * Check key is not bleeding into wrong half of the file.
   * @param key
   * @throws IOException
   */
  private void checkKey(final WritableComparable<HStoreKey> key)
  throws IOException {
    if (top) {
      if (key.compareTo(midkey) < 0) {
        throw new IOException("Illegal Access: Key is less than midKey of " +
        "backing mapfile");
      }
    } else if (key.compareTo(midkey) >= 0) {
      throw new IOException("Illegal Access: Key is greater than or equal " +
      "to midKey of backing mapfile");
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public synchronized void finalKey(WritableComparable key)
  throws IOException {
     Writables.copyWritable(this.finalKey, key);
     return;
  }

  @SuppressWarnings("unchecked")
  @Override
  public synchronized Writable get(WritableComparable key, Writable val)
  throws IOException {
    checkKey(key);
    return super.get(key, val);
  }

  @SuppressWarnings("unchecked")
  @Override
  public synchronized WritableComparable getClosest(WritableComparable key,
    Writable val)
  throws IOException {
    WritableComparable closest = null;
    if (top) {
      // If top, the lowest possible key is first key.  Do not have to check
      // what comes back from super getClosest.  Will return exact match or
      // greater.
      closest = (key.compareTo(getFirstKey()) < 0)?
        getClosest(getFirstKey(), val): super.getClosest(key, val);
    } else {
      // We're serving bottom of the file.
      if (key.compareTo(this.midkey) < 0) {
        // Check key is within range for bottom.
        closest = super.getClosest(key, val);
        // midkey was made against largest store file at time of split. Smaller
        // store files could have anything in them.  Check return value is
        // not beyond the midkey (getClosest returns exact match or next
        // after).
        if (closest != null && closest.compareTo(this.midkey) >= 0) {
          // Don't let this value out.
          closest = null;
        }
      }
      // Else, key is > midkey so let out closest = null.
    }
    return closest;
  }

  @SuppressWarnings({"unused", "unchecked"})
  @Override
  public synchronized WritableComparable midKey() throws IOException {
    // Returns null to indicate file is not splitable.
    return null;
  }

  @SuppressWarnings("unchecked")
  @Override
  public synchronized boolean next(WritableComparable key, Writable val)
  throws IOException {
    if (firstNextCall) {
      firstNextCall = false;
      if (isEmpty()) {
        return false;
      }
      // Seek and fill by calling getClosest on first key.
      if (this.firstKey != null) {
        Writables.copyWritable(this.firstKey, key);
        WritableComparable nearest = getClosest(key, val);
        if (!key.equals(nearest)) {
          throw new IOException("Keys don't match and should: " +
              key.toString() + ", " + nearest.toString());
        }
      }
      return true;
    }
    boolean result = super.next(key, val);
    if (!top && key.compareTo(midkey) >= 0) {
      result = false;
    }
    return result;
  }
  
  private boolean isEmpty() {
    return this.firstKey == null;
  }

  @Override
  public synchronized void reset() throws IOException {
    if (top) {
      firstNextCall = true;
      // I don't think this is needed. seek(this.firstKey);
      return;
    }
    super.reset();
  }

  @SuppressWarnings("unchecked")
  @Override
  public synchronized boolean seek(WritableComparable key)
  throws IOException {
    checkKey(key);
    return super.seek(key);
  }
  
  @Override
  public HStoreKey getFirstKey() {
    return (HStoreKey)this.firstKey;
  }
  
  @Override
  public HStoreKey getFinalKey() {
    return (HStoreKey)this.finalKey;
  }
  
  @Override
  public String toString() {
    return super.toString() + ", half=" + (top? "top": "bottom");
  }
}