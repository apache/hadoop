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
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * A facade for a {@link org.apache.hadoop.io.MapFile.Reader} that serves up
 * either the top or bottom half of a MapFile where 'bottom' is the first half
 * of the file containing the keys that sort lowest and 'top' is the second half
 * of the file with keys that sort greater than those of the bottom half.
 * The top includes the split files midkey, of the key that follows if it does
 * not exist in the file.
 * 
 * <p>This type works in tandem with the {@link Reference} type.  This class
 * is used reading while Reference is used writing.
 * 
 * <p>This file is not splitable.  Calls to {@link #midKey()} return null.
 */
//TODO should be fixed generic warnings from MapFile methods
public class HalfMapFileReader extends BloomFilterMapFile.Reader {
  private final boolean top;
  private final HStoreKey midkey;
  private boolean firstNextCall = true;
  
  /**
   * @param fs
   * @param dirName
   * @param conf
   * @param r
   * @param mk
   * @param hri
   * @throws IOException
   */
  public HalfMapFileReader(final FileSystem fs, final String dirName, 
      final Configuration conf, final Range r,
      final WritableComparable<HStoreKey> mk,
      final HRegionInfo hri)
  throws IOException {
    this(fs, dirName, conf, r, mk, false, false, hri);
  }
  
  /**
   * @param fs
   * @param dirName
   * @param conf
   * @param r
   * @param mk
   * @param filter
   * @param blockCacheEnabled
   * @param hri
   * @throws IOException
   */
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
    if (top) {
      super.finalKey(key); 
    } else {
      Writable value = new ImmutableBytesWritable();
      WritableComparable found = super.getClosest(midkey, value, true);
      Writables.copyWritable(found, key);
    }
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
      closest = (key.compareTo(this.midkey) < 0)?
        this.midkey: super.getClosest(key, val);
    } else {
      // We're serving bottom of the file.
      if (key.compareTo(this.midkey) < 0) {
        // Check key is within range for bottom.
        closest = super.getClosest(key, val);
        // midkey was made against largest store file at time of split. Smaller
        // store files could have anything in them.  Check return value is
        // not beyond the midkey (getClosest returns exact match or next after)
        if (closest != null && closest.compareTo(this.midkey) >= 0) {
          // Don't let this value out.
          closest = null;
        }
      }
      // Else, key is > midkey so let out closest = null.
    }
    return closest;
  }

  @SuppressWarnings("unchecked")
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
      if (this.top) {
        // Seek to midkey.  Midkey may not exist in this file.  That should be
        // fine.  Then we'll either be positioned at end or start of file.
        WritableComparable nearest = getClosest(this.midkey, val);
        // Now copy the midkey into the passed key.
        if (nearest != null) {
          Writables.copyWritable(nearest, key);
          return true;
        }
        return false; 
      }
    }
    boolean result = super.next(key, val);
    if (!top && key.compareTo(midkey) >= 0) {
      result = false;
    }
    return result;
  }
  
  @Override
  public synchronized void reset() throws IOException {
    if (top) {
      firstNextCall = true;
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
}
