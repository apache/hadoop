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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.io.Writable;

/**
 * A reference to the top or bottom half of a store file.  The file referenced
 * lives under a different region.  References are made at region split time.
 * 
 * <p>References work with a special half store file type.  References know how
 * to write out the reference format in the file system and are whats juggled
 * when references are mixed in with direct store files.  The half store file
 * type is used reading the referred to file.
 *
 * <p>References to store files located over in some other region look like
 * this in the file system
 * <code>1278437856009925445.3323223323</code>:
 * i.e. an id followed by hash of the referenced region.
 * Note, a region is itself not splitable if it has instances of store file
 * references.  References are cleaned up by compactions.
 */
public class Reference implements Writable {
  private byte [] splitkey;
  private Range region;

  /** 
   * For split HStoreFiles, it specifies if the file covers the lower half or
   * the upper half of the key range
   */
  public static enum Range {
    /** HStoreFile contains upper half of key range */
    top,
    /** HStoreFile contains lower half of key range */
    bottom
  }

  /**
   * Constructor
   * @param splitRow This is row we are splitting around.
   * @param fr
   */
  public Reference(final byte [] splitRow, final Range fr) {
    this.splitkey = splitRow == null?
      null: KeyValue.createFirstOnRow(splitRow).getKey();
    this.region = fr;
  }

  /**
   * Used by serializations.
   */
  public Reference() {
    this(null, Range.bottom);
  }

  /**
   * 
   * @return Range
   */
  public Range getFileRegion() {
    return this.region;
  }

  /**
   * @return splitKey
   */
  public byte [] getSplitKey() {
    return splitkey;
  }

  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return "" + this.region;
  }

  // Make it serializable.

  public void write(DataOutput out) throws IOException {
    // Write true if we're doing top of the file.
    out.writeBoolean(isTopFileRegion(this.region));
    Bytes.writeByteArray(out, this.splitkey);
  }

  public void readFields(DataInput in) throws IOException {
    boolean tmp = in.readBoolean();
    // If true, set region to top.
    this.region = tmp? Range.top: Range.bottom;
    this.splitkey = Bytes.readByteArray(in);
  }

  public static boolean isTopFileRegion(final Range r) {
    return r.equals(Range.top);
  }

  public Path write(final FileSystem fs, final Path p)
  throws IOException {
    FSUtils.create(fs, p);
    FSDataOutputStream out = fs.create(p);
    try {
      write(out);
    } finally {
      out.close();
    }
    return p;
  }

  /**
   * Read a Reference from FileSystem.
   * @param fs
   * @param p
   * @return New Reference made from passed <code>p</code>
   * @throws IOException
   */
  public static Reference read(final FileSystem fs, final Path p)
  throws IOException {
    FSDataInputStream in = fs.open(p);
    try {
      Reference r = new Reference();
      r.readFields(in);
      return r;
    } finally {
      in.close();
    }
  }
}