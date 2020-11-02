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

package org.apache.hadoop.runc.squashfs.table;

import static org.apache.hadoop.runc.squashfs.util.BinUtils.DumpOptions.DECIMAL;
import static org.apache.hadoop.runc.squashfs.util.BinUtils.DumpOptions.UNSIGNED;
import static org.apache.hadoop.runc.squashfs.util.BinUtils.dumpBin;

public class FragmentTableEntry {
  private final long start;
  private final int size;

  public FragmentTableEntry(long start, int size, boolean compressed) {
    this(start, compressed ? size : (size | 0x1000000));
  }

  public FragmentTableEntry(long start, int size) {
    this.start = start;
    this.size = size;
  }

  public long getStart() {
    return start;
  }

  public int getSize() {
    return size;
  }

  public boolean isCompressed() {
    return (size & 0x1000000) == 0;
  }

  public int getDiskSize() {
    return (size & 0xFFFFF);
  }

  @Override
  public String toString() {
    StringBuilder buf = new StringBuilder();
    buf.append(String.format("fragment-table-entry {%n"));
    int width = 10;
    dumpBin(buf, width, "start", start, DECIMAL, UNSIGNED);
    dumpBin(buf, width, "compressed", isCompressed() ? "true" : "false");
    dumpBin(buf, width, "diskSize", getDiskSize(), DECIMAL, UNSIGNED);
    buf.append("}");
    return buf.toString();
  }
}
