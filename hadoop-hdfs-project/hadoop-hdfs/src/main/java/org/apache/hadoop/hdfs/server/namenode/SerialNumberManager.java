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

import org.apache.hadoop.hdfs.server.namenode.INodeWithAdditionalFields.PermissionStatusFormat;
import org.apache.hadoop.hdfs.util.LongBitFormat;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

/** Manage name-to-serial-number maps for various string tables. */
public enum SerialNumberManager {
  GLOBAL(),
  USER(PermissionStatusFormat.USER, AclEntryStatusFormat.NAME),
  GROUP(PermissionStatusFormat.GROUP, AclEntryStatusFormat.NAME),
  XATTR(XAttrFormat.NAME);

  private static final SerialNumberManager[] values = values();
  private static final int maxEntryBits;
  private static final int maxEntryNumber;
  private static final int maskBits;

  private SerialNumberMap<String> serialMap;
  private int bitLength = Integer.SIZE;

  static {
    maxEntryBits = Integer.numberOfLeadingZeros(values.length);
    maxEntryNumber = (1 << maxEntryBits) - 1;
    maskBits = Integer.SIZE - maxEntryBits;
    for (SerialNumberManager snm : values) {
      // account for string table mask bits.
      snm.updateLength(maxEntryBits);
      snm.serialMap = new SerialNumberMap<String>(snm);
      FSDirectory.LOG.info(snm + " serial map: bits=" + snm.getLength() +
          " maxEntries=" + snm.serialMap.getMax());
    }
  }

  SerialNumberManager(LongBitFormat.Enum... elements) {
    // compute the smallest bit length registered with the serial manager.
    for (LongBitFormat.Enum element : elements) {
      updateLength(element.getLength());
    }
  }

  int getLength() {
    return bitLength;
  }

  private void updateLength(int maxLength) {
    bitLength = Math.min(bitLength, maxLength);
  }

  public int getSerialNumber(String u) {
    return serialMap.get(u);
  }

  public String getString(int id) {
    return serialMap.get(id);
  }

  public String getString(int id, StringTable stringTable) {
    return (stringTable != null)
        ? stringTable.get(this, id) : getString(id);
  }

  private int getMask(int bits) {
    return ordinal() << (Integer.SIZE - bits);
  }

  private static int getMaskBits() {
    return maskBits;
  }

  private int size() {
    return serialMap.size();
  }

  private Iterable<Entry<Integer, String>> entrySet() {
    return serialMap.entrySet();
  }

  // returns snapshot of current values for a save.
  public static StringTable getStringTable() {
    // approximate size for capacity.
    int size = 0;
    for (final SerialNumberManager snm : values) {
      size += snm.size();
    }
    int tableMaskBits = getMaskBits();
    StringTable map = new StringTable(size, tableMaskBits);
    for (final SerialNumberManager snm : values) {
      final int mask = snm.getMask(tableMaskBits);
      for (Entry<Integer, String> entry : snm.entrySet()) {
        map.put(entry.getKey() | mask, entry.getValue());
      }
    }
    return map;
  }

  // returns an empty table for load.
  public static StringTable newStringTable(int size, int bits) {
    if (bits > maskBits) {
      throw new IllegalArgumentException(
        "String table bits " + bits + " > " + maskBits);
    }
    return new StringTable(size, bits);
  }

  public static class StringTable implements Iterable<Entry<Integer, String>> {
    private final int tableMaskBits;
    private final Map<Integer,String> map;

    private StringTable(int size, int loadingMaskBits) {
      this.tableMaskBits = loadingMaskBits;
      this.map = new HashMap<>(size);
    }

    private String get(SerialNumberManager snm, int id) {
      if (tableMaskBits != 0) {
        if (id > maxEntryNumber) {
          throw new IllegalStateException(
              "serial id " + id + " > " + maxEntryNumber);
        }
        id |= snm.getMask(tableMaskBits);
      }
      return map.get(id);
    }

    public void put(int id, String str) {
      map.put(id, str);
    }

    public Iterator<Entry<Integer, String>> iterator() {
      return map.entrySet().iterator();
    }

    public int size() {
      return map.size();
    }

    public int getMaskBits() {
      return tableMaskBits;
    }
  }
}