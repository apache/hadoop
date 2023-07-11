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

import org.apache.hadoop.hdfs.util.LongBitFormat;

import java.nio.charset.StandardCharsets;

/**
 * Class to pack XAttrs Value.<br>
 *
 * Note:  this format is used both in-memory and on-disk.  Changes will be
 * incompatible.
 *
 */

public enum XAttrValueFormat implements LongBitFormat.Enum {
  VALUE(null, 24);

  private final LongBitFormat BITS;

  XAttrValueFormat(LongBitFormat previous, int length) {
    BITS = new LongBitFormat(name(), previous, length, 0);
  }

  @Override
  public int getLength() {
    return BITS.getLength();
  }

  public static byte[] getValue(int record) {
    int nid = (int)VALUE.BITS.retrieve(record);
    return SerialNumberManager.XATTR.getString(nid).getBytes(StandardCharsets.UTF_8);
  }

  static int toInt(byte[] value) {
    int vid = SerialNumberManager.XATTR.getSerialNumber(new String(value, StandardCharsets.UTF_8));
    long res = 0L;
    res = VALUE.BITS.combine(vid, res);
    return (int)res;
  }

}
