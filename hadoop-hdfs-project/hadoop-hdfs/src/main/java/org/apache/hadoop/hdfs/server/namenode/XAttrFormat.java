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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.hdfs.XAttrHelper;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import org.apache.hadoop.hdfs.util.LongBitFormat;

/**
 * Class to pack XAttrs into byte[].<br>
 *
 * Note:  this format is used both in-memory and on-disk.  Changes will be
 * incompatible.
 *
 */

public enum XAttrFormat implements LongBitFormat.Enum {
  RESERVED(null, 5),
  NS_EXT(RESERVED.BITS, 1),
  NAME(NS_EXT.BITS, 24),
  NS(NAME.BITS, 2);

  private static final int NS_EXT_SHIFT = NS.BITS.getLength();
  private static final int NS_MASK = (1 << NS_EXT_SHIFT) - 1;

  private static final int XATTR_VALUE_LEN_MAX = 1 << 16;
  private static final XAttr.NameSpace[] XATTR_NAMESPACE_VALUES =
      XAttr.NameSpace.values();

  private final LongBitFormat BITS;

  XAttrFormat(LongBitFormat previous, int length) {
    BITS = new LongBitFormat(name(), previous, length, 0);
  }

  @Override
  public int getLength() {
    return BITS.getLength();
  }

  static XAttr.NameSpace getNamespace(int record) {
    long nid = NS.BITS.retrieve(record);
    nid |= NS_EXT.BITS.retrieve(record) << NS_EXT_SHIFT;
    return XATTR_NAMESPACE_VALUES[(int) nid];
  }

  public static String getName(int record) {
    int nid = (int)NAME.BITS.retrieve(record);
    return SerialNumberManager.XATTR.getString(nid);
  }

  static int toInt(XAttr a) {
    int nid = SerialNumberManager.XATTR.getSerialNumber(a.getName());
    int nsOrd = a.getNameSpace().ordinal();
    long value = NS.BITS.combine(nsOrd & NS_MASK, 0L);
    value = NS_EXT.BITS.combine(nsOrd >>> NS_EXT_SHIFT, value);
    value = NAME.BITS.combine(nid, value);
    return (int)value;
  }

  static XAttr toXAttr(int record, byte[] value,
                       SerialNumberManager.StringTable stringTable) {
    int nid = (int)NAME.BITS.retrieve(record);
    String name = SerialNumberManager.XATTR.getString(nid, stringTable);
    return new XAttr.Builder()
        .setNameSpace(getNamespace(record))
        .setName(name)
        .setValue(value)
        .build();
  }

  /**
   * Unpack byte[] to XAttrs.
   * 
   * @param attrs the packed bytes of XAttrs
   * @return XAttrs list
   */
  static List<XAttr> toXAttrs(byte[] attrs) {
    List<XAttr> xAttrs = new ArrayList<>();
    if (attrs == null || attrs.length == 0) {
      return xAttrs;
    }
    for (int i = 0; i < attrs.length;) {
      XAttr.Builder builder = new XAttr.Builder();
      // big-endian
      int v = Ints.fromBytes(attrs[i], attrs[i + 1],
          attrs[i + 2], attrs[i + 3]);
      i += 4;
      builder.setNameSpace(XAttrFormat.getNamespace(v));
      builder.setName(XAttrFormat.getName(v));
      int vlen = ((0xff & attrs[i]) << 8) | (0xff & attrs[i + 1]);
      i += 2;
      if (vlen > 0) {
        byte[] value = new byte[vlen];
        System.arraycopy(attrs, i, value, 0, vlen);
        builder.setValue(value);
        i += vlen;
      }
      xAttrs.add(builder.build());
    }
    return xAttrs;
  }

  /**
   * Get XAttr by name with prefix.
   * Will unpack the byte[] until find the specific XAttr
   * 
   * @param attrs the packed bytes of XAttrs
   * @param prefixedName the XAttr name with prefix
   * @return the XAttr
   */
  static XAttr getXAttr(byte[] attrs, String prefixedName) {
    if (prefixedName == null || attrs == null) {
      return null;
    }

    XAttr xAttr = XAttrHelper.buildXAttr(prefixedName);
    for (int i = 0; i < attrs.length;) {
      // big-endian
      int v = Ints.fromBytes(attrs[i], attrs[i + 1],
          attrs[i + 2], attrs[i + 3]);
      i += 4;
      XAttr.NameSpace namespace = XAttrFormat.getNamespace(v);
      String name = XAttrFormat.getName(v);
      int vlen = ((0xff & attrs[i]) << 8) | (0xff & attrs[i + 1]);
      i += 2;
      if (xAttr.getNameSpace() == namespace &&
          xAttr.getName().equals(name)) {
        if (vlen > 0) {
          byte[] value = new byte[vlen];
          System.arraycopy(attrs, i, value, 0, vlen);
          return new XAttr.Builder().setNameSpace(namespace).
              setName(name).setValue(value).build();
        }
        return xAttr;
      }
      i += vlen;
    }
    return null;
  }

  /**
   * Pack the XAttrs to byte[].
   * 
   * @param xAttrs the XAttrs
   * @return the packed bytes
   */
  static byte[] toBytes(List<XAttr> xAttrs) {
    if (xAttrs == null || xAttrs.isEmpty()) {
      return null;
    }
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try {
      for (XAttr a : xAttrs) {
        // big-endian
        int v = XAttrFormat.toInt(a);
        out.write(Ints.toByteArray(v));
        int vlen = a.getValue() == null ? 0 : a.getValue().length;
        Preconditions.checkArgument(vlen < XATTR_VALUE_LEN_MAX,
            "The length of xAttr values is too long.");
        out.write((byte)(vlen >> 8));
        out.write((byte)(vlen));
        if (vlen > 0) {
          out.write(a.getValue());
        }
      }
    } catch (IOException e) {
      // in fact, no exception
    }
    return out.toByteArray();
  }
}
