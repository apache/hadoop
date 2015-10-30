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

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.hdfs.XAttrHelper;

import com.google.common.collect.ImmutableList;

/**
 * Feature for extended attributes.
 */
@InterfaceAudience.Private
public class XAttrFeature implements INode.Feature {
  static final int PACK_THRESHOLD = 1024;

  /** The packed bytes for small size XAttrs. */
  private byte[] attrs;

  /**
   * List to store large size XAttrs.
   * Typically XAttr value size is small, so this
   * list is null usually.
   */
  private ImmutableList<XAttr> xAttrs;

  public XAttrFeature(List<XAttr> xAttrs) {
    if (xAttrs != null && !xAttrs.isEmpty()) {
      List<XAttr> toPack = new ArrayList<XAttr>();
      ImmutableList.Builder<XAttr> b = null;
      for (XAttr attr : xAttrs) {
        if (attr.getValue() == null ||
            attr.getValue().length <= PACK_THRESHOLD) {
          toPack.add(attr);
        } else {
          if (b == null) {
            b = ImmutableList.builder();
          }
          b.add(attr);
        }
      }
      this.attrs = XAttrFormat.toBytes(toPack);
      if (b != null) {
        this.xAttrs = b.build();
      }
    }
  }

  /**
   * Get the XAttrs.
   * @return the XAttrs
   */
  public List<XAttr> getXAttrs() {
    if (xAttrs == null) {
      return XAttrFormat.toXAttrs(attrs);
    } else {
      if (attrs == null) {
        return xAttrs;
      } else {
        List<XAttr> result = new ArrayList<>();
        result.addAll(XAttrFormat.toXAttrs(attrs));
        result.addAll(xAttrs);
        return result;
      }
    }
  }

  /**
   * Get XAttr by name with prefix.
   * @param prefixedName xAttr name with prefix
   * @return the XAttr
   */
  public XAttr getXAttr(String prefixedName) {
    XAttr attr = XAttrFormat.getXAttr(attrs, prefixedName);
    if (attr == null && xAttrs != null) {
      XAttr toFind = XAttrHelper.buildXAttr(prefixedName);
      for (XAttr a : xAttrs) {
        if (a.equalsIgnoreValue(toFind)) {
          attr = a;
          break;
        }
      }
    }
    return attr;
  }
}
