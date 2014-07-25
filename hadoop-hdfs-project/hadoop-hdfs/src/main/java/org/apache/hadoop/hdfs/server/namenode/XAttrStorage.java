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

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;

/**
 * XAttrStorage is used to read and set xattrs for an inode.
 */
@InterfaceAudience.Private
public class XAttrStorage {

  private static final Map<String, String> internedNames = Maps.newHashMap();

  /**
   * Reads the existing extended attributes of an inode. If the 
   * inode does not have an <code>XAttr</code>, then this method
   * returns an empty list.
   * <p/>
   * Must be called while holding the FSDirectory read lock.
   *
   * @param inode INode to read
   * @param snapshotId
   * @return List<XAttr> <code>XAttr</code> list. 
   */
  public static List<XAttr> readINodeXAttrs(INode inode, int snapshotId) {
    XAttrFeature f = inode.getXAttrFeature(snapshotId);
    return f == null ? ImmutableList.<XAttr> of() : f.getXAttrs();
  }
  
  /**
   * Reads the existing extended attributes of an inode.
   * <p/>
   * Must be called while holding the FSDirectory read lock.
   *
   * @param inode INode to read.
   * @return List<XAttr> <code>XAttr</code> list.
   */
  public static List<XAttr> readINodeXAttrs(INode inode) {
    XAttrFeature f = inode.getXAttrFeature();
    return f == null ? ImmutableList.<XAttr> of() : f.getXAttrs();
  }
  
  /**
   * Update xattrs of inode.
   * <p/>
   * Must be called while holding the FSDirectory write lock.
   * 
   * @param inode INode to update
   * @param xAttrs to update xAttrs.
   * @param snapshotId id of the latest snapshot of the inode
   */
  public static void updateINodeXAttrs(INode inode, 
      List<XAttr> xAttrs, int snapshotId) throws QuotaExceededException {
    if (xAttrs == null || xAttrs.isEmpty()) {
      if (inode.getXAttrFeature() != null) {
        inode.removeXAttrFeature(snapshotId);
      }
      return;
    }
    // Dedupe the xAttr name and save them into a new interned list
    List<XAttr> internedXAttrs = Lists.newArrayListWithCapacity(xAttrs.size());
    for (XAttr xAttr : xAttrs) {
      final String name = xAttr.getName();
      String internedName = internedNames.get(name);
      if (internedName == null) {
        internedName = name;
        internedNames.put(internedName, internedName);
      }
      XAttr internedXAttr = new XAttr.Builder()
          .setName(internedName)
          .setNameSpace(xAttr.getNameSpace())
          .setValue(xAttr.getValue())
          .build();
      internedXAttrs.add(internedXAttr);
    }
    // Save the list of interned xattrs
    ImmutableList<XAttr> newXAttrs = ImmutableList.copyOf(internedXAttrs);
    if (inode.getXAttrFeature() != null) {
      inode.removeXAttrFeature(snapshotId);
    }
    inode.addXAttrFeature(new XAttrFeature(newXAttrs), snapshotId);
  }
}
