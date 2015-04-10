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

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;

import java.util.AbstractMap;
import java.util.Map;

class FlatINodesInPath {
  final ImmutableList<Map.Entry<ByteString, FlatINode>> inodes;

  FlatINodesInPath(ImmutableList<Map.Entry<ByteString, FlatINode>> inodes) {
    this.inodes = inodes;
  }

  public FlatINode getLastINode() {
    return getLastINode(-1);
  }

  FlatINode getLastINode(int offset) {
    return inodes.get(inodes.size() + offset).getValue();
  }

  ByteString getLastPathComponent() {
    return getLastPathComponent(-1);
  }

  ByteString getLastPathComponent(int offset) {
    return inodes.get(inodes.size() + offset).getKey();
  }

  String path() {
    StringBuilder sb = new StringBuilder();
    boolean addPrefix = false;
    for (Map.Entry<ByteString, FlatINode> p : inodes) {
      if (addPrefix) {
        sb.append('/');
      }
      sb.append(p.getKey().toStringUtf8());
      addPrefix = true;
    }
    return sb.toString();
  }

  ImmutableList<Map.Entry<ByteString, FlatINode>> inodes() {
    return inodes;
  }

  public int length() {
    return inodes.size();
  }

  static FlatINodesInPath concat(
      FlatINodesInPath iip, Map.Entry<ByteString, FlatINode> last) {
    ImmutableList<Map.Entry<ByteString, FlatINode>> l = ImmutableList.<Map.Entry<ByteString, FlatINode>>builder().addAll(
        iip.inodes).add(last).build();
    return new FlatINodesInPath(l);
  }

  static FlatINodesInPath addINode(
      FlatINodesInPath iip, ByteString localName, FlatINode inode) {
    return concat(iip, new AbstractMap.SimpleImmutableEntry<>(localName, inode));
  }
}