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
package org.apache.hadoop.hdfs.server.namenode.snapshot;

import java.util.Comparator;

import org.apache.hadoop.classification.InterfaceAudience;

/** Snapshot of a sub-tree in the namesystem. */
@InterfaceAudience.Private
public class Snapshot implements Comparable<byte[]> {
  /** Compare snapshot IDs with null <= s for any snapshot s. */
  public static final Comparator<Snapshot> ID_COMPARATOR
      = new Comparator<Snapshot>() {
    @Override
    public int compare(Snapshot left, Snapshot right) {
      if (left == null) {
        return right == null? 0: -1;
      } else {
        return right == null? 1: left.id - right.id; 
      }
    }
  };

  /** Snapshot ID. */
  private final int id;
  /** The root directory of the snapshot. */
  private final INodeDirectoryWithSnapshot root;

  Snapshot(int id, String name, INodeDirectorySnapshottable dir) {
    this.id = id;
    this.root = new INodeDirectoryWithSnapshot(name, dir);
  }

  /** @return the root directory of the snapshot. */
  public INodeDirectoryWithSnapshot getRoot() {
    return root;
  }

  @Override
  public int compareTo(byte[] bytes) {
    return root.compareTo(bytes);
  }
  
  @Override
  public String toString() {
    return getClass().getSimpleName() + ":" + root.getLocalName();
  }
}
