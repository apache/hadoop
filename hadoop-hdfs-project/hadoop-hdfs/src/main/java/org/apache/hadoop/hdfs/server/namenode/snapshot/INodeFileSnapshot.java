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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.server.namenode.FSImage;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;

/**
 *  INode representing a snapshot of a file.
 */
@InterfaceAudience.Private
public class INodeFileSnapshot extends INodeFileWithSnapshot {
  /** The file size at snapshot creation time. */
  final long snapshotFileSize;

  INodeFileSnapshot(INodeFileWithSnapshot f) {
    super(f);
    this.snapshotFileSize = f.computeFileSize(true);
    f.insertAfter(this);
  }
  
  /**
   * A constructor that only sets the basic attributes and the size. Used while
   * loading {@link FSImage}
   */
  public INodeFileSnapshot(INodeFile f, long size) {
    super(f);
    this.snapshotFileSize = size;
  }

  @Override
  public long computeFileSize(boolean includesBlockInfoUnderConstruction) {
    //ignore includesBlockInfoUnderConstruction 
    //since files in a snapshot are considered as closed.
    return snapshotFileSize;
  }
}