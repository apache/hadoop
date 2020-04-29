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
package org.apache.hadoop.fs;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;

/**
 * Wrapper for {@link BlockLocation} that also includes a {@link LocatedBlock},
 * allowing more detailed queries to the datanode about a block.
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class HdfsBlockLocation extends BlockLocation implements Serializable {
  private static final long serialVersionUID = 0x7aecec92;

  private transient LocatedBlock block;

  public HdfsBlockLocation(BlockLocation loc, LocatedBlock block) {
    // Initialize with data from passed in BlockLocation
    super(loc);
    this.block = block;
  }

  public LocatedBlock getLocatedBlock() {
    return block;
  }

  private void readObject(ObjectInputStream ois)
      throws IOException, ClassNotFoundException {
    ois.defaultReadObject();
    // LocatedBlock is not Serializable
    block = null;
  }

  @Override
  public boolean isStriped() {
    return block.isStriped();
  }
}
