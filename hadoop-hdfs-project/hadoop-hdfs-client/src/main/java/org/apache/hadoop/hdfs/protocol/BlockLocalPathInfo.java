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
package org.apache.hadoop.hdfs.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * A block and the full path information to the block data file and
 * the metadata file stored on the local file system.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class BlockLocalPathInfo {
  private final ExtendedBlock block;
  private String localBlockPath = "";  // local file storing the data
  private String localMetaPath = "";   // local file storing the checksum

  /**
   * Constructs BlockLocalPathInfo.
   * @param b The block corresponding to this lock path info.
   * @param file Block data file.
   * @param metafile Metadata file for the block.
   */
  public BlockLocalPathInfo(ExtendedBlock b, String file, String metafile) {
    block = b;
    localBlockPath = file;
    localMetaPath = metafile;
  }

  /**
   * Get the Block data file.
   * @return Block data file.
   */
  public String getBlockPath() {return localBlockPath;}

  /**
   * @return the Block
   */
  public ExtendedBlock getBlock() { return block;}

  /**
   * Get the Block metadata file.
   * @return Block metadata file.
   */
  public String getMetaPath() {return localMetaPath;}

  /**
   * Get number of bytes in the block.
   * @return Number of bytes in the block.
   */
  public long getNumBytes() {
    return block.getNumBytes();
  }
}
