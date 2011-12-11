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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/****************************************************************
 *CreateFlag specifies the file create semantic. Users can combine flags like:<br>
 *<code>
 * EnumSet.of(CreateFlag.CREATE, CreateFlag.APPEND)
 * <code>
 * and pass it to {@link org.apache.hadoop.fs.FileSystem #create(Path f, FsPermission permission,
 * EnumSet<CreateFlag> flag, int bufferSize, short replication, long blockSize,
 * Progressable progress)}.
 * 
 * <p>
 * Combine {@link #OVERWRITE} with either {@link #CREATE} 
 * or {@link #APPEND} does the same as only use 
 * {@link #OVERWRITE}. <br>
 * Combine {@link #CREATE} with {@link #APPEND} has the semantic:
 * <ol>
 * <li> create the file if it does not exist;
 * <li> append the file if it already exists.
 * </ol>
 *****************************************************************/
@InterfaceAudience.Public
@InterfaceStability.Stable
public enum CreateFlag {

  /**
   * create the file if it does not exist, and throw an IOException if it
   * already exists
   */
  CREATE((short) 0x01),

  /**
   * create the file if it does not exist, if it exists, overwrite it.
   */
  OVERWRITE((short) 0x02),

  /**
   * append to a file, and throw an IOException if it does not exist
   */
  APPEND((short) 0x04);

  private short mode;

  private CreateFlag(short mode) {
    this.mode = mode;
  }

  short getMode() {
    return mode;
  }
}