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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.EnumSet;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/****************************************************************
 * CreateFlag specifies the file create semantic. Users can combine flags like: <br>
 * <code>
 * EnumSet.of(CreateFlag.CREATE, CreateFlag.APPEND)
 * </code>
 * <p>
 * 
 * Use the CreateFlag as follows:
 * <ol>
 * <li> CREATE - to create a file if it does not exist, 
 * else throw FileAlreadyExists.</li>
 * <li> APPEND - to append to a file if it exists, 
 * else throw FileNotFoundException.</li>
 * <li> OVERWRITE - to truncate a file if it exists, 
 * else throw FileNotFoundException.</li>
 * <li> CREATE|APPEND - to create a file if it does not exist, 
 * else append to an existing file.</li>
 * <li> CREATE|OVERWRITE - to create a file if it does not exist, 
 * else overwrite an existing file.</li>
 * <li> SYNC_BLOCK - to force closed blocks to the disk device.
 * In addition {@link Syncable#hsync()} should be called after each write,
 * if true synchronous behavior is required.</li>
 * <li> LAZY_PERSIST - Create the block on transient storage (RAM) if
 * available.</li>
 * <li> APPEND_NEWBLOCK - Append data to a new block instead of end of the last
 * partial block.</li>
 * </ol>
 * 
 * Following combinations are not valid and will result in
 * {@link HadoopIllegalArgumentException}:
 * <ol>
 * <li> APPEND|OVERWRITE</li>
 * <li> CREATE|APPEND|OVERWRITE</li>
 * </ol>
 *****************************************************************/
@InterfaceAudience.Public
@InterfaceStability.Evolving
public enum CreateFlag {

  /**
   * Create a file. See javadoc for more description
   * already exists
   */
  CREATE((short) 0x01),

  /**
   * Truncate/overwrite a file. Same as POSIX O_TRUNC. See javadoc for description.
   */
  OVERWRITE((short) 0x02),

  /**
   * Append to a file. See javadoc for more description.
   */
  APPEND((short) 0x04),

  /**
   * Force closed blocks to disk. Similar to POSIX O_SYNC. See javadoc for description.
   */
  SYNC_BLOCK((short) 0x08),

  /**
   * Create the block on transient storage (RAM) if available. If
   * transient storage is unavailable then the block will be created
   * on disk.
   *
   * HDFS will make a best effort to lazily write these files to persistent
   * storage, however file contents may be lost at any time due to process/
   * node restarts, hence there is no guarantee of data durability.
   *
   * This flag must only be used for intermediate data whose loss can be
   * tolerated by the application.
   */
  LAZY_PERSIST((short) 0x10),

  /**
   * Append data to a new block instead of the end of the last partial block.
   * This is only useful for APPEND.
   */
  NEW_BLOCK((short) 0x20),

  /**
   * Advise that a block replica NOT be written to the local DataNode where
   * 'local' means the same host as the client is being run on.
   */
  @InterfaceAudience.LimitedPrivate({"HBase"})
  NO_LOCAL_WRITE((short) 0x40),

  /**
   * Enforce the file to be a replicated file, no matter what its parent
   * directory's replication or erasure coding policy is.
   */
  SHOULD_REPLICATE((short) 0x80),

  /**
   * Advise that the first block replica NOT take into account DataNode
   * locality. The first block replica should be placed randomly within the
   * cluster. Subsequent block replicas should follow DataNode locality rules.
   */
  IGNORE_CLIENT_LOCALITY((short) 0x100),

  /**
   * Advise that a block replica NOT be written to the local rack DataNode where
   * 'local' means the same rack as the client is being run on.
   */
  NO_LOCAL_RACK((short) 0x120);

  private final short mode;

  private CreateFlag(short mode) {
    this.mode = mode;
  }

  short getMode() {
    return mode;
  }
  
  /**
   * Validate the CreateFlag and throw exception if it is invalid
   * @param flag set of CreateFlag
   * @throws HadoopIllegalArgumentException if the CreateFlag is invalid
   */
  public static void validate(EnumSet<CreateFlag> flag) {
    if (flag == null || flag.isEmpty()) {
      throw new HadoopIllegalArgumentException(flag
          + " does not specify any options");
    }
    final boolean append = flag.contains(APPEND);
    final boolean overwrite = flag.contains(OVERWRITE);
    
    // Both append and overwrite is an error
    if (append && overwrite) {
      throw new HadoopIllegalArgumentException(
          flag + "Both append and overwrite options cannot be enabled.");
    }
  }
  
  /**
   * Validate the CreateFlag for create operation
   * @param path Object representing the path; usually String or {@link Path}
   * @param pathExists pass true if the path exists in the file system
   * @param flag set of CreateFlag
   * @throws IOException on error
   * @throws HadoopIllegalArgumentException if the CreateFlag is invalid
   */
  public static void validate(Object path, boolean pathExists,
      EnumSet<CreateFlag> flag) throws IOException {
    validate(flag);
    final boolean append = flag.contains(APPEND);
    final boolean overwrite = flag.contains(OVERWRITE);
    if (pathExists) {
      if (!(append || overwrite)) {
        throw new FileAlreadyExistsException("File already exists: "
            + path.toString()
            + ". Append or overwrite option must be specified in " + flag);
      }
    } else if (!flag.contains(CREATE)) {
      throw new FileNotFoundException("Non existing file: " + path.toString()
          + ". Create option is not specified in " + flag);
    }
  }

  /**
   * Validate the CreateFlag for the append operation. The flag must contain
   * APPEND, and cannot contain OVERWRITE.
   */
  public static void validateForAppend(EnumSet<CreateFlag> flag) {
    validate(flag);
    if (!flag.contains(APPEND)) {
      throw new HadoopIllegalArgumentException(flag
          + " does not contain APPEND");
    }
  }
}