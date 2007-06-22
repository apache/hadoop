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


import org.apache.hadoop.io.*;

/** Interface that represents the client side information for a file.
 */
public interface FileStatus {

  /* 
   * @return the length of this file, in blocks
   */
  public long getLen();

  /**
   * Is this a directory?
   * @return true if this is a directory
   */
  public boolean isDir();

  /**
   * Get the block size of the file.
   * @return the number of bytes
   */
  public long getBlockSize();

  /**
   * Get the replication factor of a file.
   * @return the replication factor of a file.
   */
  public short getReplication();

  /**
   * Get the modification time of the file.
   * @return the modification time of file.
   */
  public long getModificationTime();
}
