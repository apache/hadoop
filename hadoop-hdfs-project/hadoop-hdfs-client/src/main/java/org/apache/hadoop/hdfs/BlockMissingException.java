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

package org.apache.hadoop.hdfs;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
  * This exception is thrown when a read encounters a block that has no
  * locations associated with it.
  */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class BlockMissingException extends IOException {

  private static final long serialVersionUID = 1L;

  private final String filename;
  private final long   offset;

  /**
   * An exception that indicates that file was corrupted.
   * @param filename name of corrupted file
   * @param description a description of the corruption details
   */
  public BlockMissingException(String filename, String description,
      long offset) {
    super(description);
    this.filename = filename;
    this.offset = offset;
  }

  /**
   * Returns the name of the corrupted file.
   * @return name of corrupted file
   */
  public String getFile() {
    return filename;
  }

  /**
   * Returns the offset at which this file is corrupted
   * @return offset of corrupted file
   */
  public long getOffset() {
    return offset;
  }
}
