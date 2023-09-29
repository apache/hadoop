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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;

import java.io.IOException;

/**
 * This exception is thrown when the length of a LocatedBlock instance
 * can not be obtained.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class CannotObtainBlockLengthException extends IOException {
  private static final long serialVersionUID = 1L;

  public CannotObtainBlockLengthException() {
    super();
  }

  public CannotObtainBlockLengthException(String message){
    super(message);
  }

  /**
   * Constructs an {@code CannotObtainBlockLengthException} with the
   * specified LocatedBlock that failed to obtain block length.
   *
   * @param locatedBlock
   *        The LocatedBlock instance which block length can not be obtained
   */
  public CannotObtainBlockLengthException(LocatedBlock locatedBlock) {
    super("Cannot obtain block length for " + locatedBlock);
  }

  /**
   * Constructs an {@code CannotObtainBlockLengthException} with the
   * specified LocatedBlock and file that failed to obtain block length.
   *
   * @param locatedBlock
   *        The LocatedBlock instance which block length can not be obtained
   * @param src The file which include this block
   */
  public CannotObtainBlockLengthException(LocatedBlock locatedBlock,
      String src) {
    super("Cannot obtain block length for " + locatedBlock + " of " + src);
  }
}
