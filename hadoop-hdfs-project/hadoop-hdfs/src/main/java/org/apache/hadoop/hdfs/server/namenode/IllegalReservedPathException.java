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

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * Thrown when upgrading from software release that doesn't support reserved
 * path to software release that supports reserved path, and when there is 
 * reserved path name in the Fsimage.
 */
@InterfaceAudience.Private
public class IllegalReservedPathException extends IOException {
  private static final long serialVersionUID = 1L;
  
  public IllegalReservedPathException(String message, Throwable cause) {
    super(message, cause);
  }
  public IllegalReservedPathException(String message) {
    super(message);
  }
}
