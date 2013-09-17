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
package org.apache.hadoop.nfs;

/**
 * Class encapsulates different types of files
 */
public enum NfsFileType {
  NFSREG(1),    // a regular file
  NFSDIR(2),    // a directory
  NFSBLK(3),    // a block special device file
  NFSCHR(4),    // a character special device
  NFSLNK(5),    // a symbolic link
  NFSSOCK(6),   // a socket
  NFSFIFO(7);   // a named pipe
  
  private final int value;
  
  NfsFileType(int val) {
    value = val;
  }
  
  public int toValue() {
    return value;
  }
}
