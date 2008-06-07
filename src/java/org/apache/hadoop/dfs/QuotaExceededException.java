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

package org.apache.hadoop.dfs;

import java.io.IOException;

/** This class is for the error when an attempt to add an inode to namespace 
 * violates the quota restriction of any inode on the path to the newly added
 * inode.
 */
public final class QuotaExceededException extends IOException {
  private static final long serialVersionUID = 1L;
  private String pathName;
  private long quota;
  private long count;
  
  public QuotaExceededException(String msg) {
    super(msg);
  }
  
  public QuotaExceededException(long quota, long count) {
    this.quota = quota;
    this.count = count;
  }
  
  public void setPathName(String path) {
    this.pathName = path;
  }
  
  public String getMessage() {
    String msg = super.getMessage();
    if (msg == null) {
      return "The quota" + (pathName==null?"":(" of " + pathName)) + 
          " is exceeded: quota=" + quota + " count=" + count;
    } else {
      return msg;
    }
  }
}
