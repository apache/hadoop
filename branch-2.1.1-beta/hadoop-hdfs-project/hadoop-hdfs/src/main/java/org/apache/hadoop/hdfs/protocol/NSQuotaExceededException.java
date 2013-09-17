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

@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class NSQuotaExceededException extends QuotaExceededException {
  protected static final long serialVersionUID = 1L;
  
  private String prefix;
  
  public NSQuotaExceededException() {}

  public NSQuotaExceededException(String msg) {
    super(msg);
  }
  
  public NSQuotaExceededException(long quota, long count) {
    super(quota, count);
  }

  @Override
  public String getMessage() {
    String msg = super.getMessage();
    if (msg == null) {
      msg = "The NameSpace quota (directories and files)" + 
      (pathName==null?"":(" of directory " + pathName)) + 
          " is exceeded: quota=" + quota + " file count=" + count; 

      if (prefix != null) {
        msg = prefix + ": " + msg;
      }
    }
    return msg;
  }

  /** Set a prefix for the error message. */
  public void setMessagePrefix(final String prefix) {
    this.prefix = prefix;
  }
}
