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

/**
 * Abstract class for deriving exceptions related to filesystem constraints
 */
public abstract class FSLimitException extends QuotaExceededException {
  protected static final long serialVersionUID = 1L;

  protected FSLimitException() {}

  protected FSLimitException(String msg) {
    super(msg);
  }
  
  protected FSLimitException(long quota, long count) {
    super(quota, count);
  }

  /**
   * Path component length is too long
   */
  public static final
  class PathComponentTooLongException extends FSLimitException {
    protected static final long serialVersionUID = 1L;

    private String childName;

    protected PathComponentTooLongException() {}

    protected PathComponentTooLongException(String msg) {
      super(msg);
    }
    
    public PathComponentTooLongException(long quota, long count,
        String parentPath, String childName) {
      super(quota, count);
      setPathName(parentPath);
      this.childName = childName;
    }

    String getParentPath() {
      return pathName;
    }

    @Override
    public String getMessage() {
      return "The maximum path component name limit of " + childName +
      " in directory " + getParentPath() +
      " is exceeded: limit=" + quota + " length=" + count; 
    }
  }

  /**
   * Directory has too many items
   */
  public static final
  class MaxDirectoryItemsExceededException extends FSLimitException {
    protected static final long serialVersionUID = 1L;

    protected MaxDirectoryItemsExceededException() {}

    protected MaxDirectoryItemsExceededException(String msg) {
      super(msg);
    }
    
    public MaxDirectoryItemsExceededException(String path, long quota,
        long count) {
      super(quota, count);
      setPathName(path);
    }

    @Override
    public String getMessage() {
      return "The directory item limit of " + pathName +
      " is exceeded: limit=" + quota + " items=" + count; 
    }
  }
}
