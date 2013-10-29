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

import java.io.IOException;

/**
 * An exception which occurred when trying to remove a PathBasedCache entry.
 */
public abstract class RemovePathBasedCacheDescriptorException extends IOException {
  private static final long serialVersionUID = 1L;

  public RemovePathBasedCacheDescriptorException(String description) {
    super(description);
  }

  public final static class InvalidIdException
      extends RemovePathBasedCacheDescriptorException {
    private static final long serialVersionUID = 1L;

    public InvalidIdException(String msg) {
      super(msg);
    }

    public InvalidIdException(long entryId) {
      this("invalid PathBasedCacheDescriptor id " + entryId);
    }
  }

  public final static class RemovePermissionDeniedException
      extends RemovePathBasedCacheDescriptorException {
    private static final long serialVersionUID = 1L;

    public RemovePermissionDeniedException(String msg) {
      super(msg);
    }

    public RemovePermissionDeniedException(long entryId) {
      this("permission denied when trying to remove " + 
          "PathBasedCacheDescriptor id " + entryId);
    }
  }

  public final static class NoSuchIdException
      extends RemovePathBasedCacheDescriptorException {
    private static final long serialVersionUID = 1L;

    public NoSuchIdException(String msg) {
      super(msg);
    }

    public NoSuchIdException(long entryId) {
      this("there is no PathBasedCacheDescriptor with id " + entryId);
    }
  }

  public final static class UnexpectedRemovePathBasedCacheDescriptorException
      extends RemovePathBasedCacheDescriptorException {
    private static final long serialVersionUID = 1L;

    public UnexpectedRemovePathBasedCacheDescriptorException(String msg) {
      super(msg);
    }

    public UnexpectedRemovePathBasedCacheDescriptorException(long id) {
      this("encountered an unexpected error when trying to " +
          "remove PathBasedCacheDescriptor with id " + id);
    }
  }
}
