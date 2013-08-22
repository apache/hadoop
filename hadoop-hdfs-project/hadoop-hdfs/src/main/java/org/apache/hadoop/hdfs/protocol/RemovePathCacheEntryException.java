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

import com.google.common.base.Preconditions;

/**
 * An exception which occurred when trying to remove a path cache entry.
 */
public abstract class RemovePathCacheEntryException extends IOException {
  private static final long serialVersionUID = 1L;

  private final long entryId;

  public RemovePathCacheEntryException(String description, long entryId) {
    super(description);
    this.entryId = entryId;
  }
    
  public long getEntryId() {
    return this.entryId;
  }

  public final static class InvalidIdException
      extends RemovePathCacheEntryException {
    private static final long serialVersionUID = 1L;

    public InvalidIdException(long entryId) {
      super("invalid cache path entry id " + entryId, entryId);
    }
  }

  public final static class NoSuchIdException
      extends RemovePathCacheEntryException {
    private static final long serialVersionUID = 1L;

    public NoSuchIdException(long entryId) {
      super("there is no path cache entry with id " + entryId, entryId);
    }
  }

  public final static class UnexpectedRemovePathCacheEntryException
      extends RemovePathCacheEntryException {
    private static final long serialVersionUID = 1L;

    public UnexpectedRemovePathCacheEntryException(long id) {
      super("encountered an unexpected error when trying to " +
          "remove path cache entry id " + id, id);
    }
  }
}
