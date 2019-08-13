/*
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

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.ipc.RemoteException;

import java.io.IOException;
import java.util.List;

/**
 * A partial listing of the children of a parent directory. Since it is a
 * partial listing, multiple ListingBatches may need to be combined to obtain
 * the full listing of a parent directory.
 * <p/>
 * ListingBatch behaves similar to a Future, in that getting the result via
 * {@link #get()} will throw an Exception if there was a failure.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class PartialListing<T extends FileStatus> {
  private final Path parent;
  private final List<T> partialListing;
  private final RemoteException exception;

  public PartialListing(Path parent, List<T> partialListing) {
    this(parent, partialListing, null);
  }

  public PartialListing(Path parent, RemoteException exception) {
    this(parent, null, exception);
  }

  private PartialListing(Path parent, List<T> partialListing,
      RemoteException exception) {
    Preconditions.checkArgument(partialListing == null ^ exception == null);
    this.partialListing = partialListing;
    this.parent = parent;
    this.exception = exception;
  }

  /**
   * Partial listing of the path being listed.
   *
   * @return Partial listing of the path being listed.
   * @throws IOException if there was an exception getting the listing.
   */
  public List<T> get() throws IOException {
    if (exception != null) {
      throw exception.unwrapRemoteException();
    }
    return partialListing;
  }

  /**
   * Path being listed.
   *
   * @return parent path.
   */
  public Path getParent() {
    return parent;
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this)
        .append("parent", parent)
        .append("partialListing", partialListing)
        .append("exception", exception)
        .toString();
  }
}
