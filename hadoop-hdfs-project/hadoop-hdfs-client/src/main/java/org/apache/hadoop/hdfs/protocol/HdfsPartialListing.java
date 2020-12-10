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

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.ipc.RemoteException;

import java.util.List;

/**
 * A partial listing returned by the batched listing API. This is used
 * internally by the HDFS client and namenode and is not meant for public
 * consumption.
 */
@InterfaceAudience.Private
public class HdfsPartialListing {

  private final List<HdfsFileStatus> partialListing;
  private final int parentIdx;
  private final RemoteException exception;

  public HdfsPartialListing(
      int parentIdx,
      List<HdfsFileStatus> partialListing) {
    this(parentIdx, partialListing, null);
  }

  public HdfsPartialListing(
      int parentIdx,
      RemoteException exception) {
    this(parentIdx, null, exception);
  }

  private HdfsPartialListing(
      int parentIdx,
      List<HdfsFileStatus> partialListing,
      RemoteException exception) {
    Preconditions.checkArgument(partialListing == null ^ exception == null);
    this.parentIdx = parentIdx;
    this.partialListing = partialListing;
    this.exception = exception;
  }

  public int getParentIdx() {
    return parentIdx;
  }

  public List<HdfsFileStatus> getPartialListing() {
    return partialListing;
  }

  public RemoteException getException() {
    return exception;
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this)
        .append("partialListing", partialListing)
        .append("parentIdx", parentIdx)
        .append("exception", exception)
        .toString();
  }
}
