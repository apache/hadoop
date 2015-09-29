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
import org.apache.hadoop.conf.Configuration;

/**
 * The public API for creating a new ReplicaAccessor.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class ReplicaAccessorBuilder {
  /**
   * Set the file name which is being opened.  Provided for debugging purposes.
   */
  public abstract ReplicaAccessorBuilder setFileName(String fileName);

  /** Set the block ID and block pool ID which are being opened. */
  public abstract ReplicaAccessorBuilder
      setBlock(long blockId, String blockPoolId);

  /** Set the genstamp of the block which is being opened. */
  public abstract ReplicaAccessorBuilder setGenerationStamp(long genstamp);

  /**
   * Set whether checksums must be verified.  Checksums should be skipped if
   * the user has disabled checksum verification in the configuration.  Users
   * may wish to do this if their software does checksum verification at a
   * higher level than HDFS.
   */
  public abstract ReplicaAccessorBuilder
      setVerifyChecksum(boolean verifyChecksum);

  /** Set the name of the HDFS client.  Provided for debugging purposes. */
  public abstract ReplicaAccessorBuilder setClientName(String clientName);

  /**
   * Set whether short-circuit is enabled.  Short-circuit may be disabled if
   * the user has set dfs.client.read.shortcircuit to false, or if the block
   * being read is under construction.  The fact that this bit is enabled does
   * not mean that the user has permission to do short-circuit reads or to
   * access the replica-- that must be checked separately by the
   * ReplicaAccessorBuilder implementation.
   */
  public abstract ReplicaAccessorBuilder
      setAllowShortCircuitReads(boolean allowShortCircuit);

  /**
   * Set the length of the replica which is visible to this client.  If bytes
   * are added later, they will not be visible to the ReplicaAccessor we are
   * building.  In order to see more of the replica, the client must re-open
   * this HDFS file.  The visible length provides an upper bound, but not a
   * lower one.  If the replica is deleted or truncated, fewer bytes may be
   * visible than specified here.
   */
  public abstract ReplicaAccessorBuilder setVisibleLength(long visibleLength);

  /**
   * Set the configuration to use.  ReplicaAccessorBuilder subclasses should
   * define their own configuration prefix.  For example, the foobar plugin
   * could look for configuration keys like foo.bar.parameter1,
   * foo.bar.parameter2.
   */
  public abstract ReplicaAccessorBuilder setConfiguration(Configuration conf);

  /**
   * Set the block access token to use.
   */
  public abstract ReplicaAccessorBuilder setBlockAccessToken(byte[] token);

  /**
   * Build a new ReplicaAccessor.
   *
   * The implementation must perform any necessary access checks before
   * constructing the ReplicaAccessor.  If there is a hardware-level or
   * network-level setup operation that could fail, it should be done here.  If
   * the implementation returns a ReplicaAccessor, we will assume that it works
   * and not attempt to construct a normal BlockReader.
   *
   * If the ReplicaAccessor could not be built, implementations may wish to log
   * a message at TRACE level indicating why.
   *
   * @return    null if the ReplicaAccessor could not be built; the
   *                ReplicaAccessor otherwise.
   */
  public abstract ReplicaAccessor build();
}
