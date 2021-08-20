/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.protocol;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.server.common.ProvidedVolumeInfo;

import static org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol.DNA_PROVIDEDVOLADD;
import static org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol.DNA_PROVIDEDVOLREMOVE;

/**
 * The command to add/remove provided volume.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ProvidedVolumeCommand extends DatanodeCommand {
  private final ProvidedVolumeInfo volume;

  ProvidedVolumeCommand(ProvidedVolumeInfo volume, int actionId) {
    super(actionId);
    this.volume = volume;
  }

  /**
   * @param volume The provided volume to be added.
   * @return Returns a command instance with the appropriate action id.
   */
  public static ProvidedVolumeCommand buildAddCmd(ProvidedVolumeInfo volume) {
    return new ProvidedVolumeCommand(volume, DNA_PROVIDEDVOLADD);
  }

  /**
   * @param volume The provided volume to be removed.
   * @return Returns a command instance with the appropriate action id.
   */
  public static ProvidedVolumeCommand buildRemoveCmd(
      ProvidedVolumeInfo volume) {
    return new ProvidedVolumeCommand(volume, DNA_PROVIDEDVOLREMOVE);
  }

  @VisibleForTesting
  public ProvidedVolumeInfo getProvidedVolume() {
    return volume;
  }
}