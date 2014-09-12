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

package org.apache.hadoop.yarn.server.resourcemanager.recovery.records;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerRecoveryProtos.EpochProto;
import org.apache.hadoop.yarn.util.Records;

/**
 * The epoch information of RM for work-preserving restart.
 * Epoch is incremented each time RM restart. It's used for assuring
 * uniqueness of <code>ContainerId</code>.
 */
@Private
@Unstable
public abstract class Epoch {

  public static Epoch newInstance(long sequenceNumber) {
    Epoch epoch = Records.newRecord(Epoch.class);
    epoch.setEpoch(sequenceNumber);
    return epoch;
  }

  public abstract long getEpoch();

  public abstract void setEpoch(long sequenceNumber);

  public abstract EpochProto getProto();

  public String toString() {
    return String.valueOf(getEpoch());
  }

  @Override
  public int hashCode() {
    return (int) (getEpoch() ^ (getEpoch() >>> 32));
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    Epoch other = (Epoch) obj;
    if (this.getEpoch() == other.getEpoch()) {
      return true;
    } else {
      return false;
    }
  }
}
