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

package org.apache.hadoop.yarn.api.records;

import com.google.common.base.Splitter;

import java.text.NumberFormat;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.util.Records;

/**
 * <p><code>ContainerId</code> represents a globally unique identifier
 * for a {@link Container} in the cluster.</p>
 */
@Public
@Stable
public abstract class ContainerId implements Comparable<ContainerId>{
  public static final long CONTAINER_ID_BITMASK = 0xffffffffffL;
  private static final Splitter _SPLITTER = Splitter.on('_').trimResults();
  private static final String CONTAINER_PREFIX = "container";
  private static final String EPOCH_PREFIX = "e";

  @Private
  @Unstable
  public static ContainerId newContainerId(ApplicationAttemptId appAttemptId,
      long containerId) {
    ContainerId id = Records.newRecord(ContainerId.class);
    id.setContainerId(containerId);
    id.setApplicationAttemptId(appAttemptId);
    id.build();
    return id;
  }

  @Private
  @Deprecated
  @Unstable
  public static ContainerId newInstance(ApplicationAttemptId appAttemptId,
      int containerId) {
    ContainerId id = Records.newRecord(ContainerId.class);
    id.setContainerId(containerId);
    id.setApplicationAttemptId(appAttemptId);
    id.build();
    return id;
  }

  /**
   * Get the <code>ApplicationAttemptId</code> of the application to which the
   * <code>Container</code> was assigned.
   * <p>
   * Note: If containers are kept alive across application attempts via
   * {@link ApplicationSubmissionContext#setKeepContainersAcrossApplicationAttempts(boolean)}
   * the <code>ContainerId</code> does not necessarily contain the current
   * running application attempt's <code>ApplicationAttemptId</code> This
   * container can be allocated by previously exited application attempt and
   * managed by the current running attempt thus have the previous application
   * attempt's <code>ApplicationAttemptId</code>.
   * </p>
   * 
   * @return <code>ApplicationAttemptId</code> of the application to which the
   *         <code>Container</code> was assigned
   */
  @Public
  @Stable
  public abstract ApplicationAttemptId getApplicationAttemptId();
  
  @Private
  @Unstable
  protected abstract void setApplicationAttemptId(ApplicationAttemptId atId);

  /**
   * Get the lower 32 bits of identifier of the <code>ContainerId</code>,
   * which doesn't include epoch. Note that this method will be marked as
   * deprecated, so please use <code>getContainerId</code> instead.
   * @return lower 32 bits of identifier of the <code>ContainerId</code>
   */
  @Public
  @Deprecated
  @Stable
  public abstract int getId();

  /**
   * Get the identifier of the <code>ContainerId</code>. Upper 24 bits are
   * reserved as epoch of cluster, and lower 40 bits are reserved as
   * sequential number of containers.
   * @return identifier of the <code>ContainerId</code>
   */
  @Public
  @Unstable
  public abstract long getContainerId();

  @Private
  @Unstable
  protected abstract void setContainerId(long id);
 
  
  // TODO: fail the app submission if attempts are more than 10 or something
  private static final ThreadLocal<NumberFormat> appAttemptIdAndEpochFormat =
      new ThreadLocal<NumberFormat>() {
        @Override
        public NumberFormat initialValue() {
          NumberFormat fmt = NumberFormat.getInstance();
          fmt.setGroupingUsed(false);
          fmt.setMinimumIntegerDigits(2);
          return fmt;
        }
      };
  // TODO: Why thread local?
  // ^ NumberFormat instances are not threadsafe
  private static final ThreadLocal<NumberFormat> containerIdFormat =
      new ThreadLocal<NumberFormat>() {
        @Override
        public NumberFormat initialValue() {
          NumberFormat fmt = NumberFormat.getInstance();
          fmt.setGroupingUsed(false);
          fmt.setMinimumIntegerDigits(6);
          return fmt;
        }
      };

  @Override
  public int hashCode() {
    // Generated by IntelliJ IDEA 13.1.
    int result = (int) (getContainerId() ^ (getContainerId() >>> 32));
    result = 31 * result + getApplicationAttemptId().hashCode();
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    ContainerId other = (ContainerId) obj;
    if (!this.getApplicationAttemptId().equals(other.getApplicationAttemptId()))
      return false;
    if (this.getContainerId() != other.getContainerId())
      return false;
    return true;
  }

  @Override
  public int compareTo(ContainerId other) {
    int result = this.getApplicationAttemptId().compareTo(
        other.getApplicationAttemptId());
    if (result == 0) {
      return Long.compare(getContainerId(), other.getContainerId());
    } else {
      return result;
    }
  }

  /**
   * @return A string representation of containerId. The format is
   * container_e*epoch*_*clusterTimestamp*_*appId*_*attemptId*_*containerId*
   * when epoch is larger than 0
   * (e.g. container_e17_1410901177871_0001_01_000005).
   * *epoch* is increased when RM restarts or fails over.
   * When epoch is 0, epoch is omitted
   * (e.g. container_1410901177871_0001_01_000005).
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(CONTAINER_PREFIX + "_");
    long epoch = getContainerId() >> 40;
    if (epoch > 0) {
      sb.append(EPOCH_PREFIX)
          .append(appAttemptIdAndEpochFormat.get().format(epoch)).append("_");;
    }
    ApplicationId appId = getApplicationAttemptId().getApplicationId();
    sb.append(appId.getClusterTimestamp()).append("_");
    sb.append(ApplicationId.appIdFormat.get().format(appId.getId()))
        .append("_");
    sb.append(
        appAttemptIdAndEpochFormat.get().format(
            getApplicationAttemptId().getAttemptId())).append("_");
    sb.append(containerIdFormat.get()
        .format(CONTAINER_ID_BITMASK & getContainerId()));
    return sb.toString();
  }

  @Public
  @Unstable
  public static ContainerId fromString(String containerIdStr) {
    Iterator<String> it = _SPLITTER.split(containerIdStr).iterator();
    if (!it.next().equals(CONTAINER_PREFIX)) {
      throw new IllegalArgumentException("Invalid ContainerId prefix: "
          + containerIdStr);
    }
    try {
      String epochOrClusterTimestampStr = it.next();
      long epoch = 0;
      ApplicationAttemptId appAttemptID = null;
      if (epochOrClusterTimestampStr.startsWith(EPOCH_PREFIX)) {
        String epochStr = epochOrClusterTimestampStr;
        epoch = Integer.parseInt(epochStr.substring(EPOCH_PREFIX.length()));
        appAttemptID = toApplicationAttemptId(it);
      } else {
        String clusterTimestampStr = epochOrClusterTimestampStr;
        long clusterTimestamp = Long.parseLong(clusterTimestampStr);
        appAttemptID = toApplicationAttemptId(clusterTimestamp, it);
      }
      long id = Long.parseLong(it.next());
      long cid = (epoch << 40) | id;
      ContainerId containerId = ContainerId.newContainerId(appAttemptID, cid);
      return containerId;
    } catch (NumberFormatException n) {
      throw new IllegalArgumentException("Invalid ContainerId: "
          + containerIdStr, n);
    } catch (NoSuchElementException e) {
      throw new IllegalArgumentException("Invalid ContainerId: "
          + containerIdStr, e);
    }
  }

  private static ApplicationAttemptId toApplicationAttemptId(
      Iterator<String> it) throws NumberFormatException {
    return toApplicationAttemptId(Long.parseLong(it.next()), it);
  }

  private static ApplicationAttemptId toApplicationAttemptId(
      long clusterTimestamp, Iterator<String> it) throws NumberFormatException {
    ApplicationId appId = ApplicationId.newInstance(clusterTimestamp,
        Integer.parseInt(it.next()));
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(appId, Integer.parseInt(it.next()));
    return appAttemptId;
  }

  protected abstract void build();
}
