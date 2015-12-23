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

package org.apache.hadoop.yarn.api.records.timeline;

import java.util.Iterator;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import com.google.common.base.Splitter;

/**
 * <p><code>TimelineEntityGroupId</code> is an abstract way for
 * timeline service users to represent “a group of related timeline data.
 * For example, all entities that represents one data flow DAG execution
 * can be grouped into one timeline entity group. </p>
 */
@Public
@Unstable
public class TimelineEntityGroupId implements
    Comparable<TimelineEntityGroupId> {

  private static final Splitter SPLITTER = Splitter.on('_').trimResults();

  private ApplicationId applicationId;
  private String id;

  @Private
  @Unstable
  public static final String TIMELINE_ENTITY_GROUPID_STR_PREFIX =
      "timelineEntityGroupId";

  public TimelineEntityGroupId() {

  }

  public static TimelineEntityGroupId newInstance(ApplicationId applicationId,
      String id) {
    TimelineEntityGroupId timelineEntityGroupId =
        new TimelineEntityGroupId();
    timelineEntityGroupId.setApplicationId(applicationId);
    timelineEntityGroupId.setTimelineEntityGroupId(id);
    return timelineEntityGroupId;
  }

  /**
   * Get the <code>ApplicationId</code> of the
   * <code>TimelineEntityGroupId</code>.
   *
   * @return <code>ApplicationId</code> of the
   *         <code>TimelineEntityGroupId</code>
   */
  public ApplicationId getApplicationId() {
    return this.applicationId;
  }

  public void setApplicationId(ApplicationId appID) {
    this.applicationId = appID;
  }

  /**
   * Get the <code>timelineEntityGroupId</code>.
   *
   * @return <code>timelineEntityGroupId</code>
   */
  public String getTimelineEntityGroupId() {
    return this.id;
  }

  @Private
  @Unstable
  protected void setTimelineEntityGroupId(String timelineEntityGroupId) {
    this.id = timelineEntityGroupId;
  }

  @Override
  public int hashCode() {
    int result = getTimelineEntityGroupId().hashCode();
    result = 31 * result + getApplicationId().hashCode();
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    TimelineEntityGroupId otherObject = (TimelineEntityGroupId) obj;
    if (!this.getApplicationId().equals(otherObject.getApplicationId())) {
      return false;
    }
    if (!this.getTimelineEntityGroupId().equals(
        otherObject.getTimelineEntityGroupId())) {
      return false;
    }
    return true;
  }

  @Override
  public int compareTo(TimelineEntityGroupId other) {
    int compareAppIds =
        this.getApplicationId().compareTo(other.getApplicationId());
    if (compareAppIds == 0) {
      return this.getTimelineEntityGroupId().compareTo(
        other.getTimelineEntityGroupId());
    } else {
      return compareAppIds;
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(TIMELINE_ENTITY_GROUPID_STR_PREFIX + "_");
    ApplicationId appId = getApplicationId();
    sb.append(appId.getClusterTimestamp()).append("_");
    sb.append(appId.getId()).append("_");
    sb.append(getTimelineEntityGroupId());
    return sb.toString();
  }

  public static TimelineEntityGroupId
      fromString(String timelineEntityGroupIdStr) {
    StringBuffer buf = new StringBuffer();
    Iterator<String> it = SPLITTER.split(timelineEntityGroupIdStr).iterator();
    if (!it.next().equals(TIMELINE_ENTITY_GROUPID_STR_PREFIX)) {
      throw new IllegalArgumentException(
        "Invalid TimelineEntityGroupId prefix: " + timelineEntityGroupIdStr);
    }
    ApplicationId appId =
        ApplicationId.newInstance(Long.parseLong(it.next()),
          Integer.parseInt(it.next()));
    buf.append(it.next());
    while (it.hasNext()) {
      buf.append("_");
      buf.append(it.next());
    }
    return TimelineEntityGroupId.newInstance(appId, buf.toString());
  }
}
