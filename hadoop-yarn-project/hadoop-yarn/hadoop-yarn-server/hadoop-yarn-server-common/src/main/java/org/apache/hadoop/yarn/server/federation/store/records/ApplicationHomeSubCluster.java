/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.yarn.server.federation.store.records;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.util.Records;

/**
 * <p>
 * ApplicationHomeSubCluster is a report of the runtime information of the
 * application that is running in the federated cluster.
 *
 * <p>
 * It includes information such as:
 * <ul>
 * <li>{@link ApplicationId}</li>
 * <li>{@link SubClusterId}</li>
 * </ul>
 *
 */
@Private
@Unstable
public abstract class ApplicationHomeSubCluster {

  @Private
  @Unstable
  public static ApplicationHomeSubCluster newInstance(ApplicationId appId,
      SubClusterId homeSubCluster) {
    ApplicationHomeSubCluster appMapping =
        Records.newRecord(ApplicationHomeSubCluster.class);
    appMapping.setApplicationId(appId);
    appMapping.setHomeSubCluster(homeSubCluster);
    return appMapping;
  }

  @Private
  @Unstable
  public static ApplicationHomeSubCluster newInstance(ApplicationId appId, long createTime,
      SubClusterId homeSubCluster) {
    ApplicationHomeSubCluster appMapping = Records.newRecord(ApplicationHomeSubCluster.class);
    appMapping.setApplicationId(appId);
    appMapping.setHomeSubCluster(homeSubCluster);
    appMapping.setCreateTime(createTime);
    return appMapping;
  }

  @Private
  @Unstable
  public static ApplicationHomeSubCluster newInstance(ApplicationId appId, long createTime,
      SubClusterId homeSubCluster, ApplicationSubmissionContext appSubmissionContext) {
    ApplicationHomeSubCluster appMapping = Records.newRecord(ApplicationHomeSubCluster.class);
    appMapping.setApplicationId(appId);
    appMapping.setHomeSubCluster(homeSubCluster);
    appMapping.setApplicationSubmissionContext(appSubmissionContext);
    appMapping.setCreateTime(createTime);
    return appMapping;
  }

  @Private
  @Unstable
  public static ApplicationHomeSubCluster newInstance(ApplicationId appId,
      SubClusterId homeSubCluster, ApplicationSubmissionContext appSubmissionContext) {
    ApplicationHomeSubCluster appMapping = Records.newRecord(ApplicationHomeSubCluster.class);
    appMapping.setApplicationId(appId);
    appMapping.setHomeSubCluster(homeSubCluster);
    appMapping.setApplicationSubmissionContext(appSubmissionContext);
    return appMapping;
  }

  /**
   * Get the {@link ApplicationId} representing the unique identifier of the
   * application.
   *
   * @return the application identifier
   */
  @Public
  @Unstable
  public abstract ApplicationId getApplicationId();

  /**
   * Set the {@link ApplicationId} representing the unique identifier of the
   * application.
   *
   * @param applicationId the application identifier
   */
  @Private
  @Unstable
  public abstract void setApplicationId(ApplicationId applicationId);

  /**
   * Get the {@link SubClusterId} representing the unique identifier of the home
   * subcluster in which the ApplicationMaster of the application is running.
   *
   * @return the home subcluster identifier
   */
  @Public
  @Unstable
  public abstract SubClusterId getHomeSubCluster();

  /**
   * Set the {@link SubClusterId} representing the unique identifier of the home
   * subcluster in which the ApplicationMaster of the application is running.
   *
   * @param homeSubCluster the home subcluster identifier
   */
  @Private
  @Unstable
  public abstract void setHomeSubCluster(SubClusterId homeSubCluster);

  /**
   * Get the create time of the subcluster.
   *
   * @return the state of the subcluster
   */
  @Public
  @Unstable
  public abstract long getCreateTime();

  /**
   * Set the create time of the subcluster.
   *
   * @param time the last heartbeat time of the subcluster
   */
  @Private
  @Unstable
  public abstract void setCreateTime(long time);


  /**
   * Set Application Submission Context.
   *
   * @param context Application Submission Context.
   */
  @Private
  @Unstable
  public abstract void setApplicationSubmissionContext(ApplicationSubmissionContext context);

  /**
   * Get Application Submission Context.
   *
   * @return Application Submission Context.
   */
  @Private
  @Unstable
  public abstract ApplicationSubmissionContext getApplicationSubmissionContext();

  @Override
  public boolean equals(Object obj) {

    if (this == obj) {
      return true;
    }

    if (obj == null) {
      return false;
    }

    if (obj instanceof ApplicationHomeSubCluster) {
      ApplicationHomeSubCluster other = (ApplicationHomeSubCluster) obj;
      return new EqualsBuilder()
          .append(this.getApplicationId(), other.getApplicationId())
          .append(this.getHomeSubCluster(), other.getHomeSubCluster())
          .append(this.getApplicationSubmissionContext(),
          other.getApplicationSubmissionContext())
          .isEquals();
    }

    return false;
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder().
        append(this.getApplicationId()).
        append(this.getHomeSubCluster()).
        append(this.getCreateTime()).
        append(this.getApplicationSubmissionContext())
        .toHashCode();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("ApplicationHomeSubCluster: [")
        .append("ApplicationId: ").append(getApplicationId()).append(", ")
        .append("HomeSubCluster: ").append(getHomeSubCluster()).append(", ")
        .append("CreateTime: ").append(getCreateTime()).append(", ")
        .append("ApplicationSubmissionContext: ").append(getApplicationSubmissionContext())
        .append("]");
    return sb.toString();
  }
}
