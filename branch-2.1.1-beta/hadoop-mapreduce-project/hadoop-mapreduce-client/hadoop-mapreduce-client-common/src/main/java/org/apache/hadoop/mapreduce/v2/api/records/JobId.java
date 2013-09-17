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

package org.apache.hadoop.mapreduce.v2.api.records;

import java.text.NumberFormat;
import org.apache.hadoop.yarn.api.records.ApplicationId;

/**
 * <p><code>JobId</code> represents the <em>globally unique</em> 
 * identifier for a MapReduce job.</p>
 * 
 * <p>The globally unique nature of the identifier is achieved by using the 
 * <em>cluster timestamp</em> from the associated ApplicationId. i.e. 
 * start-time of the <code>ResourceManager</code> along with a monotonically
 * increasing counter for the jobId.</p>
 */
public abstract class JobId implements Comparable<JobId> {

  /**
   * Get the associated <em>ApplicationId</em> which represents the 
   * start time of the <code>ResourceManager</code> and is used to generate 
   * the globally unique <code>JobId</code>.
   * @return associated <code>ApplicationId</code>
   */
  public abstract ApplicationId getAppId();
  
  /**
   * Get the short integer identifier of the <code>JobId</code>
   * which is unique for all applications started by a particular instance
   * of the <code>ResourceManager</code>.
   * @return short integer identifier of the <code>JobId</code>
   */
  public abstract int getId();
  
  public abstract void setAppId(ApplicationId appId);
  public abstract void setId(int id);


  protected static final String JOB = "job";
  protected static final char SEPARATOR = '_';
  static final ThreadLocal<NumberFormat> jobIdFormat =
      new ThreadLocal<NumberFormat>() {
        @Override
        public NumberFormat initialValue() {
          NumberFormat fmt = NumberFormat.getInstance();
          fmt.setGroupingUsed(false);
          fmt.setMinimumIntegerDigits(4);
          return fmt;
        }
      };

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder(JOB);
    builder.append(SEPARATOR);
    builder.append(getAppId().getClusterTimestamp());
    builder.append(SEPARATOR);
    builder.append(jobIdFormat.get().format(getId()));
    return builder.toString();
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + getAppId().hashCode();
    result = prime * result + getId();
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
    JobId other = (JobId) obj;
    if (!this.getAppId().equals(other.getAppId()))
      return false;
    if (this.getId() != other.getId())
      return false;
    return true;
  }

  @Override
  public int compareTo(JobId other) {
    int appIdComp = this.getAppId().compareTo(other.getAppId());
    if (appIdComp == 0) {
      return this.getId() - other.getId();
    } else {
      return appIdComp;
    }
  }
}