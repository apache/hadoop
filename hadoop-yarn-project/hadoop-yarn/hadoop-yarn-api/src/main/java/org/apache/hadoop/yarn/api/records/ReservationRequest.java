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

import java.io.Serializable;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.util.Records;

/**
 * {@link ReservationRequest} represents the request made by an application to
 * the {@code ResourceManager} to reserve {@link Resource}s.
 * <p>
 * It includes:
 * <ul>
 *   <li>{@link Resource} required for each request.</li>
 *   <li>
 *     Number of containers, of above specifications, which are required by the
 *     application.
 *   </li>
 *   <li>Concurrency that indicates the gang size of the request.</li>
 * </ul>
 */
@Public
@Unstable
public abstract class ReservationRequest implements
    Comparable<ReservationRequest> {

  @Public
  @Unstable
  public static ReservationRequest newInstance(Resource capability,
      int numContainers) {
    return newInstance(capability, numContainers, 1, -1);
  }

  @Public
  @Unstable
  public static ReservationRequest newInstance(Resource capability,
      int numContainers, int concurrency, long duration) {
    ReservationRequest request = Records.newRecord(ReservationRequest.class);
    request.setCapability(capability);
    request.setNumContainers(numContainers);
    request.setConcurrency(concurrency);
    request.setDuration(duration);
    return request;
  }

  @Public
  @Unstable
  public static class ReservationRequestComparator implements
      java.util.Comparator<ReservationRequest>, Serializable {

    private static final long serialVersionUID = 1L;

    @Override
    public int compare(ReservationRequest r1, ReservationRequest r2) {
      // Compare numContainers, concurrency and capability
      int ret = r1.getNumContainers() - r2.getNumContainers();
      if (ret == 0) {
        ret = r1.getConcurrency() - r2.getConcurrency();
      }
      if (ret == 0) {
        ret = r1.getCapability().compareTo(r2.getCapability());
      }
      return ret;
    }
  }

  /**
   * Get the {@link Resource} capability of the request.
   * 
   * @return {@link Resource} capability of the request
   */
  @Public
  @Unstable
  public abstract Resource getCapability();

  /**
   * Set the {@link Resource} capability of the request
   * 
   * @param capability {@link Resource} capability of the request
   */
  @Public
  @Unstable
  public abstract void setCapability(Resource capability);

  /**
   * Get the number of containers required with the given specifications.
   * 
   * @return number of containers required with the given specifications
   */
  @Public
  @Unstable
  public abstract int getNumContainers();

  /**
   * Set the number of containers required with the given specifications
   * 
   * @param numContainers number of containers required with the given
   *          specifications
   */
  @Public
  @Unstable
  public abstract void setNumContainers(int numContainers);

  /**
   * Get the number of containers that need to be scheduled concurrently. The
   * default value of 1 would fall back to the current non concurrency
   * constraints on the scheduling behavior.
   * 
   * @return the number of containers to be concurrently scheduled
   */
  @Public
  @Unstable
  public abstract int getConcurrency();

  /**
   * Set the number of containers that need to be scheduled concurrently. The
   * default value of 1 would fall back to the current non concurrency
   * constraints on the scheduling behavior.
   * 
   * @param numContainers the number of containers to be concurrently scheduled
   */
  @Public
  @Unstable
  public abstract void setConcurrency(int numContainers);

  /**
   * Get the duration in milliseconds for which the resource is required. A
   * default value of -1, indicates an unspecified lease duration, and fallback
   * to current behavior.
   * 
   * @return the duration in milliseconds for which the resource is required
   */
  @Public
  @Unstable
  public abstract long getDuration();

  /**
   * Set the duration in milliseconds for which the resource is required.
   * 
   * @param duration the duration in milliseconds for which the resource is
   *          required
   */
  @Public
  @Unstable
  public abstract void setDuration(long duration);

  @Override
  public int hashCode() {
    final int prime = 2153;
    int result = 2459;
    Resource capability = getCapability();
    result =
        prime * result + ((capability == null) ? 0 : capability.hashCode());
    result = prime * result + getNumContainers();
    result = prime * result + getConcurrency();
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
    ReservationRequest other = (ReservationRequest) obj;
    Resource capability = getCapability();
    if (capability == null) {
      if (other.getCapability() != null)
        return false;
    } else if (!capability.equals(other.getCapability()))
      return false;
    if (getNumContainers() != other.getNumContainers())
      return false;
    if (getConcurrency() != other.getConcurrency())
      return false;
    return true;
  }

  @Override
  public int compareTo(ReservationRequest other) {
    int numContainersComparison =
        this.getNumContainers() - other.getNumContainers();
    if (numContainersComparison == 0) {
      int concurrencyComparison =
          this.getConcurrency() - other.getConcurrency();
      if (concurrencyComparison == 0) {
        return this.getCapability().compareTo(other.getCapability());
      } else {
        return concurrencyComparison;
      }
    } else {
      return numContainersComparison;
    }
  }

}
