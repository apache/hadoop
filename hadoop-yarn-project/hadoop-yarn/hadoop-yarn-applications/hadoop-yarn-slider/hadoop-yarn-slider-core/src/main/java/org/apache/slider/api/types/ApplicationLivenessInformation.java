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

package org.apache.slider.api.types;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;

/**
 * Serialized information about liveness
 * <p>
 *   If true liveness probes are implemented, this
 *   datatype can be extended to publish their details.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
public class ApplicationLivenessInformation {
  /** flag set if the cluster is at size */
  public boolean allRequestsSatisfied;

  /** number of outstanding requests: those needed to satisfy */
  public int requestsOutstanding;

  /** number of requests submitted to YARN */
  public int activeRequests;

  @Override
  public String toString() {
    final StringBuilder sb =
        new StringBuilder("ApplicationLivenessInformation{");
    sb.append("allRequestsSatisfied=").append(allRequestsSatisfied);
    sb.append(", requestsOutstanding=").append(requestsOutstanding);
    sb.append('}');
    return sb.toString();
  }
}


