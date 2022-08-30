/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.sls;

import org.apache.hadoop.yarn.api.records.ReservationId;
import java.util.Map;

public class JobDefinition {
  private AMDefinition amDefinition;
  private ReservationId reservationId;
  private long deadline;
  private Map<String, String> params;

  public AMDefinition getAmDefinition() {
    return amDefinition;
  }

  public ReservationId getReservationId() {
    return reservationId;
  }

  public long getDeadline() {
    return deadline;
  }

  //Currently unused
  public Map<String, String> getParams() {
    return params;
  }

  public static final class Builder {
    private AMDefinition amDefinition;
    private ReservationId reservationId;
    private long deadline;
    private Map<String, String> params;

    private Builder() {
    }

    public static Builder create() {
      return new Builder();
    }

    public Builder withAmDefinition(AMDefinition amDefinition) {
      this.amDefinition = amDefinition;
      return this;
    }

    public Builder withReservationId(ReservationId reservationId) {
      this.reservationId = reservationId;
      return this;
    }

    public Builder withDeadline(long deadline) {
      this.deadline = deadline;
      return this;
    }

    public Builder withParams(Map<String, String> params) {
      this.params = params;
      return this;
    }

    public JobDefinition build() {
      JobDefinition jobDef = new JobDefinition();
      jobDef.params = this.params;
      jobDef.amDefinition = this.amDefinition;
      jobDef.reservationId = this.reservationId;
      jobDef.deadline = this.deadline;
      return jobDef;
    }
  }
}
