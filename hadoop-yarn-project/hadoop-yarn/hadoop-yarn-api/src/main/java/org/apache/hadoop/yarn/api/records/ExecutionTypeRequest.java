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

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.yarn.util.Records;

/**
 * An object of this class represents a specification of the execution
 * guarantee of the Containers associated with a ResourceRequest. It consists
 * of an <code>ExecutionType</code> as well as flag that explicitly asks the
 * configuredScheduler to return Containers of exactly the Execution Type
 * requested.
 */
@Public
@Evolving
public abstract class ExecutionTypeRequest {

  @Public
  @Evolving
  public static ExecutionTypeRequest newInstance() {
    return newInstance(ExecutionType.GUARANTEED, false);
  }

  @Public
  @Evolving
  public static ExecutionTypeRequest newInstance(ExecutionType execType,
      boolean ensureExecutionType) {
    ExecutionTypeRequest executionTypeRequest =
        Records.newRecord(ExecutionTypeRequest.class);
    executionTypeRequest.setExecutionType(execType);
    executionTypeRequest.setEnforceExecutionType(ensureExecutionType);
    return executionTypeRequest;
  }

  /**
   * Set the <code>ExecutionType</code> of the requested container.
   *
   * @param execType
   *          ExecutionType of the requested container
   */
  @Public
  public abstract void setExecutionType(ExecutionType execType);

  /**
   * Get <code>ExecutionType</code>.
   *
   * @return <code>ExecutionType</code>.
   */
  @Public
  public abstract ExecutionType getExecutionType();

  /**
   * Set to true to explicitly ask that the Scheduling Authority return
   * Containers of exactly the Execution Type requested.
   * @param enforceExecutionType whether ExecutionType request should be
   *                            strictly honored.
   */
  @Public
  public abstract void setEnforceExecutionType(boolean enforceExecutionType);


  /**
   * Get whether Scheduling Authority should return Containers of exactly the
   * Execution Type requested for this <code>ResourceRequest</code>.
   * Defaults to false.
   * @return whether ExecutionType request should be strictly honored
   */
  @Public
  public abstract boolean getEnforceExecutionType();

  @Override
  public int hashCode() {
    final int prime = 2153;
    int result = 2459;
    ExecutionType executionType = getExecutionType();
    boolean ensureExecutionType = getEnforceExecutionType();
    result = prime * result + ((executionType == null) ? 0 :
        executionType.hashCode());
    result = prime * result + (ensureExecutionType ? 0 : 1);
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
    ExecutionTypeRequest other = (ExecutionTypeRequest) obj;
    ExecutionType executionType = getExecutionType();
    if (executionType == null) {
      if (other.getExecutionType() != null) {
        return false;
      }
    } else if (executionType != other.getExecutionType()) {
      return false;
    }
    boolean enforceExecutionType = getEnforceExecutionType();
    return enforceExecutionType == other.getEnforceExecutionType();
  }
}
