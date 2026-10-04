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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.ConfigSnapshot;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.CSConfigValidator;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ClusterFacts;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.ValidationResult;

/**
 * The {@code validate/v2} core for {@link TestCapacitySchedulerConfigBenchmark}
 * ({@code -Dcs.bench.validators=legacy,<this class name>}): captures the
 * cluster facts and runs the validation engine, failing on an ERROR.
 */
public class EngineProposalValidator
    implements TestCapacitySchedulerConfigBenchmark.ProposalValidator {

  @Override
  public void validate(RMContext rmContext, Configuration live,
      Configuration proposed) throws Exception {
    ClusterFacts facts =
        ClusterFacts.capture((CapacityScheduler) rmContext.getScheduler());
    ValidationResult result = new CSConfigValidator().validate(
        ConfigSnapshot.of(proposed), facts);
    CapacitySchedulerConfigValidator.throwIfInvalid(result);
  }
}
