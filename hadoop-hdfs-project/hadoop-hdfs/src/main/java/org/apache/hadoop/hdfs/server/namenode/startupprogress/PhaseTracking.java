/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdfs.server.namenode.startupprogress;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * Internal data structure used to track progress of a {@link Phase}.
 */
@InterfaceAudience.Private
final class PhaseTracking extends AbstractTracking {
  String file;
  long size = Long.MIN_VALUE;
  final ConcurrentMap<Step, StepTracking> steps =
    new ConcurrentHashMap<Step, StepTracking>();

  @Override
  public PhaseTracking clone() {
    PhaseTracking clone = new PhaseTracking();
    super.copy(clone);
    clone.file = file;
    clone.size = size;
    for (Map.Entry<Step, StepTracking> entry: steps.entrySet()) {
      clone.steps.put(entry.getKey(), entry.getValue().clone());
    }
    return clone;
  }
}
