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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy;

import java.util.*;

//Some policies will use multiple comparators joined together
class CompoundComparator implements Comparator<SchedulableEntity> {

    List<Comparator<SchedulableEntity>> comparators;

    CompoundComparator(List<Comparator<SchedulableEntity>> comparators) {
      this.comparators = comparators;
    }

    @Override
    public int compare(final SchedulableEntity r1, final SchedulableEntity r2) {
      for (Comparator<SchedulableEntity> comparator : comparators) {
        int result = comparator.compare(r1, r2);
        if (result != 0) return result;
      }
      return 0;
    }
}
