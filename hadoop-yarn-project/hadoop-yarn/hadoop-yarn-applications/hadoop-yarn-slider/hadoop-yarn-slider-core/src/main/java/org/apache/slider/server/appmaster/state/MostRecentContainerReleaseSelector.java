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

package org.apache.slider.server.appmaster.state;

import org.apache.slider.common.tools.Comparators;

import java.io.Serializable;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Sort the candidate list by the most recent container first.
 */
public class MostRecentContainerReleaseSelector implements ContainerReleaseSelector {

  @Override
  public List<RoleInstance> sortCandidates(int roleId,
      List<RoleInstance> candidates) {
    Collections.sort(candidates, new newerThan());
    return candidates;
  }

  private static class newerThan implements Comparator<RoleInstance>, Serializable {
    private final Comparator<Long> innerComparator =
        new Comparators.ComparatorReverser<>(new Comparators.LongComparator());
    public int compare(RoleInstance o1, RoleInstance o2) {
      return innerComparator.compare(o1.createTime, o2.createTime);

    }
    
  }
  
  
}
