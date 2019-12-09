/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.algorithm.iterators;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.hadoop.yarn.api.records.SchedulingRequest;

/**
 * Traverse Scheduling Requests in the same order as they arrive
 */
public class SerialIterator implements Iterator<SchedulingRequest> {

  private final List<SchedulingRequest> schedulingRequestList;
  private int cursor;

  public SerialIterator(Collection<SchedulingRequest> schedulingRequests) {
    this.schedulingRequestList = new ArrayList<>(schedulingRequests);
    this.cursor = 0;
  }

  @Override
  public boolean hasNext() {
    return (cursor < schedulingRequestList.size());
  }

  @Override
  public SchedulingRequest next() {
    if (hasNext()) {
      return schedulingRequestList.get(cursor++);
    }
    throw new NoSuchElementException();
  }
}
