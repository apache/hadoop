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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter.weightconversion;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSParentQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSQueue;
import org.apache.hadoop.yarn.util.resource.Resources;

public abstract class WeightConverterTestBase {

  protected FSQueue createFSQueues(int... weights){
    char current = 'a';

    List<FSQueue> queues = new ArrayList<>();

    for (int w : weights) {
      FSQueue queue = mock(FSQueue.class);
      when(queue.getWeight()).thenReturn((float)w);
      when(queue.getName()).thenReturn(
          "root." + new String(new char[] {current}));
      when(queue.getMinShare()).thenReturn(Resources.none());
      current++;
      queues.add(queue);
    }

    return createParent(queues);
  }

  protected FSParentQueue createParent(List<FSQueue> children) {
    FSParentQueue root = mock(FSParentQueue.class);
    when(root.getWeight()).thenReturn(1.0f);
    when(root.getName()).thenReturn("root");
    when(root.getMinShare()).thenReturn(Resources.none());
    when(root.getChildQueues()).thenReturn(children);
    return root;
  }
}