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

package org.apache.hadoop.yarn.server.resourcemanager.preprocessor;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.junit.Test;

import static org.mockito.Mockito.*;


/**
 * This class will test the functionality of all the three
 * processor(Node, Queue, Tag)  together on same
 * ApplicationSubmissionContext.
 */
public class TestContextProcessor {
  @Test
  public void testContextProcessor() {
    Map<ContextProcessor, String> contextProcessorsAndValues =
        new HashMap<>();
    contextProcessorsAndValues.put(new NodeLabelProcessor(), "foo");
    contextProcessorsAndValues.put(new QueueProcessor(), "queue1");
    contextProcessorsAndValues.put(new TagAddProcessor(), "cluster:cluster1");
    ApplicationId app = ApplicationId.newInstance(123456, 111);
    ApplicationSubmissionContext applicationSubmissionContext =
        mock(ApplicationSubmissionContext.class);
    for(Map.Entry<ContextProcessor, String> entry :
        contextProcessorsAndValues.entrySet()){
      entry.getKey().process("host.cluster2.com", entry.getValue(),
          app, applicationSubmissionContext);
    }
    Set<String> applicationTags =new HashSet<String>();
    applicationTags.add("cluster:cluster1");
    verify(applicationSubmissionContext, times(1))
        .setNodeLabelExpression("foo");
    verify(applicationSubmissionContext, times(1))
        .setQueue("queue1");
    verify(applicationSubmissionContext, times(1))
        .setApplicationTags(applicationTags);

  }
}
