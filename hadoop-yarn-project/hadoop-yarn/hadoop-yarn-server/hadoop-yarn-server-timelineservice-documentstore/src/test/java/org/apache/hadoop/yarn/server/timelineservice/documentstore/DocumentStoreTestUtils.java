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

package org.apache.hadoop.yarn.server.timelineservice.documentstore;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.document.entity.TimelineEntityDocument;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.document.flowactivity.FlowActivityDocument;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.document.flowrun.FlowRunDocument;
import org.codehaus.jackson.type.TypeReference;

import java.io.IOException;
import java.util.List;

/**
 * This is util class for baking sample TimelineEntities data for test.
 */
public final class DocumentStoreTestUtils {

  private DocumentStoreTestUtils(){}


  public static List<TimelineEntity> bakeTimelineEntities()
      throws IOException {
    String jsonStr = IOUtils.toString(
        DocumentStoreTestUtils.class.getClassLoader().getResourceAsStream(
            "documents/timeline-entities.json"), "UTF-8");
    return JsonUtils.fromJson(jsonStr,
        new TypeReference<List<TimelineEntity>>(){});
  }

  public static List<TimelineEntityDocument> bakeYarnAppTimelineEntities()
      throws IOException {
    String jsonStr = IOUtils.toString(
        DocumentStoreTestUtils.class.getClassLoader().getResourceAsStream(
            "documents/test-timeline-entities-doc.json"), "UTF-8");
    return JsonUtils.fromJson(jsonStr,
        new TypeReference<List<TimelineEntityDocument>>() {});
  }

  public static TimelineEntityDocument bakeTimelineEntityDoc()
      throws IOException {
    String jsonStr = IOUtils.toString(
        DocumentStoreTestUtils.class.getClassLoader().getResourceAsStream(
            "documents/timeline-app-doc.json"), "UTF-8");
    return JsonUtils.fromJson(jsonStr,
        new TypeReference<TimelineEntityDocument>() {});
  }

  public static FlowActivityDocument bakeFlowActivityDoc() throws IOException {
    String jsonStr = IOUtils.toString(
        DocumentStoreTestUtils.class.getClassLoader().getResourceAsStream(
            "documents/flowactivity-doc.json"), "UTF-8");
    return JsonUtils.fromJson(jsonStr,
        new TypeReference<FlowActivityDocument>() {});
  }

  public static FlowRunDocument bakeFlowRunDoc() throws IOException {
    String jsonStr = IOUtils.toString(
        DocumentStoreTestUtils.class.getClassLoader().getResourceAsStream(
            "documents/flowrun-doc.json"), "UTF-8");
    return JsonUtils.fromJson(jsonStr,
        new TypeReference<FlowRunDocument>(){});
  }
}