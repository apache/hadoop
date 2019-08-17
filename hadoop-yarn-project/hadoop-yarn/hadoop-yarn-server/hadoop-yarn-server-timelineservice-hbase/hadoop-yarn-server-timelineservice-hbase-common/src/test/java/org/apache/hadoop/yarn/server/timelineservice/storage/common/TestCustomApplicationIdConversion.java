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
package org.apache.hadoop.yarn.server.timelineservice.storage.common;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test for HBaseTimelineStorageUtils.convertApplicationIdToString(),
 * a custom conversion from ApplicationId to String that avoids the
 * incompatibility issue caused by mixing hadoop-common 2.5.1 and
 * hadoop-yarn-api 3.0. See YARN-6905.
 */
public class TestCustomApplicationIdConversion {
  @Test
  public void testConvertAplicationIdToString() {
    ApplicationId applicationId = ApplicationId.newInstance(0, 1);
    String applicationIdStr =
        HBaseTimelineSchemaUtils.convertApplicationIdToString(applicationId);
    Assert.assertEquals(applicationId,
        ApplicationId.fromString(applicationIdStr));
  }
}
