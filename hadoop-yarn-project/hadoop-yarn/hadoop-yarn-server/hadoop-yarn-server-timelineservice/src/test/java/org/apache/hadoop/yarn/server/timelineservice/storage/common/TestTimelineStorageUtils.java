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

package org.apache.hadoop.yarn.server.timelineservice.storage.common;

import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.junit.Test;

public class TestTimelineStorageUtils {

  @Test
  public void testEncodeDecodeAppId() {
    long currentTs = System.currentTimeMillis();
    ApplicationId appId1 = ApplicationId.newInstance(currentTs, 1);
    ApplicationId appId2 = ApplicationId.newInstance(currentTs, 2);
    ApplicationId appId3 = ApplicationId.newInstance(currentTs + 300, 1);
    String appIdStr1 = appId1.toString();
    String appIdStr2 = appId2.toString();
    String appIdStr3 = appId3.toString();
    byte[] appIdBytes1 = TimelineStorageUtils.encodeAppId(appIdStr1);
    byte[] appIdBytes2 = TimelineStorageUtils.encodeAppId(appIdStr2);
    byte[] appIdBytes3 = TimelineStorageUtils.encodeAppId(appIdStr3);
    // App ids' should be encoded in a manner wherein descending order
    // is maintained.
    assertTrue("Ordering of app ids' is incorrect",
        Bytes.compareTo(appIdBytes1, appIdBytes2) > 0 &&
        Bytes.compareTo(appIdBytes1, appIdBytes3) > 0 &&
        Bytes.compareTo(appIdBytes2, appIdBytes3) > 0);
    String decodedAppId1 = TimelineStorageUtils.decodeAppId(appIdBytes1);
    String decodedAppId2 = TimelineStorageUtils.decodeAppId(appIdBytes2);
    String decodedAppId3 = TimelineStorageUtils.decodeAppId(appIdBytes3);
    assertTrue("Decoded app id is not same as the app id encoded",
        appIdStr1.equals(decodedAppId1));
    assertTrue("Decoded app id is not same as the app id encoded",
        appIdStr2.equals(decodedAppId2));
    assertTrue("Decoded app id is not same as the app id encoded",
        appIdStr3.equals(decodedAppId3));
  }
}
