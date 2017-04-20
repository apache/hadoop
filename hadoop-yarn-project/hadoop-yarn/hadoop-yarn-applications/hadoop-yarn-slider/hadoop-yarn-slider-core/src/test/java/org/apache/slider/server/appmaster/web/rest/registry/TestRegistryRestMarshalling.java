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

package org.apache.slider.server.appmaster.web.rest.registry;

import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.apache.hadoop.registry.client.types.yarn.YarnRegistryAttributes;
import org.junit.Test;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

/**
 * This test exists because Jersey appears to behave "oddly"
 * when it comes to marshalling JSON, and some of the REST resources
 * appear to have trouble.
 *
 * This test tries to isolate it
 */
public class TestRegistryRestMarshalling {

  @Test
  public void testDeser() throws Throwable {
    PathEntryMarshalling pem = new PathEntryMarshalling();
    PathEntryResource unmarshalled = pem.fromResource(
        "/org/apache/slider/server/appmaster/web/rest/registry/sample.json");

    ServiceRecord serviceRecord = unmarshalled.service;
    assertNotNull(serviceRecord);
    assertNotNull(serviceRecord.get(YarnRegistryAttributes.YARN_ID));
    assertNotEquals("", serviceRecord.get(YarnRegistryAttributes
        .YARN_PERSISTENCE));
  }


}
