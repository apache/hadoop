/*
 *
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
 *
 */

package org.apache.hadoop.resourceestimator.common.serialization;

import org.apache.hadoop.yarn.api.records.Resource;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

/**
 * Test ResourceSerDe.
 */
public class TestResourceSerDe {
  /**
   * Testing variables.
   */
  private Gson gson;

  private Resource resource;

  @Before public final void setup() {
    resource = Resource.newInstance(1024 * 100, 100);
    gson = new GsonBuilder()
        .registerTypeAdapter(Resource.class, new ResourceSerDe()).create();
  }

  @Test public final void testSerialization() {
    final String json = gson.toJson(resource, new TypeToken<Resource>() {
    }.getType());
    final Resource resourceDe = gson.fromJson(json, new TypeToken<Resource>() {
    }.getType());
    Assert.assertEquals(resource.getMemorySize(), resourceDe.getMemorySize());
    Assert
        .assertEquals(resource.getVirtualCores(), resourceDe.getVirtualCores());
  }

  @After public final void cleanUp() {
    resource = null;
    gson = null;
  }
}
