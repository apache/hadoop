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

import java.lang.reflect.Type;
import java.util.NavigableMap;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.RLESparseResourceAllocation;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.google.gson.reflect.TypeToken;

/**
 * Serialize/deserialize RLESparseResourceAllocation object to/from JSON.
 */
public class RLESparseResourceAllocationSerDe
    implements JsonSerializer<RLESparseResourceAllocation>,
    JsonDeserializer<RLESparseResourceAllocation> {
  private static final String KEY = "resourceAllocation";
  private final Gson gson =
      new GsonBuilder().registerTypeAdapter(Resource.class, new ResourceSerDe())
          .create();
  private final Type type = new TypeToken<NavigableMap<Long, Resource>>() {
  }.getType();
  private final ResourceCalculator resourceCalculator =
      new DefaultResourceCalculator();

  @Override public final JsonElement serialize(
      final RLESparseResourceAllocation resourceAllocation,
      final Type typeOfSrc, final JsonSerializationContext context) {
    NavigableMap<Long, Resource> myMap = resourceAllocation.getCumulative();
    JsonObject jo = new JsonObject();
    JsonElement element = gson.toJsonTree(myMap, type);
    jo.add(KEY, element);

    return jo;
  }

  @Override public final RLESparseResourceAllocation deserialize(
      final JsonElement json, final Type typeOfT,
      final JsonDeserializationContext context) throws JsonParseException {
    NavigableMap<Long, Resource> resAllocation =
        gson.fromJson(json.getAsJsonObject().get(KEY), type);
    RLESparseResourceAllocation rleSparseResourceAllocation =
        new RLESparseResourceAllocation(resAllocation, resourceCalculator);
    return rleSparseResourceAllocation;
  }
}
