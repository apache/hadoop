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

import org.apache.hadoop.yarn.api.records.Resource;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

/**
 * Serialize/deserialize Resource object to/from JSON.
 */
public class ResourceSerDe
    implements JsonSerializer<Resource>, JsonDeserializer<Resource> {
  private static final String KEY1 = "memory";
  private static final String KEY2 = "vcores";

  @Override public final JsonElement serialize(final Resource resource,
      final Type type, final JsonSerializationContext context) {
    JsonObject jo = new JsonObject();
    jo.addProperty(KEY1, resource.getMemorySize());
    jo.addProperty(KEY2, resource.getVirtualCores());
    return jo;
  }

  @Override public final Resource deserialize(final JsonElement json,
      final Type type, final JsonDeserializationContext context)
      throws JsonParseException {
    JsonObject jo = json.getAsJsonObject();
    long mem = jo.getAsJsonPrimitive(KEY1).getAsLong();
    int vcore = jo.getAsJsonPrimitive(KEY2).getAsInt();
    Resource resource = Resource.newInstance(mem, vcore);

    return resource;
  }
}
