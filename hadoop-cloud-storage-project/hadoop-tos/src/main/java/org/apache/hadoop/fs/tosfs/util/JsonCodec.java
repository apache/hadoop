/*
 * ByteDance Volcengine EMR, Copyright 2022.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.tosfs.util;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class JsonCodec<T> {
  private static final ObjectMapper MAPPER = new ObjectMapper()
      .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true)
      .configure(SerializationFeature.INDENT_OUTPUT, true)
      .setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
  private final Class<T> clazz;

  public JsonCodec(Class<T> clazz) {
    this.clazz = clazz;
  }

  public byte[] toBytes(T instance) throws IOException {
    return MAPPER.writeValueAsBytes(instance);
  }

  public T fromBytes(byte[] data) throws IOException {
    return MAPPER.readValue(new String(data, 0, data.length, StandardCharsets.UTF_8), clazz);
  }
}
