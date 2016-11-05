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
package org.apache.hadoop.tools.rumen.serializers;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import org.apache.hadoop.tools.rumen.datatypes.DataType;

/**
 * Default Rumen JSON serializer.
 */
@SuppressWarnings("unchecked")
public class DefaultRumenSerializer extends JsonSerializer<DataType> {
  public void serialize(DataType object, JsonGenerator jGen, SerializerProvider sProvider) 
  throws IOException, JsonProcessingException {
    Object data = object.getValue();
    if (data instanceof String) {
      jGen.writeString(data.toString());
    } else {
      jGen.writeObject(data);
    }
  };
}