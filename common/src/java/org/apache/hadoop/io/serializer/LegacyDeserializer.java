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
package org.apache.hadoop.io.serializer;

import java.io.IOException;
import java.io.InputStream;

@SuppressWarnings("deprecation")
class LegacyDeserializer<T> extends DeserializerBase<T> {
  
  private Deserializer<T> deserializer;

  public LegacyDeserializer(Deserializer<T> deserializer) {
    this.deserializer = deserializer;
  }

  @Override
  public void open(InputStream in) throws IOException {
    deserializer.open(in);
  }
  
  @Override
  public T deserialize(T t) throws IOException {
    return deserializer.deserialize(t);
  }

  @Override
  public void close() throws IOException {
    deserializer.close();
  }

}
