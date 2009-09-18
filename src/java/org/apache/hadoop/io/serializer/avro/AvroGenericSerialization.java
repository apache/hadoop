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

package org.apache.hadoop.io.serializer.avro;

import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.hadoop.io.serializer.SerializationBase;

/**
 * Serialization for Avro Generic classes. For a class to be accepted by this 
 * serialization it must have metadata with key
 * {@link SerializationBase#SERIALIZATION_KEY} set to {@link AvroGenericSerialization}'s
 * fully-qualified classname.
 * The schema used is the one set by {@link AvroSerialization#AVRO_SCHEMA_KEY}.
 */
@SuppressWarnings("unchecked")
public class AvroGenericSerialization extends AvroSerialization<Object> {
  
  @Override
  public boolean accept(Map<String, String> metadata) {
    return metadata.get(AVRO_SCHEMA_KEY) != null;
  }

  @Override
  protected DatumReader getReader(Map<String, String> metadata) {
    Schema schema = Schema.parse(metadata.get(AVRO_SCHEMA_KEY));
    return new GenericDatumReader<Object>(schema);
  }

  @Override
  protected Schema getSchema(Object t, Map<String, String> metadata) {
    String jsonSchema = metadata.get(AVRO_SCHEMA_KEY);
    return jsonSchema != null ? Schema.parse(jsonSchema) : GenericData.get().induce(t);
  }

  @Override
  protected DatumWriter getWriter(Map<String, String> metadata) {
    return new GenericDatumWriter<Object>();
  }

}
