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

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.util.GenericsUtil;

public class SerializationTestUtil {

  /**
   * A utility that tests serialization/deserialization. 
   * @param <K> the class of the item
   * @param conf configuration to use, "io.serializations" is read to 
   * determine the serialization
   * @param before item to (de)serialize
   * @return deserialized item
   */
  public static<K> K testSerialization(Configuration conf, K before) 
      throws Exception {
    Map<String, String> metadata =
      SerializationBase.getMetadataFromClass(GenericsUtil.getClass(before));
    return testSerialization(conf, metadata, before);
  }
  
  /**
   * A utility that tests serialization/deserialization. 
   * @param conf configuration to use, "io.serializations" is read to 
   * determine the serialization
   * @param metadata the metadata to pass to the serializer/deserializer
   * @param <K> the class of the item
   * @param before item to (de)serialize
   * @return deserialized item
   */
  public static <K> K testSerialization(Configuration conf, 
      Map<String, String> metadata, K before) throws Exception {

    SerializationFactory factory = new SerializationFactory(conf);
    SerializerBase<K> serializer = factory.getSerializer(metadata);
    DeserializerBase<K> deserializer = factory.getDeserializer(metadata);

    DataOutputBuffer out = new DataOutputBuffer();
    serializer.open(out);
    serializer.serialize(before);
    serializer.close();

    DataInputBuffer in = new DataInputBuffer();
    in.reset(out.getData(), out.getLength());
    deserializer.open(in);
    K after = deserializer.deserialize(null);
    deserializer.close();
    return after;
  }

}
