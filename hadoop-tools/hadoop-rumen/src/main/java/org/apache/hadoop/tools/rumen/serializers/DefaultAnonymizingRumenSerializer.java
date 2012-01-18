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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.rumen.datatypes.AnonymizableDataType;
import org.apache.hadoop.tools.rumen.state.StatePool;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.JsonSerializer;
import org.codehaus.jackson.map.SerializerProvider;

/**
 * Default Rumen JSON serializer.
 */
@SuppressWarnings("unchecked")
public class DefaultAnonymizingRumenSerializer 
  extends JsonSerializer<AnonymizableDataType> {
  private StatePool statePool;
  private Configuration conf;
  
  public DefaultAnonymizingRumenSerializer(StatePool statePool, 
                                           Configuration conf) {
    this.statePool = statePool;
    this.conf = conf;
  }
  
  public void serialize(AnonymizableDataType object, JsonGenerator jGen, 
                        SerializerProvider sProvider) 
  throws IOException, JsonProcessingException {
    Object val = object.getAnonymizedValue(statePool, conf);
    // output the data if its a string
    if (val instanceof String) {
      jGen.writeString(val.toString());
    } else {
      // let the mapper (JSON generator) handle this anonymized object.
      jGen.writeObject(val);
    }
  };
}