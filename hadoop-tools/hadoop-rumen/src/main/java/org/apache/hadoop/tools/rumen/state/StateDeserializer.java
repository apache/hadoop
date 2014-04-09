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
package org.apache.hadoop.tools.rumen.state;

import java.io.IOException;

import org.apache.hadoop.tools.rumen.state.StatePool.StatePair;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.DeserializationContext;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.deser.std.StdDeserializer;
import org.codehaus.jackson.node.ObjectNode;

/**
 * Rumen JSON deserializer for deserializing the {@link State} object.
 */
public class StateDeserializer extends StdDeserializer<StatePair> {
  public StateDeserializer() {
      super(StatePair.class);
  }
  
  @Override
  public StatePair deserialize(JsonParser parser, 
                               DeserializationContext context)
  throws IOException, JsonProcessingException {
    ObjectMapper mapper = (ObjectMapper) parser.getCodec();
    // set the state-pair object tree
    ObjectNode statePairObject = (ObjectNode) mapper.readTree(parser);
    Class<?> stateClass = null;
    
    try {
      stateClass = 
        Class.forName(statePairObject.get("className").getTextValue().trim());
    } catch (ClassNotFoundException cnfe) {
      throw new RuntimeException("Invalid classname!", cnfe);
    }
    
    String stateJsonString = statePairObject.get("state").toString();
    State state = (State) mapper.readValue(stateJsonString, stateClass);
    
    return new StatePair(state);
  }
}