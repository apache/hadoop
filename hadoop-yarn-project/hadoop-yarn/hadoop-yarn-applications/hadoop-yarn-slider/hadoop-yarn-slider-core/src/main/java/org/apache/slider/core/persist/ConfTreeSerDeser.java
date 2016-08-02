/*
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

package org.apache.slider.core.persist;

import org.apache.slider.core.conf.ConfTree;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;

import java.io.IOException;

/**
 * Conf tree to JSON binding
 */
public class ConfTreeSerDeser extends JsonSerDeser<ConfTree> {
  public ConfTreeSerDeser() {
    super(ConfTree.class);
  }


  private static final ConfTreeSerDeser staticinstance = new ConfTreeSerDeser();

  /**
   * Convert a tree instance to a JSON string -sync access to a shared ser/deser
   * object instance
   * @param instance object to convert
   * @return a JSON string description
   * @throws JsonParseException parse problems
   * @throws JsonMappingException O/J mapping problems
   */
  public static String toString(ConfTree instance) throws IOException,
                                                          JsonGenerationException,
                                                          JsonMappingException {
    synchronized (staticinstance) {
      return staticinstance.toJson(instance);
    }
  }
}
