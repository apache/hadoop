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
package org.apache.hadoop.lib.wsrs;

import org.apache.hadoop.classification.InterfaceAudience;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;

/**
 * Class that contains all parsed JAX-RS parameters.
 * <p>
 * Instances are created by the {@link ParametersProvider} class.
 */
@InterfaceAudience.Private
public class Parameters {
  private Map<String, List<Param<?>>> params;

  /**
   * Constructor that receives the request parsed parameters.
   *
   * @param params the request parsed parameters.
   */
  public Parameters(Map<String, List<Param<?>>> params) {
    this.params = params;
  }

  /**
   * Returns the value of a request parsed parameter.
   *
   * @param name parameter name.
   * @param klass class of the parameter, used for value casting.
   * @return the value of the parameter.
   */
  @SuppressWarnings("unchecked")
  public <V, T extends Param<V>> V get(String name, Class<T> klass) {
    List<Param<?>> multiParams = (List<Param<?>>)params.get(name);
    if (multiParams != null && multiParams.size() > 0) {
      return ((T) multiParams.get(0)).value(); // Return first value;
    }
    return null;
  }
  
  /**
   * Returns the values of a request parsed parameter.
   *
   * @param name parameter name.
   * @param klass class of the parameter, used for value casting.
   * @return the values of the parameter.
   */
  @SuppressWarnings("unchecked")
  public <V, T extends Param<V>> List<V> getValues(String name, Class<T> klass) {
    List<Param<?>> multiParams = (List<Param<?>>)params.get(name);
    List<V> values = Lists.newArrayList();
    if (multiParams != null) {
      for (Param<?> param : multiParams) {
        V value = ((T) param).value();
        if (value != null) {
          values.add(value);
        }
      }
    }
    return values;
  }
}
