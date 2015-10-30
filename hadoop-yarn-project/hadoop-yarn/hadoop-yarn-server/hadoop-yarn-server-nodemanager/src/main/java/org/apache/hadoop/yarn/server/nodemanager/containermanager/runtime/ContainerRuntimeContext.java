/*
 * *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 * /
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class ContainerRuntimeContext {
  private final Container container;
  private final Map<Attribute<?>, Object> executionAttributes;

  /** An attribute class that attempts to provide better type safety as compared
   * with using a map of string to object.
   * @param <T>
   */
  public static final class Attribute<T> {
    private final Class<T> valueClass;
    private final String id;

    private Attribute(Class<T> valueClass, String id) {
        this.valueClass = valueClass;
        this.id = id;
    }

    @Override
    public int hashCode() {
      return valueClass.hashCode() + 31 * id.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null || !(obj instanceof Attribute)){
        return false;
      }

      Attribute<?> attribute = (Attribute<?>) obj;

      return valueClass.equals(attribute.valueClass) && id.equals(attribute.id);
    }
    public static <T> Attribute<T> attribute(Class<T> valueClass, String id) {
      return new Attribute<T>(valueClass, id);
    }
  }

  public static final class Builder {
    private final Container container;
    private Map<Attribute<?>, Object> executionAttributes;

    public Builder(Container container) {
      executionAttributes = new HashMap<>();
      this.container = container;
    }

    public <E> Builder setExecutionAttribute(Attribute<E> attribute, E value) {
      this.executionAttributes.put(attribute, attribute.valueClass.cast(value));
      return this;
    }

    public ContainerRuntimeContext build() {
      return new ContainerRuntimeContext(this);
    }
  }

  private ContainerRuntimeContext(Builder builder) {
    this.container = builder.container;
    this.executionAttributes = builder.executionAttributes;
  }

  public Container getContainer() {
    return this.container;
  }

  public Map<Attribute<?>, Object> getExecutionAttributes() {
    return Collections.unmodifiableMap(this.executionAttributes);
  }

  public <E> E getExecutionAttribute(Attribute<E> attribute) {
    return attribute.valueClass.cast(executionAttributes.get(attribute));
  }
}
