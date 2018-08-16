/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.server.events;

/**
 * Basic event implementation to implement custom events.
 *
 * @param <T>
 */
public class TypedEvent<T> implements Event<T> {

  private final Class<T> payloadType;

  private final String name;

  public TypedEvent(Class<T> payloadType, String name) {
    this.payloadType = payloadType;
    this.name = name;
  }

  public TypedEvent(Class<T> payloadType) {
    this.payloadType = payloadType;
    this.name = payloadType.getSimpleName();
  }

  @Override
  public Class<T> getPayloadType() {
    return payloadType;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String toString() {
    return "TypedEvent{" +
        "payloadType=" + payloadType.getSimpleName() +
        ", name='" + name + '\'' +
        '}';
  }
}
