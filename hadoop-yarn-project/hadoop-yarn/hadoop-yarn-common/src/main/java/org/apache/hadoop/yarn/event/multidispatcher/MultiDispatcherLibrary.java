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

package org.apache.hadoop.yarn.event.multidispatcher;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;

/**
 * Stores {@link EventHandler} for {@link Event} in {@link MultiDispatcher}
 */
class MultiDispatcherLibrary {

  private final Map<String, EventHandler> lib = new HashMap<>();

  public EventHandler getEventHandler(Event e) {
    EventHandler handler = lib.get(e.getType().getClass().getCanonicalName());
    if (handler == null) {
      throw new Error("EventHandler for " + e.getType() + ", was not found in " + lib.keySet());
    }
    return handler;
  }

  public void register(Class<? extends Enum> eventType, EventHandler handler) {
    lib.put(eventType.getCanonicalName(), handler);
  }
}
