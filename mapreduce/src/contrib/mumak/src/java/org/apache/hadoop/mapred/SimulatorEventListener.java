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
package org.apache.hadoop.mapred;

import java.io.IOException;
import java.util.List;

/**
 * Interface for entities that handle events.
 */
public interface SimulatorEventListener {
  /**
   * Get the initial events to put in event queue.
   * @param when time to schedule the initial events
   * @return list of the initial events
   */
  List<SimulatorEvent> init(long when) throws IOException;
  
  /**
   * Process an event, generate more events to put in event queue.
   * @param event the event to be processed
   * @return list of generated events by processing this event
   */
  List<SimulatorEvent> accept(SimulatorEvent event) throws IOException;
}
