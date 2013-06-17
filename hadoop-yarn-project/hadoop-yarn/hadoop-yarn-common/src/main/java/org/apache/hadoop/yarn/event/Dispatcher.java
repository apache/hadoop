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

package org.apache.hadoop.yarn.event;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;

/**
 * Event Dispatcher interface. It dispatches events to registered 
 * event handlers based on event types.
 * 
 */
@SuppressWarnings("rawtypes")
@Public
@Evolving
public interface Dispatcher {

  // Configuration to make sure dispatcher crashes but doesn't do system-exit in
  // case of errors. By default, it should be false, so that tests are not
  // affected. For all daemons it should be explicitly set to true so that
  // daemons can crash instead of hanging around.
  public static final String DISPATCHER_EXIT_ON_ERROR_KEY =
      "yarn.dispatcher.exit-on-error";

  public static final boolean DEFAULT_DISPATCHER_EXIT_ON_ERROR = false;

  EventHandler getEventHandler();

  void register(Class<? extends Enum> eventType, EventHandler handler);

}
