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

package org.apache.hadoop.mapreduce.jobhistory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Interface for event wrapper classes.  Implementations each wrap an
 * Avro-generated class, adding constructors and accessor methods.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public interface HistoryEvent {

  /** Return this event's type. */
  EventType getEventType();

  /** Return the Avro datum wrapped by this. */
  Object getDatum();

  /** Set the Avro datum wrapped by this. */
  void setDatum(Object datum);
}
