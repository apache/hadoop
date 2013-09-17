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
package org.apache.hadoop.mapreduce;

import org.apache.hadoop.classification.InterfaceAudience;

@InterfaceAudience.Private
public abstract class StatusReporter {
  public abstract Counter getCounter(Enum<?> name);
  public abstract Counter getCounter(String group, String name);
  public abstract void progress();
  /**
   * Get the current progress.
   * @return a number between 0.0 and 1.0 (inclusive) indicating the attempt's 
   * progress.
   */
  public abstract float getProgress();
  public abstract void setStatus(String status);
}
