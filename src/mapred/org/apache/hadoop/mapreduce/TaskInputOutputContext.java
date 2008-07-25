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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

/**
 * A context object that allows input and output from the task. It is only
 * supplied to the {@link Mapper} or {@link Reducer}.
 * @param <KEYIN> the input key type for the task
 * @param <VALUEIN> the input value type for the task
 * @param <KEYOUT> the output key type for the task
 * @param <VALUEOUT> the output value type for the task
 */
public abstract class TaskInputOutputContext<KEYIN,VALUEIN,KEYOUT,VALUEOUT> 
    extends TaskAttemptContext {

  public TaskInputOutputContext(Configuration conf, TaskAttemptID taskid) {
    super(conf, taskid);
  }

  /**
   * Advance to the next key, returning null if at end.
   * @param key the key object to read in to, which may be null
   * @return the key object that was read into
   */
  public abstract KEYIN nextKey(KEYIN key
                                ) throws IOException, InterruptedException;
  
  /**
   * Read the next value. Must be called after nextKey.
   * @param value the value object to read in to, which may be null
   * @return the value object that was read into
   * @throws IOException
   * @throws InterruptedException
   */
  public abstract VALUEIN nextValue(VALUEIN value
                                    ) throws IOException, InterruptedException;

  /**
   * Generate an output key/value pair.
   */
  public abstract void collect(KEYOUT key, VALUEOUT value
                               ) throws IOException, InterruptedException;

}
