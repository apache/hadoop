/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

/** Expert: Permits greater control of map processing. For example,
 * implementations might perform multi-threaded, asynchronous mappings. */
public interface MapRunnable extends JobConfigurable {
  /** Called to execute mapping.  Mapping is complete when this returns.
   * @param input the {@link RecordReader} with input key/value pairs.
   * @param output the {@link OutputCollector} for mapped key/value pairs.
   */
  void run(RecordReader input, OutputCollector output, Reporter reporter)
    throws IOException;
}
