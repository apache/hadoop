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

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/** Default {@link MapRunnable} implementation.*/
public class MapRunner implements MapRunnable {
  private JobConf job;
  private Mapper mapper;
  private Class inputKeyClass;
  private Class inputValueClass;

  public void configure(JobConf job) {
    this.job = job;
    this.mapper = (Mapper)job.newInstance(job.getMapperClass());
    this.inputKeyClass = job.getInputKeyClass();
    this.inputValueClass = job.getInputValueClass();
  }

  public void run(RecordReader input, OutputCollector output,
                  Reporter reporter)
    throws IOException {
    try {
      // allocate key & value instances that are re-used for all entries
      WritableComparable key =
        (WritableComparable)job.newInstance(inputKeyClass);
      Writable value = (Writable)job.newInstance(inputValueClass);
      while (input.next(key, value)) {
        // map pair to output
        mapper.map(key, value, output, reporter);
      }
    } finally {
        mapper.close();
    }
  }

}
