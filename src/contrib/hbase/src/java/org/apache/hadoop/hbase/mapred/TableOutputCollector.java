/**
 * Copyright 2007 The Apache Software Foundation
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
package org.apache.hadoop.hbase.mapred;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;

import org.apache.hadoop.hbase.io.KeyedDataArrayWritable;

/**
 * Refine the types that can be collected from a Table Map/Reduce jobs.
 */
public class TableOutputCollector {
  /** The collector object */
  public OutputCollector collector;

  /**
   * Restrict Table Map/Reduce's output to be a Text key and a record.
   * 
   * @param key
   * @param value
   * @throws IOException
   */
  public void collect(Text key, KeyedDataArrayWritable value)
  throws IOException {
    collector.collect(key, value);
  }
}
