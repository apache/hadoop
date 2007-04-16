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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * This class converts the input keys and values to their String forms by calling toString()
 * method. This class to SequenceFileAsTextInputFormat class is as LineRecordReader
 * class to TextInputFormat class.
 *
 */
public class SequenceFileAsTextRecordReader extends SequenceFileRecordReader {

  private Writable innerKey = super.createKey();
  private Writable innerValue = super.createValue();

  public SequenceFileAsTextRecordReader(Configuration conf, FileSplit split)
    throws IOException {
    super(conf, split);
  }

  public WritableComparable createKey() {
    return new Text();
  }
  
  public Writable createValue() {
    return new Text();
  }

  /** Read key/value pair in a line. */
  public synchronized boolean next(Writable key, Writable value)
    throws IOException {
    Text tKey = (Text) key;
    Text tValue = (Text) value;
    if (!super.next(innerKey, innerValue)) {
      return false;
    }
    tKey.set(innerKey.toString());
    tValue.set(innerValue.toString());
    return true;
  }
}
