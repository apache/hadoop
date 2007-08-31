/**
 * Copyright 2007 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.mapred;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Reporter;


/**
 * Write to table each key, record pair
 */
public class IdentityTableReduce extends TableReduce {

  /** constructor */
  public IdentityTableReduce() {
    super();
  }

  /**
   * No aggregation, output pairs of (key, record)
   *
   * @see org.apache.hadoop.hbase.mapred.TableReduce#reduce(org.apache.hadoop.io.Text, java.util.Iterator, org.apache.hadoop.hbase.mapred.TableOutputCollector, org.apache.hadoop.mapred.Reporter)
   */
  @Override
  public void reduce(Text key, @SuppressWarnings("unchecked") Iterator values,
      TableOutputCollector output,
      @SuppressWarnings("unused") Reporter reporter) throws IOException {
    
    while(values.hasNext()) {
      MapWritable r = (MapWritable)values.next();
      output.collect(key, r);
    }
  }
}
