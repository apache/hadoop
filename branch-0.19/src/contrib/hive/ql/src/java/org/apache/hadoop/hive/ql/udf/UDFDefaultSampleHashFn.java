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

package org.apache.hadoop.hive.ql.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class UDFDefaultSampleHashFn implements UDF {
  protected final Log LOG;

  public UDFDefaultSampleHashFn() {
    LOG = LogFactory.getLog(this.getClass().getName());
  }

  public int evaluate(Object o) {
    return o.hashCode();
  }
  
  // TODO: For now, only allow up to two columns on which to sample
  // Going forward we will allow sampling on an arbitrary number of columns
  public int evaluate(Object o1, Object o2) {
    return o1.hashCode() ^ o2.hashCode();
  }
}
