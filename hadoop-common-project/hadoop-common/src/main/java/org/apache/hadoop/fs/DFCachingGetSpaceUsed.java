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
package org.apache.hadoop.fs;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import java.io.IOException;

/**
 * Fast but inaccurate class to tell how much space HDFS is using.
 * This class makes the assumption that the entire mount is used for
 * HDFS and that no two hdfs data dirs are on the same disk.
 *
 * To use set fs.getspaceused.classname
 * to org.apache.hadoop.fs.DFCachingGetSpaceUsed in your core-site.xml
 *
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
public class DFCachingGetSpaceUsed extends CachingGetSpaceUsed {
  private final DF df;

  public DFCachingGetSpaceUsed(Builder builder) throws IOException {
    super(builder);
    this.df = new DF(builder.getPath(), builder.getInterval());
  }

  @Override
  protected void refresh() {
    this.used.set(df.getUsed());
  }
}
