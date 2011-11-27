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
import org.apache.hadoop.fs.FileSystem;

/**
 * A MiniMRCluster factory. In MR2, it provides a wrapper MiniMRClientCluster
 * interface around the MiniMRYarnCluster. While in MR1, it provides such
 * wrapper around MiniMRCluster. This factory should be used in tests to provide
 * an easy migration of tests across MR1 and MR2.
 */
public class MiniMRClientClusterFactory {

  public static MiniMRClientCluster create(Class<?> caller, int noOfNMs,
      Configuration conf) throws IOException {
    FileSystem fs = FileSystem.getLocal(new Configuration());
    MiniMRCluster miniMRCluster = new MiniMRCluster(noOfNMs, fs.getUri()
        .toString(), 1);
    return new MiniMRClusterAdapter(miniMRCluster);
  }

}