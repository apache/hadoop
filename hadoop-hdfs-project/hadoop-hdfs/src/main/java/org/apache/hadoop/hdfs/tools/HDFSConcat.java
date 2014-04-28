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
package org.apache.hadoop.hdfs.tools;


import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;


@InterfaceAudience.Private
public class HDFSConcat {
  private final static String def_uri = "hdfs://localhost:9000";

  public static void main(String... args) throws IOException {

    if(args.length < 2) {
      System.err.println("Usage HDFSConcat target srcs..");
      System.exit(0);
    }
    
    Configuration conf = new Configuration();
    String uri = conf.get("fs.default.name", def_uri);
    Path path = new Path(uri);
    DistributedFileSystem dfs = 
      (DistributedFileSystem)FileSystem.get(path.toUri(), conf);
    
    Path [] srcs = new Path[args.length-1];
    for(int i=1; i<args.length; i++) {
      srcs[i-1] = new Path(args[i]);
    }
    dfs.concat(new Path(args[0]), srcs);
  }

}
