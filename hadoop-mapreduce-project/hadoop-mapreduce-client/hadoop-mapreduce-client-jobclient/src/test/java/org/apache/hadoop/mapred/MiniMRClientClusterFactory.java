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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.v2.MiniMRYarnCluster;
import org.apache.hadoop.util.JarFinder;

/**
 * A MiniMRCluster factory. In MR2, it provides a wrapper MiniMRClientCluster
 * interface around the MiniMRYarnCluster. While in MR1, it provides such
 * wrapper around MiniMRCluster. This factory should be used in tests to provide
 * an easy migration of tests across MR1 and MR2.
 */
public class MiniMRClientClusterFactory {

  public static MiniMRClientCluster create(Class<?> caller, int noOfNMs,
      Configuration conf) throws IOException {
    return create(caller, caller.getSimpleName(), noOfNMs, conf);
  }

  public static MiniMRClientCluster create(Class<?> caller, String identifier,
      int noOfNMs, Configuration conf) throws IOException {

    if (conf == null) {
      conf = new Configuration();
    }

    FileSystem fs = FileSystem.get(conf);

    Path testRootDir = fs.makeQualified(
        new Path("target", identifier + "-tmpDir"));
    Path appJar = new Path(testRootDir, "MRAppJar.jar");

    // Copy MRAppJar and make it private.
    Path appMasterJar =
        new Path(MiniMRYarnCluster.copyAppJarIntoTestDir(identifier));

    fs.copyFromLocalFile(appMasterJar, appJar);
    fs.setPermission(appJar, new FsPermission("744"));

    Job job = Job.getInstance(conf);

    job.addFileToClassPath(appJar);

    Path callerJar = new Path(JarFinder.getJar(caller, identifier));
    Path remoteCallerJar = new Path(testRootDir, callerJar.getName());
    fs.copyFromLocalFile(callerJar, remoteCallerJar);
    fs.setPermission(remoteCallerJar, new FsPermission("744"));
    job.addFileToClassPath(remoteCallerJar);

    MiniMRYarnCluster miniMRYarnCluster = new MiniMRYarnCluster(identifier,
        noOfNMs);
    job.getConfiguration().set("minimrclientcluster.caller.name",
        identifier);
    job.getConfiguration().setInt("minimrclientcluster.nodemanagers.number",
        noOfNMs);
    miniMRYarnCluster.init(job.getConfiguration());
    miniMRYarnCluster.start();

    return new MiniMRYarnClusterAdapter(miniMRYarnCluster);
  }

}
