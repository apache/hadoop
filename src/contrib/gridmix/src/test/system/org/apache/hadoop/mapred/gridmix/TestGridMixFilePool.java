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
package org.apache.hadoop.mapred.gridmix;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.test.system.MRCluster;
import org.apache.hadoop.mapreduce.test.system.JTClient;
import org.apache.hadoop.mapreduce.test.system.JTProtocol;
import org.apache.hadoop.mapred.gridmix.FilePool;
import org.apache.hadoop.mapred.gridmix.test.system.UtilsForGridmix;
import org.apache.hadoop.mapred.gridmix.test.system.GridMixRunMode;
import org.apache.hadoop.mapred.gridmix.test.system.GridMixConfig;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.junit.Test;
import java.io.IOException;
import java.util.ArrayList;

public class TestGridMixFilePool {
  private static final Log LOG = 
      LogFactory.getLog(TestGridMixFilePool.class);
  private static Configuration conf = new Configuration();
  private static MRCluster cluster;
  private static JTProtocol remoteClient;
  private static JTClient jtClient;
  private static Path gridmixDir;
  private static int clusterSize; 
  
  @BeforeClass
  public static void before() throws Exception {
    String []  excludeExpList = {"java.net.ConnectException", 
                                 "java.io.IOException"};
    cluster = MRCluster.createCluster(conf);
    cluster.setExcludeExpList(excludeExpList);
    cluster.setUp();
    jtClient = cluster.getJTClient();
    remoteClient = jtClient.getProxy();
    clusterSize = cluster.getTTClients().size();
    gridmixDir = new Path("herriot-gridmix");
    UtilsForGridmix.createDirs(gridmixDir, remoteClient.getDaemonConf());
  }

  @AfterClass
  public static void after() throws Exception {
    UtilsForGridmix.cleanup(gridmixDir, conf);
    cluster.tearDown();
  }
  
  @Test
  public void testFilesCountAndSizesForSpecifiedFilePool() throws Exception {
    conf = remoteClient.getDaemonConf();
    final long inputSizeInMB = clusterSize * 200;
    int [] fileSizesInMB = {50, 100, 400, 50, 300, 10, 60, 40, 20 ,10 , 500};
    long targetSize = Long.MAX_VALUE;
    final int expFileCount = clusterSize + 4;
    String [] runtimeValues ={"LOADJOB",
                              SubmitterUserResolver.class.getName(),
                              "STRESS",
                              inputSizeInMB + "m",
                              "file:///dev/null"}; 

    String [] otherArgs = {
        "-D", GridMixConfig.GRIDMIX_DISTCACHE_ENABLE + "=false",
        "-D", GridmixJob.GRIDMIX_HIGHRAM_EMULATION_ENABLE + "=false",
        "-D", GridMixConfig.GRIDMIX_COMPRESSION_ENABLE + "=false"
    };

    // Generate the input data by using gridmix framework.
    int exitCode = 
        UtilsForGridmix.runGridmixJob(gridmixDir, conf, 
            GridMixRunMode.DATA_GENERATION.getValue(), 
            runtimeValues, otherArgs);
    Assert.assertEquals("Data generation has failed.", 0 , exitCode);
    // Create the files without using gridmix input generation with 
    // above mentioned sizes in a array.
    createFiles(new Path(gridmixDir, "input"), fileSizesInMB);
    conf.setLong(FilePool.GRIDMIX_MIN_FILE, 100 * 1024 * 1024);
    FilePool fpool = new FilePool(conf, new Path(gridmixDir, "input"));
    fpool.refresh();
    verifyFilesSizeAndCountForSpecifiedPool(expFileCount, targetSize, fpool);
  }
  
  private void createFiles(Path inputDir, int [] fileSizes) 
      throws Exception { 
    for (int size : fileSizes) {
      UtilsForGridmix.createFile(size, inputDir, conf);
    }
  }
  
  private void verifyFilesSizeAndCountForSpecifiedPool(int expFileCount, 
      long minFileSize, FilePool pool) throws IOException {
    final ArrayList<FileStatus> files = new ArrayList<FileStatus>();
    long filesSizeInBytes = pool.getInputFiles(minFileSize, files);
    long actFilesSizeInMB = filesSizeInBytes / (1024 * 1024);
    long expFilesSizeInMB = (clusterSize * 200) + 1300;
    Assert.assertEquals("Files Size has not matched for specified pool.", 
                        expFilesSizeInMB, actFilesSizeInMB);
    int actFileCount = files.size();
    Assert.assertEquals("File count has not matched.", expFileCount, 
                        actFileCount);
    int count = 0;
    for (FileStatus fstat : files) {
      String fp = fstat.getPath().toString();
      count = count + ((fp.indexOf("datafile_") > 0)? 0 : 1);
    }
    Assert.assertEquals("Total folders are not matched with cluster size", 
                        clusterSize, count);
  }
}
