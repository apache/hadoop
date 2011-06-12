/** Licensed to the Apache Software Foundation (ASF) under one
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

package org.apache.hadoop.mapreduce.security;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.SleepJob;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.ToolRunner;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

@SuppressWarnings("deprecation")
public class TestBinaryTokenFile {

  // my sleep class
  static class MySleepMapper extends SleepJob.SleepMapper {
    /**
     * attempts to access tokenCache as from client
     */
    @Override
    public void map(IntWritable key, IntWritable value, Context context)
    throws IOException, InterruptedException {
      // get token storage and a key
      Credentials ts = context.getCredentials();
      Collection<Token<? extends TokenIdentifier>> dts = ts.getAllTokens();
      
      
      if(dts.size() != 2) { // one job token and one delegation token
        throw new RuntimeException("tokens are not available"); // fail the test
      }
      
      Token<? extends TokenIdentifier> dt = ts.getToken(new Text("Hdfs"));
      
      //Verify that dt is same as the token in the file
      String tokenFile = context.getConfiguration().get(
          "mapreduce.job.credentials.binary");
      Credentials cred = new Credentials();
      cred.readTokenStorageStream(new DataInputStream(new FileInputStream(
          tokenFile)));
      for (Token<? extends TokenIdentifier> t : cred.getAllTokens()) {
        if (!dt.equals(t)) {
          throw new RuntimeException(
              "Delegation token in job is not same as the token passed in file."
                  + " tokenInFile=" + t + ", dt=" + dt);
        }
      }
      
      super.map(key, value, context);
    }
  }
  
  class MySleepJob extends SleepJob {
    @Override
    public Job createJob(int numMapper, int numReducer, 
        long mapSleepTime, int mapSleepCount, 
        long reduceSleepTime, int reduceSleepCount) 
    throws IOException {
      Job job =  super.createJob(numMapper, numReducer,
           mapSleepTime, mapSleepCount, 
          reduceSleepTime, reduceSleepCount);
      
      job.setMapperClass(MySleepMapper.class);
      //Populate tokens here because security is disabled.
      setupBinaryTokenFile(job);
      return job;
    }
    
    private void setupBinaryTokenFile(Job job) {
    // Credentials in the job will not have delegation tokens
    // because security is disabled. Fetch delegation tokens
    // and store in binary token file.
      try {
        Credentials cred1 = new Credentials();
        Credentials cred2 = new Credentials();
        TokenCache.obtainTokensForNamenodesInternal(cred1, new Path[] { p1 },
            job.getConfiguration());
        for (Token<? extends TokenIdentifier> t : cred1.getAllTokens()) {
          cred2.addToken(new Text("Hdfs"), t);
        }
        DataOutputStream os = new DataOutputStream(new FileOutputStream(
            binaryTokenFileName.toString()));
        cred2.writeTokenStorageToStream(os);
        os.close();
        job.getConfiguration().set("mapreduce.job.credentials.binary",
            binaryTokenFileName.toString());
      } catch (IOException e) {
        Assert.fail("Exception " + e);
      }
    }
  }
  
  private static MiniMRCluster mrCluster;
  private static MiniDFSCluster dfsCluster;
  private static final Path TEST_DIR = 
    new Path(System.getProperty("test.build.data","/tmp"));
  private static final Path binaryTokenFileName = new Path(TEST_DIR, "tokenFile.binary");
  private static int numSlaves = 1;
  private static JobConf jConf;
  private static Path p1;
  
  @BeforeClass
  public static void setUp() throws Exception {
    Configuration conf = new Configuration();
    dfsCluster = new MiniDFSCluster(conf, numSlaves, true, null);
    jConf = new JobConf(conf);
    mrCluster = new MiniMRCluster(0, 0, numSlaves, 
        dfsCluster.getFileSystem().getUri().toString(), 1, null, null, null, 
        jConf);

    dfsCluster.getNamesystem().getDelegationTokenSecretManager().startThreads();
    FileSystem fs = dfsCluster.getFileSystem();
    
    p1 = new Path("file1");
    p1 = fs.makeQualified(p1);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    if(mrCluster != null)
      mrCluster.shutdown();
    mrCluster = null;
    if(dfsCluster != null)
      dfsCluster.shutdown();
    dfsCluster = null;
  }
  
  /**
   * run a distributed job and verify that TokenCache is available
   * @throws IOException
   */
  @Test
  public void testBinaryTokenFile() throws IOException {
    
    System.out.println("running dist job");
    
    // make sure JT starts
    jConf = mrCluster.createJobConf();
    
    // provide namenodes names for the job to get the delegation tokens for
    String nnUri = dfsCluster.getURI(0).toString();
    jConf.set(MRJobConfig.JOB_NAMENODES, nnUri + "," + nnUri);
    // job tracker principla id..
    jConf.set(MRConfig.MASTER_USER_NAME, "jt_id");
    
    // using argument to pass the file name
    String[] args = { 
        "-m", "1", "-r", "1", "-mt", "1", "-rt", "1"
        };
     
    int res = -1;
    try {
      res = ToolRunner.run(jConf, new MySleepJob(), args);
    } catch (Exception e) {
      System.out.println("Job failed with" + e.getLocalizedMessage());
      e.printStackTrace(System.out);
      fail("Job failed");
    }
    assertEquals("dist job res is not 0", res, 0);
  }
}
