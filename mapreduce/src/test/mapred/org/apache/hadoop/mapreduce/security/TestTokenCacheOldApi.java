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

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.crypto.KeyGenerator;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.EmptyInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;


@SuppressWarnings("deprecation")
public class TestTokenCacheOldApi {
  private static final int NUM_OF_KEYS = 10;

  // my sleep class - adds check for tokenCache
  static class MyDummyJob extends Configured implements Tool,
      Mapper<IntWritable, IntWritable, IntWritable, NullWritable>,
      Reducer<IntWritable, NullWritable, NullWritable, NullWritable>,
      Partitioner<IntWritable, NullWritable> {
    Credentials ts;

    public void configure(JobConf job) {
    }
    
    /**
     * attempts to access tokenCache as from client
     */
    public void map(IntWritable key, IntWritable value,
        OutputCollector<IntWritable, NullWritable> output, Reporter reporter)
        throws IOException {
      // get token storage and a key
      byte[] key1 = ts.getSecretKey(new Text("alias1"));
      Collection<Token<? extends TokenIdentifier>> dts = ts.getAllTokens();
      int dts_size = 0;
      if(dts != null)
        dts_size = dts.size();

      if(dts.size() != 2) { // one job token and one delegation token
        throw new RuntimeException("tokens are not available"); // fail the test
      }

      if(key1 == null || ts == null || ts.numberOfSecretKeys() != NUM_OF_KEYS) {
        throw new RuntimeException("secret keys are not available"); // fail the test
      }
      
      output.collect(new IntWritable(1), NullWritable.get());
    }
    
    public JobConf setupJobConf() {
      
      JobConf job = new JobConf(getConf(), MyDummyJob.class);
      job.setNumMapTasks(1);
      job.setNumReduceTasks(1);
      job.setMapperClass(MyDummyJob.class);
      job.setMapOutputKeyClass(IntWritable.class);
      job.setMapOutputValueClass(NullWritable.class);
      job.setReducerClass(MyDummyJob.class);
      job.setOutputFormat(NullOutputFormat.class);
      job.setInputFormat(EmptyInputFormat.class);
      job.setPartitionerClass(MyDummyJob.class);
      job.setSpeculativeExecution(false);
      job.setJobName("Sleep job");
      populateTokens(job);
      return job;
    }
    
    private void populateTokens(JobConf job) {
      // Credentials in the job will not have delegation tokens
      // because security is disabled. Fetch delegation tokens
      // and populate the credential in the job.
      try {
        Credentials ts = job.getCredentials();
        Path p1 = new Path("file1");
        p1 = p1.getFileSystem(job).makeQualified(p1);
        Credentials cred = new Credentials();
        TokenCache.obtainTokensForNamenodesInternal(cred, new Path[] { p1 },
            job);
        for (Token<? extends TokenIdentifier> t : cred.getAllTokens()) {
          ts.addToken(new Text("Hdfs"), t);
        }
      } catch (IOException e) {
        Assert.fail("Exception " + e);
      }
    }

    public void close() throws IOException {
    }

    public void reduce(IntWritable key, Iterator<NullWritable> values,
        OutputCollector<NullWritable, NullWritable> output, Reporter reporter)
        throws IOException {
      return;
    }

    public int getPartition(IntWritable key, NullWritable value,
        int numPartitions) {
      return key.get() % numPartitions;
    }

    public int run(String[] args) throws Exception {
      JobConf job = setupJobConf();
      JobClient.runJob(job);
      return 0;
    }
  }
  
  private static MiniMRCluster mrCluster;
  private static MiniDFSCluster dfsCluster;
  private static final Path TEST_DIR = 
    new Path(System.getProperty("test.build.data","/tmp"), "sleepTest");
  private static final Path tokenFileName = new Path(TEST_DIR, "tokenFile.json");
  private static int numSlaves = 1;
  private static JobConf jConf;
  private static ObjectMapper mapper = new ObjectMapper();
  private static Path p1;
  private static Path p2;
  
  @BeforeClass
  public static void setUp() throws Exception {
    Configuration conf = new Configuration();
    dfsCluster = new MiniDFSCluster(conf, numSlaves, true, null);
    jConf = new JobConf(conf);
    mrCluster = new MiniMRCluster(0, 0, numSlaves, 
        dfsCluster.getFileSystem().getUri().toString(), 1, null, null, null, 
        jConf);
    
    createTokenFileJson();
    verifySecretKeysInJSONFile();
    dfsCluster.getNamesystem()
				.getDelegationTokenSecretManager().startThreads();
    FileSystem fs = dfsCluster.getFileSystem();
    
    p1 = new Path("file1");
    p2 = new Path("file2");
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

  // create jason file and put some keys into it..
  private static void createTokenFileJson() throws IOException {
    Map<String, String> map = new HashMap<String, String>();
    
    try {
      KeyGenerator kg = KeyGenerator.getInstance("HmacSHA1");
      for(int i=0; i<NUM_OF_KEYS; i++) {
        SecretKeySpec key = (SecretKeySpec) kg.generateKey();
        byte [] enc_key = key.getEncoded();
        map.put("alias"+i, new String(Base64.encodeBase64(enc_key)));

      }
    } catch (NoSuchAlgorithmException e) {
      throw new IOException(e);
    }
    
    try {
      File p  = new File(tokenFileName.getParent().toString());
      p.mkdirs();
      // convert to JSON and save to the file
      mapper.writeValue(new File(tokenFileName.toString()), map);

    } catch (Exception e) {
      System.out.println("failed with :" + e.getLocalizedMessage());
    }
  }
  
  @SuppressWarnings("unchecked")
  private static void verifySecretKeysInJSONFile() throws IOException {
    Map<String, String> map;
    map = mapper.readValue(new File(tokenFileName.toString()), Map.class);
    assertEquals("didn't read JSON correctly", map.size(), NUM_OF_KEYS);
  }
  
  /**
   * run a distributed job and verify that TokenCache is available
   * @throws IOException
   */
  @Test
  public void testTokenCache() throws IOException {
    // make sure JT starts
    jConf = mrCluster.createJobConf();
    
    // provide namenodes names for the job to get the delegation tokens for
    //String nnUri = dfsCluster.getNameNode().getUri(namenode).toString();
    NameNode nn = dfsCluster.getNameNode();
    URI nnUri = NameNode.getUri(nn.getNameNodeAddress());
    jConf.set(JobContext.JOB_NAMENODES, nnUri + "," + nnUri.toString());
    // job tracker principle id..
    jConf.set(MRConfig.MASTER_USER_NAME, "jt_id");

    // using argument to pass the file name
    String[] args = {
       "-tokenCacheFile", tokenFileName.toString(), 
        "-m", "1", "-r", "1", "-mt", "1", "-rt", "1"
        };
     
    int res = -1;
    try {
      res = ToolRunner.run(jConf, new MyDummyJob(), args);
    } catch (Exception e) {
      System.out.println("Job failed with" + e.getLocalizedMessage());
      e.printStackTrace(System.out);
      Assert.fail("Job failed");
    }
    assertEquals("dist job res is not 0", res, 0);
  }
  
  /**
   * run a local job and verify that TokenCache is available
   * @throws NoSuchAlgorithmException
   * @throws IOException
   */
  @Test
  public void testLocalJobTokenCache() throws NoSuchAlgorithmException, IOException {
    // this is local job
    String[] args = {"-m", "1", "-r", "1", "-mt", "1", "-rt", "1"}; 
    jConf.set("mapreduce.job.credentials.json", tokenFileName.toString());

    int res = -1;
    try {
      res = ToolRunner.run(jConf, new MyDummyJob(), args);
    } catch (Exception e) {
      System.out.println("Job failed with" + e.getLocalizedMessage());
      e.printStackTrace(System.out);
      fail("local Job failed");
    }
    assertEquals("local job res is not 0", res, 0);
  }
}
