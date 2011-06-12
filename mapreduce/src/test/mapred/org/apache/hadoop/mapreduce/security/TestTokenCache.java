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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import javax.crypto.KeyGenerator;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.SleepJob;
import org.apache.hadoop.security.TokenStorage;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolRunner;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestTokenCache {
  private static final int NUM_OF_KEYS = 10;

  // my sleep class - adds check for tokenCache
  static class MySleepMapper extends SleepJob.SleepMapper {
    /**
     * attempts to access tokenCache as from client
     */
    @Override
    public void map(IntWritable key, IntWritable value, Context context)
    throws IOException, InterruptedException {
      // get token storage and a key
      TokenStorage ts = TokenCache.getTokenStorage();
      byte[] key1 = TokenCache.getSecretKey(new Text("alias1"));
      Collection<Token<? extends TokenIdentifier>> dts = TokenCache.getAllTokens();
      int dts_size = 0;
      if(dts != null)
        dts_size = dts.size();
      
      System.out.println("inside MAP: ts==NULL?=" + (ts==null) + 
          "; #keys = " + (ts==null? 0:ts.numberOfSecretKeys()) + 
          ";jobToken = " +  (ts==null? "n/a":TokenCache.getJobToken(ts)) +
          "; alias1 key=" + new String(key1) + 
          "; dts size= " + dts_size);
    
      for(Token<? extends TokenIdentifier> t : dts) {
        System.out.println(t.getKind() + "=" + StringUtils.byteToHexString(t.getPassword()));
      }
      
      if(dts.size() != 2) { // one job token and one delegation token
        throw new RuntimeException("tokens are not available"); // fail the test
      }
      
      
      if(key1 == null || ts == null || ts.numberOfSecretKeys() != NUM_OF_KEYS) {
        throw new RuntimeException("secret keys are not available"); // fail the test
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
      return job;
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
    dfsCluster.getNamesystem().getDelegationTokenSecretManager().startThreads();
    FileSystem fs = dfsCluster.getFileSystem();
    
    p1 = new Path("file1");
    p2 = new Path("file2");
    
    p1 = fs.makeQualified(p1);
    // do not qualify p2
    TokenCache.setTokenStorage(new TokenStorage());
    TokenCache.obtainTokensForNamenodesInternal(new Path [] {p1, p2}, jConf);
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
    
    System.out.println("writing secret keys into " + tokenFileName);
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
    
    System.out.println("file " + tokenFileName + " verified; size="+ map.size());
  }
  
  /**
   * run a distributed job and verify that TokenCache is available
   * @throws IOException
   */
  @Test
  public void testTokenCache() throws IOException {
    
    System.out.println("running dist job");
    
    // make sure JT starts
    jConf = mrCluster.createJobConf();
    
    // provide namenodes names for the job to get the delegation tokens for
    String nnUri = dfsCluster.getURI().toString();
    jConf.set(MRJobConfig.JOB_NAMENODES, nnUri + "," + nnUri);
    // job tracker principla id..
    jConf.set(MRJobConfig.JOB_JOBTRACKER_ID, "jt_id");
    
    // using argument to pass the file name
    String[] args = {
       "-tokenCacheFile", tokenFileName.toString(), 
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
  
  /**
   * run a local job and verify that TokenCache is available
   * @throws NoSuchAlgorithmException
   * @throws IOException
   */
  @Test
  public void testLocalJobTokenCache() throws NoSuchAlgorithmException, IOException {
    
    System.out.println("running local job");
    // this is local job
    String[] args = {"-m", "1", "-r", "1", "-mt", "1", "-rt", "1"}; 
    jConf.set("tokenCacheFile", tokenFileName.toString());
    
    int res = -1;
    try {
      res = ToolRunner.run(jConf, new MySleepJob(), args);
    } catch (Exception e) {
      System.out.println("Job failed with" + e.getLocalizedMessage());
      e.printStackTrace(System.out);
      fail("local Job failed");
    }
    assertEquals("local job res is not 0", res, 0);
  }
  
  @Test
  public void testGetTokensForNamenodes() throws IOException {
    FileSystem fs = dfsCluster.getFileSystem();
    
    // this token is keyed by hostname:port key.
    String fs_addr = TokenCache.buildDTServiceName(p1.toUri());
    Token<DelegationTokenIdentifier> nnt = TokenCache.getDelegationToken(fs_addr);
    System.out.println("dt for " + p1 + "(" + fs_addr + ")" + " = " +  nnt);
    assertNotNull("Token for nn is null", nnt);
    
    // verify the size
    Collection<Token<? extends TokenIdentifier>> tns = TokenCache.getAllTokens();
    assertEquals("number of tokens is not 1", 1, tns.size());
    
    boolean found = false;
    for(Token<? extends TokenIdentifier> t: tns) {
      System.out.println("kind="+t.getKind() + ";servic=" + t.getService() + ";str=" + t.toString());
      
      if(t.getKind().equals(DelegationTokenIdentifier.HDFS_DELEGATION_KIND) &&
          t.getService().equals(new Text(fs_addr))) {
        found = true;
      }
      assertTrue("didn't find token for " + p1 ,found);
    }
  }
}
