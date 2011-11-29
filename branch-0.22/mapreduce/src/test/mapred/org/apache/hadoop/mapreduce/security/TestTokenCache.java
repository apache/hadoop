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
import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
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
import org.apache.hadoop.hdfs.HftpFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenSecretManager;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.SleepJob;
import org.apache.hadoop.mapreduce.server.jobtracker.JTConfig;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.ToolRunner;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

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
      Credentials ts = context.getCredentials();
      byte[] key1 = ts.getSecretKey(new Text("alias1"));
      Collection<Token<? extends TokenIdentifier>> dts = ts.getAllTokens();
      int dts_size = 0;
      if(dts != null)
        dts_size = dts.size();
      
      
      if(dts_size != 2) { // one job token and one delegation token
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
      //Populate tokens here because security is disabled.
      populateTokens(job);
      return job;
    }
    
    private void populateTokens(Job job) {
    // Credentials in the job will not have delegation tokens
    // because security is disabled. Fetch delegation tokens
    // and populate the credential in the job.
      try {
        Credentials ts = job.getCredentials();
        Path p1 = new Path("file1");
        p1 = p1.getFileSystem(job.getConfiguration()).makeQualified(p1);
        Credentials cred = new Credentials();
        TokenCache.obtainTokensForNamenodesInternal(cred, new Path[] { p1 },
            job.getConfiguration());
        for (Token<? extends TokenIdentifier> t : cred.getAllTokens()) {
          ts.addToken(new Text("Hdfs"), t);
        }
      } catch (IOException e) {
        Assert.fail("Exception " + e);
      }
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
    
    System.out.println("running dist job");
    
    // make sure JT starts
    jConf = mrCluster.createJobConf();
    
    // provide namenodes names for the job to get the delegation tokens for
    String nnUri = dfsCluster.getURI().toString();
    jConf.set(MRJobConfig.JOB_NAMENODES, nnUri + "," + nnUri);
    // job tracker principla id..
    jConf.set(JTConfig.JT_USER_NAME, "jt_id");
    
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
    jConf.set("mapreduce.job.credentials.json", tokenFileName.toString());
    
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
    
    Credentials credentials = new Credentials();
    TokenCache.obtainTokensForNamenodesInternal(credentials, new Path[] { p1,
        p2 }, jConf);

    // this token is keyed by hostname:port key.
    String fs_addr = 
      SecurityUtil.buildDTServiceName(p1.toUri(), NameNode.DEFAULT_PORT);
    Token<DelegationTokenIdentifier> nnt = TokenCache.getDelegationToken(
        credentials, fs_addr);
    System.out.println("dt for " + p1 + "(" + fs_addr + ")" + " = " +  nnt);
    assertNotNull("Token for nn is null", nnt);
    
    // verify the size
    Collection<Token<? extends TokenIdentifier>> tns = credentials.getAllTokens();
    assertEquals("number of tokens is not 1", 1, tns.size());
    
    boolean found = false;
    for(Token<? extends TokenIdentifier> t: tns) {
      if(t.getKind().equals(DelegationTokenIdentifier.HDFS_DELEGATION_KIND) &&
          t.getService().equals(new Text(fs_addr))) {
        found = true;
      }
      assertTrue("didn't find token for " + p1 ,found);
    }
  }
  
  @Test
  public void testGetTokensForHftpFS() throws IOException, URISyntaxException {
    HftpFileSystem hfs = mock(HftpFileSystem.class);

    DelegationTokenSecretManager dtSecretManager = 
      dfsCluster.getNamesystem().getDelegationTokenSecretManager();
    String renewer = "renewer";
    jConf.set(JTConfig.JT_USER_NAME,renewer);
    DelegationTokenIdentifier dtId = 
      new DelegationTokenIdentifier(new Text("user"), new Text(renewer), null);
    final Token<DelegationTokenIdentifier> t = 
      new Token<DelegationTokenIdentifier>(dtId, dtSecretManager);

    final URI uri = new URI("hftp://host:2222/file1");
    final String fs_addr = 
      SecurityUtil.buildDTServiceName(uri, NameNode.DEFAULT_PORT);
    t.setService(new Text(fs_addr));

    //when(hfs.getUri()).thenReturn(uri);
    Mockito.doAnswer(new Answer<URI>(){
      @Override
      public URI answer(InvocationOnMock invocation)
      throws Throwable {
        return uri;
      }}).when(hfs).getUri();

    //when(hfs.getDelegationToken()).thenReturn((Token<? extends TokenIdentifier>) t);
    Mockito.doAnswer(new Answer<Token<DelegationTokenIdentifier>>(){
      @Override
      public Token<DelegationTokenIdentifier>  answer(InvocationOnMock invocation)
      throws Throwable {
        return t;
      }}).when(hfs).getDelegationToken(renewer);
    
    //when(hfs.getCanonicalServiceName).thenReturn(fs_addr);
    Mockito.doAnswer(new Answer<String>(){
      @Override
      public String answer(InvocationOnMock invocation)
      throws Throwable {
        return fs_addr;
      }}).when(hfs).getCanonicalServiceName();
    
    Credentials credentials = new Credentials();
    Path p = new Path(uri.toString());
    System.out.println("Path for hftp="+ p + "; fs_addr="+fs_addr + "; rn=" + renewer);
    TokenCache.obtainTokensForNamenodesInternal(hfs, credentials, jConf);

    Collection<Token<? extends TokenIdentifier>> tns = credentials.getAllTokens();
    assertEquals("number of tokens is not 1", 1, tns.size());

    boolean found = false;
    for(Token<? extends TokenIdentifier> tt: tns) {
      System.out.println("token="+tt);
      if(tt.getKind().equals(DelegationTokenIdentifier.HDFS_DELEGATION_KIND) &&
          tt.getService().equals(new Text(fs_addr))) {
        found = true;
        assertEquals("different token", tt, t);
      }
      assertTrue("didn't find token for " + p, found);
    }
  }

}
