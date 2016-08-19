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
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.SleepJob;
import org.apache.hadoop.mapreduce.v2.MiniMRYarnCluster;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestBinaryTokenFile {

  private static final String KEY_SECURITY_TOKEN_FILE_NAME = "key-security-token-file";
  private static final String DELEGATION_TOKEN_KEY = "Hdfs";

  // my sleep class
  static class MySleepMapper extends SleepJob.SleepMapper {
    /**
     * attempts to access tokenCache as from client
     */
    @Override
    public void map(IntWritable key, IntWritable value, Context context)
    throws IOException, InterruptedException {
      // get context token storage:
      final Credentials contextCredentials = context.getCredentials();

      final Collection<Token<? extends TokenIdentifier>> contextTokenCollection = contextCredentials.getAllTokens();
      for (Token<? extends TokenIdentifier> t : contextTokenCollection) {
        System.out.println("Context token: [" + t + "]");
      }
      if (contextTokenCollection.size() != 2) { // one job token and one delegation token
        // fail the test:
        throw new RuntimeException("Exactly 2 tokens are expected in the contextTokenCollection: " +
        		"one job token and one delegation token, but was found " + contextTokenCollection.size() + " tokens.");
      }

      final Token<? extends TokenIdentifier> dt = contextCredentials.getToken(new Text(DELEGATION_TOKEN_KEY));
      if (dt == null) {
        throw new RuntimeException("Token for key ["+DELEGATION_TOKEN_KEY+"] not found in the job context.");
      }

      String tokenFile0 = context.getConfiguration().get(MRJobConfig.MAPREDUCE_JOB_CREDENTIALS_BINARY);
      if (tokenFile0 != null) {
        throw new RuntimeException("Token file key ["+MRJobConfig.MAPREDUCE_JOB_CREDENTIALS_BINARY+"] found in the configuration. It should have been removed from the configuration.");
      }

      final String tokenFile = context.getConfiguration().get(KEY_SECURITY_TOKEN_FILE_NAME);
      if (tokenFile == null) {
        throw new RuntimeException("Token file key ["+KEY_SECURITY_TOKEN_FILE_NAME+"] not found in the job configuration.");
      }
      final Credentials binaryCredentials = new Credentials();
      binaryCredentials.readTokenStorageStream(new DataInputStream(new FileInputStream(
          tokenFile)));
      final Collection<Token<? extends TokenIdentifier>> binaryTokenCollection = binaryCredentials.getAllTokens();
      if (binaryTokenCollection.size() != 1) {
        throw new RuntimeException("The token collection read from file ["+tokenFile+"] must have size = 1.");
      }
      final Token<? extends TokenIdentifier> binTok = binaryTokenCollection
          .iterator().next();
      System.out.println("The token read from binary file: t = [" + binTok + "]");
      // Verify that dt is same as the token in the file:
      if (!dt.equals(binTok)) {
        throw new RuntimeException(
              "Delegation token in job is not same as the token passed in file:"
                  + " tokenInFile=[" + binTok + "], dt=[" + dt + "].");
      }

      // Now test the user tokens.
      final UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
      // Print all the UGI tokens for diagnostic purposes:
      final Collection<Token<? extends TokenIdentifier>> ugiTokenCollection = ugi.getTokens();
      for (Token<? extends TokenIdentifier> t: ugiTokenCollection) {
        System.out.println("UGI token: [" + t + "]");
      }
      final Token<? extends TokenIdentifier> ugiToken
        = ugi.getCredentials().getToken(new Text(DELEGATION_TOKEN_KEY));
      if (ugiToken == null) {
        throw new RuntimeException("Token for key ["+DELEGATION_TOKEN_KEY+"] not found among the UGI tokens.");
      }
      if (!ugiToken.equals(binTok)) {
        throw new RuntimeException(
              "UGI token is not same as the token passed in binary file:"
                  + " tokenInBinFile=[" + binTok + "], ugiTok=[" + ugiToken + "].");
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
      createBinaryTokenFile(job.getConfiguration());
      job.getConfiguration().set(MRJobConfig.MAPREDUCE_JOB_CREDENTIALS_BINARY,
          binaryTokenFileName.toString());
      // NB: the MRJobConfig.MAPREDUCE_JOB_CREDENTIALS_BINARY
      // key now gets deleted from config,
      // so it's not accessible in the job's config. So,
      // we use another key to pass the file name into the job configuration:
      job.getConfiguration().set(KEY_SECURITY_TOKEN_FILE_NAME,
          binaryTokenFileName.toString());
    }
  }

  private static MiniMRYarnCluster mrCluster;
  private static MiniDFSCluster dfsCluster;

  private static final Path TEST_DIR =
    new Path(System.getProperty("test.build.data","/tmp"));
  private static final Path binaryTokenFileName = new Path(TEST_DIR, "tokenFile.binary");

  private static final int NUMWORKERS = 1; // num of data nodes
  private static final int noOfNMs = 1;

  private static Path p1;

  @BeforeClass
  public static void setUp() throws Exception {
    final Configuration conf = new Configuration();

    conf.set(MRConfig.FRAMEWORK_NAME, MRConfig.YARN_FRAMEWORK_NAME);
    conf.set(YarnConfiguration.RM_PRINCIPAL, "jt_id/" + SecurityUtil.HOSTNAME_PATTERN + "@APACHE.ORG");

    final MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
    builder.checkExitOnShutdown(true);
    builder.numDataNodes(NUMWORKERS);
    builder.format(true);
    builder.racks(null);
    dfsCluster = builder.build();

    mrCluster = new MiniMRYarnCluster(TestBinaryTokenFile.class.getName(), noOfNMs);
    mrCluster.init(conf);
    mrCluster.start();

    NameNodeAdapter.getDtSecretManager(dfsCluster.getNamesystem())
        .startThreads();

    FileSystem fs = dfsCluster.getFileSystem();
    p1 = new Path("file1");
    p1 = fs.makeQualified(p1);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    if(mrCluster != null) {
      mrCluster.stop();
      mrCluster = null;
    }
    if(dfsCluster != null) {
      dfsCluster.shutdown();
      dfsCluster = null;
    }
  }

  private static void createBinaryTokenFile(Configuration conf) {
    // Fetch delegation tokens and store in binary token file.
    try {
      Credentials cred1 = new Credentials();
      Credentials cred2 = new Credentials();
      TokenCache.obtainTokensForNamenodesInternal(cred1, new Path[] { p1 },
          conf);
      for (Token<? extends TokenIdentifier> t : cred1.getAllTokens()) {
        cred2.addToken(new Text(DELEGATION_TOKEN_KEY), t);
      }
      DataOutputStream os = new DataOutputStream(new FileOutputStream(
          binaryTokenFileName.toString()));
      try {
        cred2.writeTokenStorageToStream(os);
      } finally {
        os.close();
      }
    } catch (IOException e) {
      Assert.fail("Exception " + e);
    }
  }

  /**
   * run a distributed job and verify that TokenCache is available
   * @throws IOException
   */
  @Test
  public void testBinaryTokenFile() throws IOException {
    Configuration conf = mrCluster.getConfig();

    // provide namenodes names for the job to get the delegation tokens for
    final String nnUri = dfsCluster.getURI(0).toString();
    conf.set(MRJobConfig.JOB_NAMENODES, nnUri + "," + nnUri);

    // using argument to pass the file name
    final String[] args = {
        "-m", "1", "-r", "1", "-mt", "1", "-rt", "1"
        };
    int res = -1;
    try {
      res = ToolRunner.run(conf, new MySleepJob(), args);
    } catch (Exception e) {
      System.out.println("Job failed with " + e.getLocalizedMessage());
      e.printStackTrace(System.out);
      fail("Job failed");
    }
    assertEquals("dist job res is not 0:", 0, res);
  }

  /**
   * run a distributed job with -tokenCacheFile option parameter and
   * verify that no exception happens.
   * @throws IOException
  */
  @Test
  public void testTokenCacheFile() throws IOException {
    Configuration conf = mrCluster.getConfig();
    createBinaryTokenFile(conf);
    // provide namenodes names for the job to get the delegation tokens for
    final String nnUri = dfsCluster.getURI(0).toString();
    conf.set(MRJobConfig.JOB_NAMENODES, nnUri + "," + nnUri);

    // using argument to pass the file name
    final String[] args = {
        "-tokenCacheFile", binaryTokenFileName.toString(),
        "-m", "1", "-r", "1", "-mt", "1", "-rt", "1"
        };
    int res = -1;
    try {
      res = ToolRunner.run(conf, new SleepJob(), args);
    } catch (Exception e) {
      System.out.println("Job failed with " + e.getLocalizedMessage());
      e.printStackTrace(System.out);
      fail("Job failed");
    }
    assertEquals("dist job res is not 0:", 0, res);
  }
}
