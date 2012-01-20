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
package org.apache.hadoop.tools.rumen;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.ID;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.MapReduceTestUtil;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.jobhistory.JobHistory;
import org.apache.hadoop.mapreduce.server.tasktracker.TTConfig;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.tools.rumen.anonymization.*;
import org.apache.hadoop.tools.rumen.datatypes.*;
import org.apache.hadoop.tools.rumen.datatypes.FileName.FileNameState;
import org.apache.hadoop.tools.rumen.datatypes.NodeName.NodeNameState;
import org.apache.hadoop.tools.rumen.datatypes.util.DefaultJobPropertiesParser;
import org.apache.hadoop.tools.rumen.datatypes.util.MapReduceJobPropertiesParser;
import org.apache.hadoop.tools.rumen.serializers.*;
import org.apache.hadoop.tools.rumen.state.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.Version;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.map.JsonSerializer;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.module.SimpleModule;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Tests Rumen output anonymization.
 */
@SuppressWarnings("deprecation")
public class TestRumenAnonymization {
  /**
   * Test {@link UserName}, serialization and anonymization.
   */
  @Test
  public void testUserNameSerialization() throws IOException {
    JsonSerializer<?> defaultSerializer = new DefaultRumenSerializer();
    
    JsonSerializer<?> anonymizingSerializer = 
      new DefaultAnonymizingRumenSerializer(new StatePool(), 
                                            new Configuration());
    // test username
    UserName uname = new UserName("bob");
    assertEquals("Username error!", "bob", uname.getValue());
    
    // test username serialization
    //   test with no anonymization
    //      test bob
    testSerializer(new UserName("bob"), "bob", defaultSerializer);
    //      test alice
    testSerializer(new UserName("alice"), "alice", defaultSerializer);
    
    // test user-name serialization
    //   test with anonymization
    //      test bob
    testSerializer(new UserName("bob"), "user0", anonymizingSerializer);
    //      test alice
    testSerializer(new UserName("alice"), "user1", anonymizingSerializer);
  }
  
  /**
   * Test {@link JobName}, serialization and anonymization.
   */
  @Test
  public void testJobNameSerialization() throws IOException {
    JsonSerializer<?> defaultSerializer = new DefaultRumenSerializer();
    
    JsonSerializer<?> anonymizingSerializer = 
      new DefaultAnonymizingRumenSerializer(new StatePool(), 
                                            new Configuration());
    
    // test jobname
    JobName jname = new JobName("job-secret");
    assertEquals("Jobname error!", "job-secret", jname.getValue());
    
    // test job-name serialization
    //  test with no anonymization
    //      test job1
    testSerializer(new JobName("job-myjob"), "job-myjob", defaultSerializer);
    //      test job2
    testSerializer(new JobName("job-yourjob"), "job-yourjob", 
                   defaultSerializer);
    
    // test job-name serialization
    //   test with anonymization
    //  test queue1
    testSerializer(new JobName("secret-job-1"), "job0", anonymizingSerializer);
    //      test queue2
    testSerializer(new JobName("secret-job-2"), "job1", anonymizingSerializer);
  }
  
  /**
   * Test {@link QueueName}, serialization and anonymization.
   */
  @Test
  public void testQueueNameSerialization() throws IOException {
    JsonSerializer<?> defaultSerializer = new DefaultRumenSerializer();
    
    JsonSerializer<?> anonymizingSerializer = 
      new DefaultAnonymizingRumenSerializer(new StatePool(), 
                                            new Configuration());
    
    // test queuename
    QueueName qname = new QueueName("queue-secret");
    assertEquals("Queuename error!", "queue-secret", qname.getValue());
    
    // test queuename serialization
    //  test with no anonymization
    //      test queue1
    testSerializer(new QueueName("project1-queue"), 
                   "project1-queue", defaultSerializer);
    //      test queue2
    testSerializer(new QueueName("project2-queue"), 
                   "project2-queue", defaultSerializer);
    
    // test queue-name serialization
    //   test with anonymization
    //  test queue1
    testSerializer(new QueueName("project1-queue"), 
                   "queue0", anonymizingSerializer);
    //      test queue2
    testSerializer(new QueueName("project2-queue"), 
                   "queue1", anonymizingSerializer);
  }
  
  /**
   * Test {@link NodeName}.
   */
  @Test
  public void testNodeNameDataType() throws IOException {
    // test hostname
    //   test only hostname
    NodeName hname = new NodeName("host1.myorg.com");
    assertNull("Expected missing rack name", hname.getRackName());
    assertEquals("Hostname's test#1 hostname error!", 
                 "host1.myorg.com", hname.getHostName());
    assertEquals("Hostname test#1 error!", "host1.myorg.com", hname.getValue());
    
    //   test rack/hostname
    hname = new NodeName("/rack1.myorg.com/host1.myorg.com");
    assertEquals("Hostname's test#2 rackname error!", 
                 "rack1.myorg.com", hname.getRackName());
    assertEquals("Hostname test#2 hostname error!", 
                 "host1.myorg.com", hname.getHostName());
    assertEquals("Hostname test#2 error!", 
                 "/rack1.myorg.com/host1.myorg.com", hname.getValue());
    
    //   test hostname and rackname separately
    hname = new NodeName("rack1.myorg.com", "host1.myorg.com");
    assertEquals("Hostname's test#3 rackname error!", 
                 "rack1.myorg.com", hname.getRackName());
    assertEquals("Hostname test#3 hostname error!", 
                 "host1.myorg.com", hname.getHostName());
    assertEquals("Hostname test#3 error!", 
                 "/rack1.myorg.com/host1.myorg.com", hname.getValue());
    
    //   test hostname with no rackname
    hname = new NodeName(null, "host1.myorg.com");
    assertNull("Hostname's test#4 rackname error!", hname.getRackName());
    assertEquals("Hostname test#4 hostname error!", 
                 "host1.myorg.com", hname.getHostName());
    assertEquals("Hostname test#4 error!", 
                 "host1.myorg.com", hname.getValue());
    
    //  test rackname with no hostname
    hname = new NodeName("rack1.myorg.com", null);
    assertEquals("Hostname test#4 rackname error!", 
                 "rack1.myorg.com", hname.getRackName());
    assertNull("Hostname's test#5 hostname error!", hname.getHostName());
    assertEquals("Hostname test#5 error!", 
                 "rack1.myorg.com", hname.getValue());
  }
  
  /**
   * Test {@link NodeName} serialization.
   */
  @Test
  public void testNodeNameDefaultSerialization() throws IOException {
    JsonSerializer<?> defaultSerializer = new DefaultRumenSerializer();
    
    // test hostname serialization
    //  test with no anonymization
    //      test hostname
    testSerializer(new NodeName("hostname.myorg.com"), "hostname.myorg.com",
                   defaultSerializer);
    //      test rack/hostname
    testSerializer(new NodeName("/rackname.myorg.com/hostname.myorg.com"), 
                   "/rackname.myorg.com/hostname.myorg.com",
                   defaultSerializer);
    //      test rack,hostname
    testSerializer(new NodeName("rackname.myorg.com", "hostname.myorg.com"), 
                   "/rackname.myorg.com/hostname.myorg.com",
                   defaultSerializer);
    //      test -,hostname
    testSerializer(new NodeName(null, "hostname.myorg.com"), 
                   "hostname.myorg.com", defaultSerializer);
    //      test rack,-
    testSerializer(new NodeName("rackname.myorg.com", null), 
                   "rackname.myorg.com", defaultSerializer);
  }
  
  /**
   * Test {@link NodeName} anonymization.
   */
  @Test
  public void testNodeNameAnonymization() throws IOException {
    JsonSerializer<?> anonymizingSerializer = 
      new DefaultAnonymizingRumenSerializer(new StatePool(), 
                                            new Configuration());
    
    // test hostname serializer
    //   test with anonymization
    //      test hostname
    testSerializer(new NodeName("hostname.myorg.com"), "host0",
                   anonymizingSerializer);
    //      test hostname reuse
    testSerializer(new NodeName("hostname213.myorg.com"), "host1",
                   anonymizingSerializer);
    //      test rack/hostname
    testSerializer(new NodeName("/rackname.myorg.com/hostname.myorg.com"), 
                   "/rack0/host0", anonymizingSerializer);
    //  test rack/hostname (hostname reuse)
    testSerializer(new NodeName("/rackname654.myorg.com/hostname.myorg.com"), 
                   "/rack1/host0", anonymizingSerializer);
    //  test rack/hostname (rack reuse)
    testSerializer(new NodeName("/rackname654.myorg.com/hostname765.myorg.com"), 
                   "/rack1/host2", anonymizingSerializer);
    //  test rack,hostname (rack & hostname reuse)
    testSerializer(new NodeName("rackname.myorg.com", "hostname.myorg.com"), 
                   "/rack0/host0", anonymizingSerializer);
    //      test rack,hostname (rack reuse)
    testSerializer(new NodeName("rackname.myorg.com", "hostname543.myorg.com"), 
                   "/rack0/host3", anonymizingSerializer);
    //      test rack,hostname (hostname reuse)
    testSerializer(new NodeName("rackname987.myorg.com", "hostname.myorg.com"), 
                   "/rack2/host0", anonymizingSerializer);
    //      test rack,hostname (rack reuse)
    testSerializer(new NodeName("rackname.myorg.com", "hostname654.myorg.com"), 
                   "/rack0/host4", anonymizingSerializer);
    //      test rack,hostname (host reuse)
    testSerializer(new NodeName("rackname876.myorg.com", "hostname.myorg.com"), 
                   "/rack3/host0", anonymizingSerializer);
    //      test rack,hostname (rack & hostname reuse)
    testSerializer(new NodeName("rackname987.myorg.com", 
                                "hostname543.myorg.com"), 
                   "/rack2/host3", anonymizingSerializer);
    //      test -,hostname (hostname reuse)
    testSerializer(new NodeName(null, "hostname.myorg.com"), 
                   "host0", anonymizingSerializer);
    //      test -,hostname 
    testSerializer(new NodeName(null, "hostname15.myorg.com"), 
                   "host5", anonymizingSerializer);
    //      test rack,- (rack reuse)
    testSerializer(new NodeName("rackname987.myorg.com", null), 
                   "rack2", anonymizingSerializer);
    //      test rack,- 
    testSerializer(new NodeName("rackname15.myorg.com", null), 
                   "rack4", anonymizingSerializer);
  }
  
  /**
   * Test {@link JobProperties}.
   */
  @Test
  public void testJobPropertiesDataType() throws IOException {
    // test job properties
    Properties properties = new Properties();
    JobProperties jp = new JobProperties(properties);
    
    // test empty job properties
    assertEquals("Job Properties (default) store error", 
                 0, jp.getValue().size());
    // test by adding some data
    properties.put("test-key", "test-value"); // user config
    properties.put(MRJobConfig.USER_NAME, "bob"); // job config
    properties.put(JobConf.MAPRED_TASK_JAVA_OPTS, "-Xmx1G"); // deprecated
    jp = new JobProperties(properties);
    assertEquals("Job Properties (default) store error", 
                 3, jp.getValue().size());
    assertEquals("Job Properties (default) key#1 error", 
                 "test-value", jp.getValue().get("test-key"));
    assertEquals("Job Properties (default) key#2 error", 
                 "bob", jp.getValue().get(MRJobConfig.USER_NAME));
    assertEquals("Job Properties (default) key#3 error", 
                 "-Xmx1G", jp.getValue().get(JobConf.MAPRED_TASK_JAVA_OPTS));
  }
  
  /**
   * Test {@link JobProperties} serialization.
   */
  @Test
  public void testJobPropertiesSerialization() throws IOException {
    JsonSerializer<?> defaultSerializer = new DefaultRumenSerializer();
    
    // test job properties
    Properties properties = new Properties();
    properties.put("test-key", "test-value"); // user config
    properties.put(MRJobConfig.USER_NAME, "bob"); // job config
    properties.put(JobConf.MAPRED_TASK_JAVA_OPTS, "-Xmx1G"); // deprecated
    JobProperties jp = new JobProperties(properties);
    
    testSerializer(jp, "{test-key:test-value," 
                       + "mapreduce.job.user.name:bob," 
                       + "mapred.child.java.opts:-Xmx1G}", defaultSerializer);
  }
  
  /**
   * Test {@link JobProperties} anonymization.
   */
  @Test
  public void testJobPropertiesAnonymization() throws IOException {
    // test job properties
    Properties properties = new Properties();
    Configuration conf = new Configuration();
    
    properties.put("test-key", "test-value"); // user config
    properties.put(MRJobConfig.USER_NAME, "bob"); // job config
    // deprecated
    properties.put("mapred.map.child.java.opts", 
                   "-Xmx2G -Xms500m -Dsecret=secret");
    // deprecated and not supported
    properties.put(JobConf.MAPRED_TASK_JAVA_OPTS, 
                   "-Xmx1G -Xms200m -Dsecret=secret");
    JobProperties jp = new JobProperties(properties);
    
    // define a module
    SimpleModule module = new SimpleModule("Test Anonymization Serializer",  
                                           new Version(0, 0, 0, "TEST"));
    // add various serializers to the module
    module.addSerializer(DataType.class, new DefaultRumenSerializer());
    module.addSerializer(AnonymizableDataType.class, 
                         new DefaultAnonymizingRumenSerializer(new StatePool(),
                                                               conf));
    
    //TODO Support deprecated and un-supported keys
    testSerializer(jp, "{mapreduce.job.user.name:user0," 
                       + "mapred.map.child.java.opts:-Xmx2G -Xms500m}", module);
  }
  
  /**
   * Test {@link ClassName}, serialization and anonymization.
   */
  @Test
  public void testClassNameSerialization() throws IOException {
    JsonSerializer<?> defaultSerializer = new DefaultRumenSerializer();
    
    JsonSerializer<?> anonymizingSerializer = 
      new DefaultAnonymizingRumenSerializer(new StatePool(), 
                                            new Configuration());
    
    // test classname
    ClassName cName = new ClassName(TestRumenAnonymization.class.getName());
    assertEquals("Classname error!", TestRumenAnonymization.class.getName(), 
                 cName.getValue());
    
    // test classname serialization
    //  test with no anonymization
    //      test class1
    testSerializer(new ClassName("org.apache.hadoop.Test"), 
                   "org.apache.hadoop.Test", defaultSerializer);
    //      test class2
    testSerializer(new ClassName("org.apache.hadoop.Test2"), 
                   "org.apache.hadoop.Test2", defaultSerializer);
    
    // test class-name serialization
    //  test with anonymization
    //      test class1
    testSerializer(new ClassName("org.apache.hadoop.Test1"), 
                   "class0", anonymizingSerializer);
    //      test class2
    testSerializer(new ClassName("org.apache.hadoop.Test2"), 
                   "class1", anonymizingSerializer);
    
    // test classnames with preserves
    Configuration conf = new Configuration();
    conf.set(ClassName.CLASSNAME_PRESERVE_CONFIG, "org.apache.hadoop.");
    anonymizingSerializer = 
      new DefaultAnonymizingRumenSerializer(new StatePool(), conf);
    // test word with prefix
    testSerializer(new ClassName("org.apache.hadoop.Test3"), 
                   "org.apache.hadoop.Test3", anonymizingSerializer);
    // test word without prefix
    testSerializer(new ClassName("org.apache.hadoop2.Test4"), 
                   "class0", anonymizingSerializer);
  }
  
  /**
   * Test {@link FileName}.
   */
  @Test
  public void testFileName() throws IOException {
    // test file on hdfs
    FileName hFile = new FileName("hdfs://testnn:123/user/test.json");
    assertEquals("Filename error!", "hdfs://testnn:123/user/test.json", 
                 hFile.getValue());
    // test file on local-fs
    hFile = new FileName("file:///user/test.json");
    assertEquals("Filename error!", "file:///user/test.json", 
                 hFile.getValue());
    // test dir on hdfs
    hFile = new FileName("hdfs://testnn:123/user/");
    assertEquals("Filename error!", "hdfs://testnn:123/user/",
                 hFile.getValue());
    // test dir on local-fs
    hFile = new FileName("file:///user/");
    assertEquals("Filename error!", "file:///user/", hFile.getValue());
    // test absolute file
    hFile = new FileName("/user/test/test.json");
    assertEquals("Filename error!", "/user/test/test.json", hFile.getValue());
    // test absolute directory
    hFile = new FileName("/user/test/");
    assertEquals("Filename error!", "/user/test/", hFile.getValue());
    // test relative file
    hFile = new FileName("user/test/test2.json");
    assertEquals("Filename error!", "user/test/test2.json", hFile.getValue());
    // test relative directory
    hFile = new FileName("user/test/");
    assertEquals("Filename error!", "user/test/", hFile.getValue());
    // test absolute file
    hFile = new FileName("user");
    assertEquals("Filename error!", "user", hFile.getValue());
    // test absolute directory
    hFile = new FileName("user/");
    assertEquals("Filename error!", "user/", hFile.getValue());
    hFile = new FileName("./tmp");
    assertEquals("Filename error!","./tmp", hFile.getValue());
    hFile = new FileName("./tmp/");
    assertEquals("Filename error!","./tmp/", hFile.getValue());
    hFile = new FileName("../tmp");
    assertEquals("Filename error!","../tmp", hFile.getValue());
    hFile = new FileName("../tmp/");
    assertEquals("Filename error!","../tmp/", hFile.getValue());
    
    // test comma separated filenames
    //  test hdfs filenames, absolute and local-fs filenames
    hFile = new FileName("hdfs://testnn:123/user/test1," 
                         + "file:///user/test2,/user/test3");
    assertEquals("Filename error!", 
                 "hdfs://testnn:123/user/test1,file:///user/test2,/user/test3", 
                 hFile.getValue());
  }
  
  /**
   * Test {@link FileName} serialization.
   */
  @Test
  public void testFileNameSerialization() throws IOException {
    JsonSerializer<?> defaultSerializer = new DefaultRumenSerializer();
    
    // test filename serialization
    //  test with no anonymization
    //      test a file on hdfs
    testSerializer(new FileName("hdfs://mynn:123/home/user/test.json"), 
                   "hdfs://mynn:123/home/user/test.json", defaultSerializer);
    // test a file on local-fs
    testSerializer(new FileName("file:///home/user/test.json"), 
                   "file:///home/user/test.json", defaultSerializer);
    // test directory on hdfs
    testSerializer(new FileName("hdfs://mynn:123/home/user/"), 
                   "hdfs://mynn:123/home/user/", defaultSerializer);
    // test directory on local fs
    testSerializer(new FileName("file:///home/user/"), 
                   "file:///home/user/", defaultSerializer);
    // test absolute file
    testSerializer(new FileName("/home/user/test.json"), 
                   "/home/user/test.json", defaultSerializer);
    // test relative file
    testSerializer(new FileName("home/user/test.json"), 
                   "home/user/test.json", defaultSerializer);
    // test absolute folder
    testSerializer(new FileName("/home/user/"), "/home/user/", 
                   defaultSerializer);
    // test relative folder
    testSerializer(new FileName("home/user/"), "home/user/", 
                   defaultSerializer);
    // relative file
    testSerializer(new FileName("home"), "home", defaultSerializer);
    // relative folder
    testSerializer(new FileName("home/"), "home/", defaultSerializer);
    // absolute file
    testSerializer(new FileName("/home"), "/home", defaultSerializer);
    // absolute folder
    testSerializer(new FileName("/home/"), "/home/", defaultSerializer);
    // relative folder
    testSerializer(new FileName("./tmp"), "./tmp", defaultSerializer);
    testSerializer(new FileName("./tmp/"), "./tmp/", defaultSerializer);
    testSerializer(new FileName("../tmp"), "../tmp", defaultSerializer);
    
    // test comma separated filenames
    //  test hdfs filenames, absolute and local-fs filenames
    FileName fileName = 
      new FileName("hdfs://testnn:123/user/test1,file:///user/test2,"
                   + "/user/test3");
    testSerializer(fileName, 
        "hdfs://testnn:123/user/test1,file:///user/test2,/user/test3",
        defaultSerializer);
  }
  
  /**
   * Test {@link FileName} anonymization.
   */
  @Test
  public void testFileNameAnonymization() throws IOException {
    JsonSerializer<?> anonymizingSerializer = 
      new DefaultAnonymizingRumenSerializer(new StatePool(), 
                                            new Configuration());
    
    // test filename serialization
    //  test with no anonymization
    //      test hdfs file
    testSerializer(new FileName("hdfs://mynn:123/home/user/bob/test.json"),
        "hdfs://host0/home/user/dir0/test.json", anonymizingSerializer);
    //      test local-fs file
    testSerializer(new FileName("file:///home/user/alice/test.jar"), 
        "file:///home/user/dir1/test.jar", anonymizingSerializer);
    //      test hdfs dir
    testSerializer(new FileName("hdfs://mynn:123/home/user/"),
                   "hdfs://host0/home/user/", anonymizingSerializer);
    //      test local-fs dir
    testSerializer(new FileName("file:///home/user/secret/more-secret/"), 
                   "file:///home/user/dir2/dir3/", anonymizingSerializer);
    //  test absolute filenames
    testSerializer(new FileName("/home/user/top-secret.txt"),
                   "/home/user/file0.txt", anonymizingSerializer);
    //      test relative filenames
    testSerializer(new FileName("home/user/top-top-secret.zip"), 
                   "home/user/file1.zip", anonymizingSerializer);
    //  test absolute dirnames
    testSerializer(new FileName("/home/user/project1/"),
                   "/home/user/dir4/", anonymizingSerializer);
    //      test relative filenames
    testSerializer(new FileName("home/user/project1"), 
                   "home/user/file2", anonymizingSerializer);
    //  test absolute dirnames (re-use)
    testSerializer(new FileName("more-secret/"),
                   "dir3/", anonymizingSerializer);
    //      test relative filenames (re-use)
    testSerializer(new FileName("secret/project1"), 
                   "dir2/file2", anonymizingSerializer);
    //  test absolute filenames (re-use)
    testSerializer(new FileName("/top-secret.txt"),
                   "/file0.txt", anonymizingSerializer);
    //  test relative filenames (re-use)
    testSerializer(new FileName("top-top-secret.tar"), 
                   "file1.tar", anonymizingSerializer);
    //  test absolute dirname
    testSerializer(new FileName("sensitive-projectname/"),
                   "dir5/", anonymizingSerializer);
    //  test relative filenames 
    testSerializer(new FileName("/real-sensitive-projectname/"), 
                   "/dir6/", anonymizingSerializer);
    //  test absolute filenames 
    testSerializer(new FileName("/usernames.xml"),
                   "/file3.xml", anonymizingSerializer);
    //  test relative filenames 
    testSerializer(new FileName("passwords.zip"), 
                   "file4.zip", anonymizingSerializer);
    //  test relative filenames 
    testSerializer(new FileName("./tmp"), 
                   "./tmp", anonymizingSerializer);
    testSerializer(new FileName("./tmp/"), 
                   "./tmp/", anonymizingSerializer);
    testSerializer(new FileName("../tmp"), 
                   "../tmp", anonymizingSerializer);
    testSerializer(new FileName("../tmp/"), 
                   "../tmp/", anonymizingSerializer);
    
    // test comma separated filenames
    //  test hdfs filenames, absolute and local-fs filenames
    FileName fileName = 
      new FileName("hdfs://mynn:123/home/user/bob/test.json," 
                   + "file:///home/user/bob/test.json,/user/alice/test.json");
    testSerializer(fileName, 
        "hdfs://host0/home/user/dir0/test.json,file:///home/user/dir0/test.json"
        + ",/user/dir1/test.json",
        anonymizingSerializer);
  }
  
  
  /**
   * Test {@link DefaultDataType} serialization.
   */
  @Test
  public void testDefaultDataTypeSerialization() throws IOException {
    JsonSerializer<?> defaultSerializer = new DefaultRumenSerializer();
    
    // test default data-type
    DefaultDataType dt = new DefaultDataType("test");
    assertEquals("DefaultDataType error!", "test", dt.getValue());
    
    // test default data-type
    //  test with no anonymization
    //      test data
    testSerializer(new DefaultDataType("test"), "test", defaultSerializer);
  }
  
  // A faked OutputStream which stores the stream content into a StringBuffer.
  private static class MyOutputStream extends OutputStream {
    private StringBuffer data = new StringBuffer();
    
    @Override
    public void write(int b) throws IOException {
      data.append((char)b);
    }
    
    @Override
    public void write(byte[] b) throws IOException {
      data.append(b);
    }
    
    @Override
    public String toString() {
      // remove all the '"' for ease of testing
      return data.toString().trim().replaceAll("\"", "");
    }
  }
  
  // tests the object serializing using the class of the specified object
  @SuppressWarnings("unchecked")
  private static void testSerializer(Object toBeSerialized, String expData, 
                                     JsonSerializer serializer) 
  throws IOException {
    // define a module
    SimpleModule module = new SimpleModule("Test Anonymization Serializer",  
                                           new Version(0, 0, 0, "TEST"));
    // add various serializers to the module
    module.addSerializer(toBeSerialized.getClass(), serializer);
    testSerializer(toBeSerialized, expData, module);
  }
  
  // tests the object serializing using the specified class
  private static void testSerializer(Object toBeSerialized, String expData, 
                                     SimpleModule module) 
  throws IOException {
    // define a custom generator
    ObjectMapper outMapper = new ObjectMapper();
    
    // register the module
    outMapper.registerModule(module);
    
    // get the json factory
    JsonFactory outFactory = outMapper.getJsonFactory();
    // define a fake output stream which will cache the data
    MyOutputStream output = new MyOutputStream();
    // define the json output generator
    JsonGenerator outGen = 
      outFactory.createJsonGenerator(output, JsonEncoding.UTF8);
    
    // serialize the object
    outGen.writeObject(toBeSerialized);
    //serializer.serialize(toBeSerialized, outGen, null);
    
    // close the json generator so that it flushes out the data to the output
    // stream
    outGen.close();
    
    assertEquals("Serialization failed!", expData, output.toString());
  }
  
  /**
   * Test {@link DefaultRumenSerializer}.
   */
  @Test
  public void testDefaultDataSerializers() throws Exception {
    JsonSerializer<?> defaultSer = new DefaultRumenSerializer();
    // test default data-type
    //  test with no anonymization
    //      test data
    testSerializer(new DefaultDataType("test"), "test", defaultSer);
  }
  
  @Test
  public void testBlockingDataSerializers() throws Exception {
    JsonSerializer<?> blockingSerializer = new BlockingSerializer();
    
    // test string serializer
    testSerializer("username:password", "null", blockingSerializer);
  }
  
  @Test
  public void testObjectStringDataSerializers() throws Exception {
    JsonSerializer<?> objectStringSerializer = new ObjectStringSerializer<ID>();
    // test job/task/attempt id serializer
    //   test job-id 
    JobID jid = JobID.forName("job_1_1");
    testSerializer(jid, jid.toString(), objectStringSerializer);
    //   test task-id
    TaskID tid = new TaskID(jid, TaskType.MAP, 1);
    testSerializer(tid, tid.toString(), objectStringSerializer);
    //   test attempt-id
    TaskAttemptID aid = new TaskAttemptID(tid, 0);
    testSerializer(aid, aid.toString(), objectStringSerializer);
  }
  
  // test anonymizer
  @Test
  public void testRumenAnonymization() throws Exception {
    Configuration conf = new Configuration();

    // Run a MR job
    // create a MR cluster
    conf.setInt(TTConfig.TT_MAP_SLOTS, 1);
    conf.setInt(TTConfig.TT_REDUCE_SLOTS, 1);
    
    MiniDFSCluster dfsCluster = null;
    MiniMRCluster mrCluster =  null;
    
    // local filesystem for running TraceBuilder
    FileSystem lfs = FileSystem.getLocal(conf);
    Path rootTempDir =
      new Path(System.getProperty("test.build.data", "/tmp"));

    Path tempDir = new Path(rootTempDir, "testRumenAnonymization");
    tempDir = lfs.makeQualified(tempDir);
    lfs.delete(tempDir, true);
    
    try {
      dfsCluster = new MiniDFSCluster(conf, 1, true, null);
      String[] racks = new String[] {"/rack123.myorg.com", 
                                     "/rack456.myorg.com"};
      String[] hosts = new String[] {"host1230.myorg.com", 
                                     "host4560.myorg.com"};
      mrCluster = 
        new MiniMRCluster(2, dfsCluster.getFileSystem().getUri().toString(), 
                          1, racks, hosts, new JobConf(conf));

      // run a job
      Path inDir = new Path("secret-input");
      Path outDir = new Path("secret-output");

      JobConf jConf = mrCluster.createJobConf();
      // add some usr sensitive data in the job conf
      jConf.set("user-secret-code", "abracadabra");
      
      jConf.setJobName("top-secret");
      // construct a job with 1 map and 1 reduce task.
      Job job = MapReduceTestUtil.createJob(jConf, inDir, outDir, 2, 2);
      // wait for the job to complete
      job.waitForCompletion(false);

      assertTrue("Job failed", job.isSuccessful());

      JobID id = job.getJobID();
      Cluster cluster = new Cluster(jConf);
      String user = cluster.getAllJobStatuses()[0].getUsername();

      // get the jobhistory filepath
      Path jhPath = 
        new Path(mrCluster.getJobTrackerRunner().getJobTracker()
                          .getJobHistoryDir());
      Path inputLogPath = JobHistory.getJobHistoryFile(jhPath, id, user);
      Path inputConfPath = JobHistory.getConfFile(jhPath, id);
      // wait for 10 secs for the jobhistory file to move into the done folder
      FileSystem fs = inputLogPath.getFileSystem(jConf);
      for (int i = 0; i < 100; ++i) {
        if (fs.exists(inputLogPath)) {
          break;
        }
        TimeUnit.MILLISECONDS.wait(100);
      }

      assertTrue("Missing job history file", fs.exists(inputLogPath));

      // run trace builder on the job history logs
      Path goldTraceFilename = new Path(tempDir, "trace.json");
      Path goldTopologyFilename = new Path(tempDir, "topology.json");

      // build the trace-builder command line args
      String[] args = new String[] {goldTraceFilename.toString(), 
                                    goldTopologyFilename.toString(),
                                    inputLogPath.toString(),
                                    inputConfPath.toString()};
      Tool analyzer = new TraceBuilder();
      int result = ToolRunner.run(analyzer, args);
      assertEquals("Non-zero exit", 0, result);

      // anonymize the job trace
      Path anonymizedTraceFilename = new Path(tempDir, "trace-anonymized.json");
      Path anonymizedClusterTopologyFilename = 
        new Path(tempDir, "topology-anonymized.json");
      args = new String[] {"-trace", goldTraceFilename.toString(), 
                           anonymizedTraceFilename.toString(),
                           "-topology", goldTopologyFilename.toString(), 
                           anonymizedClusterTopologyFilename.toString()};
      Tool anonymizer = new Anonymizer();
      result = ToolRunner.run(anonymizer, args);
      assertEquals("Non-zero exit", 0, result);

      JobTraceReader reader = new JobTraceReader(anonymizedTraceFilename, conf);
      LoggedJob anonymizedJob = reader.getNext();
      reader.close(); // close the reader as we need only 1 job
      // test
      //   user-name
      String currentUser = UserGroupInformation.getCurrentUser().getUserName();
      assertFalse("Username not anonymized!", 
                  currentUser.equals(anonymizedJob.getUser().getValue()));
      //   jobid
      assertEquals("JobID mismatch!", 
                   id.toString(), anonymizedJob.getJobID().toString());
      //   queue-name
      assertFalse("Queuename mismatch!", 
                  "default".equals(anonymizedJob.getQueue().getValue()));
      //   job-name
      assertFalse("Jobname mismatch!", 
                  "top-secret".equals(anonymizedJob.getJobName().getValue()));
      
      //   job properties
      for (Map.Entry<Object, Object> entry : 
           anonymizedJob.getJobProperties().getValue().entrySet()) {
        assertFalse("User sensitive configuration key not anonymized", 
                    entry.getKey().toString().equals("user-secret-code"));
        assertFalse("User sensitive data not anonymized", 
                    entry.getValue().toString().contains(currentUser));
        assertFalse("User sensitive data not anonymized", 
                    entry.getValue().toString().contains("secret"));
      }
      
      // test map tasks
      testTasks(anonymizedJob.getMapTasks(), id, TaskType.MAP);
      
      // test reduce tasks
      testTasks(anonymizedJob.getReduceTasks(), id, TaskType.REDUCE);
      
      // test other tasks
      testTasks(anonymizedJob.getOtherTasks(), id, null);

      // test the anonymized cluster topology file
      ClusterTopologyReader cReader = 
        new ClusterTopologyReader(anonymizedClusterTopologyFilename, conf);
      LoggedNetworkTopology loggedNetworkTopology = cReader.get();
      // test the cluster topology
      testClusterTopology(loggedNetworkTopology, 0, "myorg");
    } finally {
      // shutdown and cleanup
      if (mrCluster != null) {
        mrCluster.shutdown();
      }
      
      if (dfsCluster != null) {
        dfsCluster.formatDataNodeDirs();
        dfsCluster.shutdown();
      }
      lfs.delete(tempDir, true);
    }
  }
  
  // test task level details lije
  //   - taskid
  //   - locality info
  //   - attempt details
  //     - attempt execution hostname
  private static void testTasks(List<LoggedTask> tasks, JobID id, 
                                TaskType type) {
    int index = 0;
    for (LoggedTask task : tasks) {
      // generate the expected task id for this task
      if (type != null) {
        TaskID tid = new TaskID(id, type, index++);
        assertEquals("TaskID mismatch!", 
                     tid.toString(), task.getTaskID().toString());
      }

      // check locality information
      if (task.getPreferredLocations() != null) {
        for (LoggedLocation loc : task.getPreferredLocations()) {
          for (NodeName name : loc.getLayers()) {
            assertFalse("Hostname mismatch!", 
                        name.getValue().contains("myorg"));
          }
        }
      }
      
      // check execution host
      for (LoggedTaskAttempt attempt : task.getAttempts()) {
        // generate the expected task id for this task
        TaskAttemptID aid = new TaskAttemptID(task.getTaskID(), 0);
        assertEquals("TaskAttemptID mismatch!", 
                     aid.toString(), attempt.getAttemptID().toString());

        assertNotNull("Hostname null!", attempt.getHostName());
        assertFalse("Hostname mismatch!", 
                    attempt.getHostName().getValue().contains("myorg"));
      }
    }
  }
  
  // tests the logged network topology
  private static void testClusterTopology(LoggedNetworkTopology topology, 
                                          int level, String bannedString) {
    assertFalse("Cluster topology test failed!", 
                topology.getName().getValue().contains(bannedString));
    if (level == 0) {
      assertEquals("Level-1 data mismatch!", 
                   "<root>", topology.getName().getValue());
    } else if (level == 1) {
      assertTrue("Level-2 data mismatch!", 
                 topology.getName().getValue().contains("rack"));
      assertFalse("Level-2 data mismatch!", 
                 topology.getName().getValue().contains("host"));
    } else {
      assertTrue("Level-2 data mismatch!", 
                 topology.getName().getValue().contains("host"));
      assertFalse("Level-2 data mismatch!", 
                  topology.getName().getValue().contains("rack"));
    }
    
    // if the current node is a rack, then test the nodes under it
    if (topology.getChildren() != null) {
      for (LoggedNetworkTopology child : topology.getChildren()) {
        testClusterTopology(child, level + 1, bannedString);
      }
    }
  }
  
  @Test
  public void testCLI() throws Exception {
    Configuration conf = new Configuration();
    FileSystem lfs = FileSystem.getLocal(conf);
    Path rootTempDir =
      new Path(System.getProperty("test.build.data", "/tmp"));

    Path tempDir = new Path(rootTempDir, "testCLI");
    tempDir = lfs.makeQualified(tempDir);
    lfs.delete(tempDir, true);
    
    // test no args
    String[] args = new String[] {};
    testAnonymizerCLI(args, -1);
    
    // test with wrong args
    args = new String[] {"-test"};
    testAnonymizerCLI(args, -1);
    
    args = new String[] {"-trace"};
    testAnonymizerCLI(args, -1);
    
    args = new String[] {"-topology"};
    testAnonymizerCLI(args, -1);
    
    args = new String[] {"-trace -topology"};
    testAnonymizerCLI(args, -1);
    
    Path testTraceInputFilename = new Path(tempDir, "trace-in.json");
    args = new String[] {"-trace", testTraceInputFilename.toString()};
    testAnonymizerCLI(args, -1);
    
    Path testTraceOutputFilename = new Path(tempDir, "trace-out.json");
    args = new String[] {"-trace", testTraceInputFilename.toString(), 
                         testTraceOutputFilename.toString()};
    testAnonymizerCLI(args, -1);
    
    OutputStream out = lfs.create(testTraceInputFilename);
    out.write("{\n}".getBytes());
    out.close();
    args = new String[] {"-trace", testTraceInputFilename.toString(), 
                         testTraceOutputFilename.toString()};
    testAnonymizerCLI(args, 0);
    
    Path testToplogyInputFilename = new Path(tempDir, "topology-in.json");
    args = new String[] {"-topology", testToplogyInputFilename.toString()};
    testAnonymizerCLI(args, -1);
    
    Path testTopologyOutputFilename = new Path(tempDir, "topology-out.json");
    args = new String[] {"-topology", testToplogyInputFilename.toString(), 
                         testTopologyOutputFilename.toString()};
    testAnonymizerCLI(args, -1);
    
    out = lfs.create(testToplogyInputFilename);
    out.write("{\n}".getBytes());
    out.close();
    args = new String[] {"-topology", testToplogyInputFilename.toString(), 
                         testTopologyOutputFilename.toString()};
    testAnonymizerCLI(args, 0);
    
    args = new String[] {"-trace", testTraceInputFilename.toString(), 
                         "-topology", testToplogyInputFilename.toString()};
    testAnonymizerCLI(args, -1);

    args = new String[] {"-trace", testTraceInputFilename.toString(), 
                         testTraceOutputFilename.toString(),
                         "-topology", testToplogyInputFilename.toString(), 
                         testTopologyOutputFilename.toString()};
    testAnonymizerCLI(args, 0);
  }
  
  // tests the Anonymizer CLI via the Tools interface
  private static void testAnonymizerCLI(String[] args, int eExitCode) 
  throws Exception {
    Anonymizer anonymizer = new Anonymizer();
    
    int exitCode = ToolRunner.run(anonymizer, args);
    assertEquals("Exit code mismatch", eExitCode, exitCode);
  }
  
  /**
   * Test {@link StatePool}'s reload and persistence feature.
   */
  @Test
  public void testStatePool() throws Exception {
    Configuration conf = new Configuration();
    FileSystem lfs = FileSystem.getLocal(conf);
    Path rootTempDir =
      new Path(System.getProperty("test.build.data", "/tmp"));

    Path tempDir = new Path(rootTempDir, "testStatePool");
    tempDir = lfs.makeQualified(tempDir);
    lfs.delete(tempDir, true);
    
    // set the state dir
    conf.set(StatePool.DIR_CONFIG, tempDir.toString());
    
    StatePool pool = new StatePool();
    // test reload, persist and dir config
    //   test with no reload and persist
    pool.initialize(conf);
    
    //  test with reload and/or persist enabled with no dir
    assertNull("Default state pool error", 
               pool.getState(MyState.class));
    
    // try persisting 
    pool.persist();
    assertFalse("State pool persisted when disabled", lfs.exists(tempDir));
    
    // test wrongly configured state-pool
    conf.setBoolean(StatePool.RELOAD_CONFIG, true);
    conf.unset(StatePool.DIR_CONFIG);
    pool = new StatePool();
    boolean success = true;
    try {
      pool.initialize(conf);
    } catch (Exception e) {
      success = false;
    }
    assertFalse("State pool bad configuration succeeded", success);
    
    // test wrongly configured state-pool
    conf.setBoolean(StatePool.RELOAD_CONFIG, false);
    conf.setBoolean(StatePool.PERSIST_CONFIG, true);
    pool = new StatePool();
    success = true;
    try {
      pool.initialize(conf);
    } catch (Exception e) {
      success = false;
    }
    assertFalse("State manager bad configuration succeeded", success);
    
    
    // test persistence
    conf.setBoolean(StatePool.RELOAD_CONFIG, false);
    conf.set(StatePool.DIR_CONFIG, tempDir.toString());
    conf.setBoolean(StatePool.PERSIST_CONFIG, true);
    pool = new StatePool();
    pool.initialize(conf);
    
    // add states to the state pool
    MyState myState = new MyState();
    pool.addState(MyState.class, myState);
    myState.setState("test-1");
    // try persisting 
    pool.persist();
    assertTrue("State pool persisted when enabled", lfs.exists(tempDir));
    assertEquals("State pool persisted when enabled", 
                 1, lfs.listStatus(tempDir).length);
    
    // reload
    conf.setBoolean(StatePool.RELOAD_CONFIG, true);
    conf.set(StatePool.DIR_CONFIG, tempDir.toString());
    conf.setBoolean(StatePool.PERSIST_CONFIG, true);
    pool = new StatePool();
    pool.initialize(conf);
    MyState pState = 
      (MyState) pool.getState(MyState.class);
    assertEquals("State pool persistence/reload failed", "test-1", 
                 pState.getState());
    
    // try persisting with no state change
    pool.persist();
    assertEquals("State pool persisted when disabled", 
                 1, lfs.listStatus(tempDir).length);
    
    // modify the state of the pool and check persistence
    pState.setUpdated(true);
    pool.persist();
    assertEquals("State pool persisted when disabled", 
                 2, lfs.listStatus(tempDir).length);
    
    // delete the temp directory if everything goes fine
    lfs.delete(tempDir, true);
  }
  
  /**
   * Test state.
   */
  static class MyState implements State {
    private boolean updated = false;
    private String state = "null";
    
    @Override
    @JsonIgnore
    public String getName() {
      return "test";
    }
    
    @Override
    public void setName(String name) {
      // for now, simply assert since this class has a hardcoded name
      if (!getName().equals(name)) {
        throw new RuntimeException("State name mismatch! Expected '" 
                                   + getName() + "' but found '" + name + "'.");
      }
    }
    
    public void setState(String state) {
      this.state = state;
    }
    
    public String getState() {
      return state;
    }
    
    void setUpdated(boolean up) {
      this.updated = up;
    }
    
    @Override
    @JsonIgnore
    public boolean isUpdated() {
      return updated;
    }
  }
  
  @SuppressWarnings("unchecked")
  private static String getValueFromDataType(Object object) {
    DataType<String> dt = (DataType<String>) object;
    return dt.getValue();
  }
  
  @Test
  public void testJobPropertiesParser() {
    // test default parser
    Properties properties = new Properties();
    Configuration conf = new Configuration();
    JobProperties jp = new JobProperties(properties);
    assertEquals("Job Properties (default filter) store error", 
                 0, jp.getAnonymizedValue(null, conf).size());
    
    // define key-value pairs for job configuration
    String key1 = "test-key";
    String value1 = "test-value";
    properties.put(key1, value1); // user config
    String key2 = MRJobConfig.USER_NAME;
    String value2 = "bob";
    properties.put(key2, value2); // job config
    String key3 = JobConf.MAPRED_MAP_TASK_JAVA_OPTS;
    String value3 = "-Xmx1G";
    properties.put(key3, value3); // deprecated
    String key4 = MRJobConfig.REDUCE_JAVA_OPTS;
    String value4 = "-Xms100m";
    properties.put(key4, value4);
    
    jp = new JobProperties(properties);
    
    // Configure the default parser
    conf.set(JobProperties.PARSERS_CONFIG_KEY, 
             DefaultJobPropertiesParser.class.getName());
    // anonymize
    Properties defaultProp = jp.getAnonymizedValue(null, conf);
    assertEquals("Job Properties (all-pass filter) store error", 
                 4, defaultProp.size());
    assertEquals("Job Properties (default filter) key#1 error", value1, 
                 getValueFromDataType(defaultProp.get(key1)));
    assertEquals("Job Properties (default filter) key#2 error", value2, 
                 getValueFromDataType(defaultProp.get(key2)));
    assertEquals("Job Properties (default filter) key#3 error", value3, 
                 getValueFromDataType(defaultProp.get(key3)));
    assertEquals("Job Properties (default filter) key#4 error", value4, 
                 getValueFromDataType(defaultProp.get(key4)));
    
    // test MR parser
    conf.set(JobProperties.PARSERS_CONFIG_KEY, 
             MapReduceJobPropertiesParser.class.getName());
    // anonymize
    Properties filteredProp = jp.getAnonymizedValue(null, conf);
    assertEquals("Job Properties (MR filter) store error", 
                 3, filteredProp.size());
    assertNull("Job Properties (MR filter) key#1 error", 
               filteredProp.get(key1));
    assertEquals("Job Properties (MR filter) key#2 error", value2, 
                 getValueFromDataType(filteredProp.get(key2)));
    assertEquals("Job Properties (MR filter) key#3 error", value3, 
                 getValueFromDataType(filteredProp.get(key3)));
    assertEquals("Job Properties (MR filter) key#4 error", value4, 
                 getValueFromDataType(filteredProp.get(key4)));
  }
  
  /**
   * Test {@link WordListAnonymizerUtility}. Test various features like
   *   - test known words
   *   - test known suffix
   */
  @Test
  public void testWordListBasedAnonymizer() {
    String[] knownSuffixes = new String[] {".1", ".2", ".3", ".4"};
    
    // test with valid suffix
    assertTrue("suffix test#0 failed!", 
               WordListAnonymizerUtility.hasSuffix("a.1", knownSuffixes));
    String split[] = 
      WordListAnonymizerUtility.extractSuffix("a.1", knownSuffixes);
    assertEquals("suffix test#1 failed!", 2, split.length);
    assertEquals("suffix test#2 failed!", "a", split[0]);
    assertEquals("suffix test#3 failed!", ".1", split[1]);
    
    // test with valid suffix
    assertTrue("suffix test#0 failed!",
               WordListAnonymizerUtility.hasSuffix("a.1", knownSuffixes));
    split = 
      WordListAnonymizerUtility.extractSuffix("/a/b.2", knownSuffixes);
    assertEquals("suffix test#0 failed!", 2, split.length);
    assertEquals("suffix test#1 failed!", "/a/b", split[0]);
    assertEquals("suffix test#2 failed!", ".2", split[1]);
    
    // test with invalid suffix
    assertFalse("suffix test#0 failed!", 
                WordListAnonymizerUtility.hasSuffix("a.b", knownSuffixes));
    
    boolean failed = false;
    try {
      split = WordListAnonymizerUtility.extractSuffix("a.b", knownSuffixes);
    } catch (Exception e) {
      failed = true;
    }
    assertTrue("Exception expected!", failed);
    
    String[] knownWords = new String[] {"a", "b"};
    
    // test with valid data
    assertTrue("data test#0 failed!", 
               WordListAnonymizerUtility.isKnownData("a", knownWords));
    // test with valid data
    assertTrue("data test#1 failed!", 
               WordListAnonymizerUtility.isKnownData("b", knownWords));
    // test with invalid data
    assertFalse("data test#2 failed!", 
                WordListAnonymizerUtility.isKnownData("c", knownWords));
    
    // test with valid known word
    assertTrue("data test#3 failed!", 
               WordListAnonymizerUtility.isKnownData("job"));
    // test with invalid known word
    assertFalse("data test#4 failed!", 
                WordListAnonymizerUtility.isKnownData("bob"));
    
    // test numeric data
    assertFalse("Numeric test failed!", 
                 WordListAnonymizerUtility.needsAnonymization("123"));
    // test numeric data (unsupported)
    assertTrue("Numeric test failed!", 
               WordListAnonymizerUtility.needsAnonymization("123.456"));
    // test text data
    assertTrue("Text test failed!", 
               WordListAnonymizerUtility.needsAnonymization("123abc"));
  }
  
  /**
   * Test {@link WordList} features like
   *   - add words
   *   - index 
   *   - contains
   */
  @Test
  public void testWordList() throws Exception {
    // test features with fresh state
    WordList wordList = new WordList();
    assertFalse("Word list state incorrect", wordList.isUpdated());
    
    // add some special word
    String test = "abbracadabra";
    wordList.add(test);
    assertTrue("Word list failed to store", wordList.contains(test));
    assertEquals("Word list index failed", 0, wordList.indexOf(test));
    assertEquals("Word list size failed", 1, wordList.getSize());
    assertTrue("Word list state incorrect", wordList.isUpdated());
    
    // add already added word
    wordList.add(test);
    assertEquals("Word list index failed", 0, wordList.indexOf(test));
    assertEquals("Word list size failed", 1, wordList.getSize());
    assertTrue("Word list state incorrect", wordList.isUpdated());
    
    String test2 = "hakuna-matata";
    wordList.add(test2);
    assertTrue("Word list failed to store", wordList.contains(test2));
    assertEquals("Word list index failed", 1, wordList.indexOf(test2));
    assertEquals("Word list size failed", 2, wordList.getSize());
    assertTrue("Word list state incorrect", wordList.isUpdated());

    // test persistence
    Configuration conf = new Configuration();
    FileSystem lfs = FileSystem.getLocal(conf);
    Path rootTempDir =
      new Path(System.getProperty("test.build.data", "/tmp"));

    Path tempDir = new Path(rootTempDir, "testWordList");
    tempDir = lfs.makeQualified(tempDir);
    lfs.delete(tempDir, true);
    
    // define a state pool to help persist the wordlist
    StatePool pool = new StatePool();
    
    try {
      // set the persistence directory
      conf.set(StatePool.DIR_CONFIG, tempDir.toString());
      conf.setBoolean(StatePool.PERSIST_CONFIG, true);

      // initialize the state-pool
      pool.initialize(conf);

      // add the wordlist to the pool
      pool.addState(getClass(), wordList);

      pool.persist();

      // now clear the pool state
      pool = new StatePool();
      
      // set reload to true
      conf.setBoolean(StatePool.RELOAD_CONFIG, true);
      // initialize the state-pool
      pool.initialize(conf);

      State state = pool.getState(getClass());
      assertNotNull("Missing state!", state);
      assertEquals("Incorrect state class!", WordList.class, state.getClass());
      WordList pList = (WordList) state;

      // check size
      assertEquals("Word list size on reload failed", 2, pList.getSize());
      assertFalse("Word list state incorrect", pList.isUpdated());

      // add already added word
      pList.add(test);
      assertEquals("Word list index on reload failed", 0, pList.indexOf(test));
      assertEquals("Word list size on reload failed", 2, pList.getSize());
      assertFalse("Word list state on reload incorrect", pList.isUpdated());

      String test3 = "disco-dancer";
      assertFalse("Word list failed to after reload", pList.contains(test3));
      pList.add(test3);
      assertTrue("Word list failed to store on reload", pList.contains(test3));
      assertEquals("Word list index on reload failed", 2, pList.indexOf(test3));
      assertEquals("Word list size on reload failed", 3, pList.getSize());
      assertTrue("Word list state on reload incorrect", pList.isUpdated());
      
      // test previously added (persisted) word
      assertTrue("Word list failed to store on reload", pList.contains(test2));
      assertEquals("Word list index on reload failed", 1, pList.indexOf(test2));
    } finally {
      lfs.delete(tempDir, true);
    }
  }
  
  /**
   * Test {@link FileName#FileNameState} persistence with directories only.
   */
  @Test
  public void testFileNameStateWithDir() throws Exception {
    // test persistence
    Configuration conf = new Configuration();
    FileSystem lfs = FileSystem.getLocal(conf);
    Path rootTempDir =
      new Path(System.getProperty("test.build.data", "/tmp"));

    Path tempDir = new Path(rootTempDir, "testFileNameStateWithDir");
    tempDir = lfs.makeQualified(tempDir);
    lfs.delete(tempDir, true);
    
    // set the persistence directory
    conf.set(StatePool.DIR_CONFIG, tempDir.toString());
    conf.setBoolean(StatePool.PERSIST_CONFIG, true);
    
    // define a state pool to help persist the dirs
    StatePool pool = new StatePool();
    
    FileNameState fState = new FileNameState();
    
    // define the directory names
    String test1 = "test";
    String test2 = "home";
    
    // test dir only
    WordList dirState = new WordList("dir");
    dirState.add(test1);
    dirState.add(test2);
    
    // set the directory state
    fState.setDirectoryState(dirState);
    
    try {
      // initialize the state-pool
      pool.initialize(conf);

      // add the wordlist to the pool
      pool.addState(getClass(), fState);

      // persist the state
      pool.persist();

      // now clear the pool state
      pool = new StatePool();
      
      // set reload to true
      conf.setBoolean(StatePool.RELOAD_CONFIG, true);
      
      // initialize the state-pool
      pool.initialize(conf);

      State state = pool.getState(getClass());
      assertNotNull("Missing state!", state);
      assertEquals("Incorrect state class!", 
                    FileNameState.class, state.getClass());
      FileNameState newFState = (FileNameState) state;

      // check the state contents
      WordList newStateWordList = newFState.getDirectoryState();
      assertTrue("File state failed to store on reload", 
                 newStateWordList.contains(test1));
      assertEquals("File state index on reload failed", 
                   0, newStateWordList.indexOf(test1));
      
      assertTrue("File state failed to store on reload", 
                 newStateWordList.contains(test2));
      assertEquals("File state index on reload failed", 
                   1, newStateWordList.indexOf(test2));
    } finally {
      lfs.delete(tempDir, true);
    }
  }
  
  /**
   * Test {@link FileName#FileNameState} persistence with files only.
   */
  @Test
  public void testFileNameStateWithFiles() throws Exception {
    // test persistence
    Configuration conf = new Configuration();
    FileSystem lfs = FileSystem.getLocal(conf);
    Path rootTempDir =
      new Path(System.getProperty("test.build.data", "/tmp"));

    Path tempDir = new Path(rootTempDir, "testFileNameStateWithFiles");
    tempDir = lfs.makeQualified(tempDir);
    lfs.delete(tempDir, true);
    
    // set the persistence directory
    conf.set(StatePool.DIR_CONFIG, tempDir.toString());
    conf.setBoolean(StatePool.PERSIST_CONFIG, true);
    
    // define a state pool to help persist the filename parts
    StatePool pool = new StatePool();
    
    FileNameState fState = new FileNameState();
    
    // define the file names
    String test1 = "part-00.bzip";
    String test2 = "file1.txt";
    
    // test filenames only
    WordList fileNameState = new WordList("files");
    fileNameState.add(test1);
    fileNameState.add(test2);
    
    // set the filename state
    fState.setDirectoryState(fileNameState);
    
    try {
      // initialize the state-pool
      pool.initialize(conf);

      // add the wordlist to the pool
      pool.addState(getClass(), fState);

      // persist the state
      pool.persist();

      // now clear the pool state
      pool = new StatePool();
      
      // set reload to true
      conf.setBoolean(StatePool.RELOAD_CONFIG, true);
      
      // initialize the state-pool
      pool.initialize(conf);

      State state = pool.getState(getClass());
      assertNotNull("Missing state!", state);
      assertEquals("Incorrect state class!", 
                    FileNameState.class, state.getClass());
      FileNameState newFState = (FileNameState) state;

      // check the state contents
      WordList newFileWordList = newFState.getDirectoryState();
      assertTrue("File state failed on reload", 
                 newFileWordList.contains(test1));
      assertEquals("File state indexing on reload failed", 
                   0, newFileWordList.indexOf(test1));
      
      assertTrue("File state failed on reload", 
                 newFileWordList.contains(test2));
      assertEquals("File state indexing on reload failed", 
                   1, newFileWordList.indexOf(test2));
    } finally {
      lfs.delete(tempDir, true);
    }
  }
  
  /**
   * Test {@link FileName#FileNameState} persistence with files and directories.
   */
  @Test
  public void testFileNameState() throws Exception {
    // test persistence
    Configuration conf = new Configuration();
    FileSystem lfs = FileSystem.getLocal(conf);
    Path rootTempDir =
      new Path(System.getProperty("test.build.data", "/tmp"));

    Path tempDir = new Path(rootTempDir, "testFileNameState");
    tempDir = lfs.makeQualified(tempDir);
    lfs.delete(tempDir, true);
    
    // set the persistence directory
    conf.set(StatePool.DIR_CONFIG, tempDir.toString());
    conf.setBoolean(StatePool.PERSIST_CONFIG, true);
    
    // define a state pool to help persist the filename parts
    StatePool pool = new StatePool();
    
    FileNameState fState = new FileNameState();
    
    // define the directory names
    String testD1 = "test";
    String testD2 = "home";
    String testD3 = "tmp";
    
    // test dir only
    WordList dirState = new WordList("dir");
    dirState.add(testD1);
    dirState.add(testD2);
    dirState.add(testD3);
    
    // define the file names
    String testF1 = "part-00.bzip";
    String testF2 = "file1.txt";
    String testF3 = "tmp";
    
    // test filenames only
    WordList fileNameState = new WordList("files");
    fileNameState.add(testF1);
    fileNameState.add(testF2);
    fileNameState.add(testF3);
    
    // set the filename state
    fState.setFileNameState(fileNameState);
    // set the directory state
    fState.setDirectoryState(dirState);
    
    try {
      // initialize the state-pool
      pool.initialize(conf);

      // add the wordlist to the pool
      pool.addState(getClass(), fState);

      // persist the state
      pool.persist();

      // now clear the pool state
      pool = new StatePool();
      
      // set reload to true
      conf.setBoolean(StatePool.RELOAD_CONFIG, true);
      
      // initialize the state-pool
      pool.initialize(conf);

      State state = pool.getState(getClass());
      assertNotNull("Missing state!", state);
      assertEquals("Incorrect state class!", 
                    FileNameState.class, state.getClass());
      FileNameState newFState = (FileNameState) state;

      // test filenames
      WordList newStateWordList = newFState.getFileNameState();
      assertTrue("File state failed on reload", 
                 newStateWordList.contains(testF1));
      assertEquals("File state indexing on reload failed", 
                   0, newStateWordList.indexOf(testF1));
      
      assertTrue("File state failed on reload", 
                 newStateWordList.contains(testF2));
      assertEquals("File state indexing on reload failed", 
                   1, newStateWordList.indexOf(testF2));
      
      assertTrue("File state failed on reload", 
                 newStateWordList.contains(testF3));
      assertEquals("File state indexing on reload failed", 
                   2, newStateWordList.indexOf(testF3));
      
      // test dirs
      WordList newDirWordList = newFState.getDirectoryState();
      assertTrue("File state failed on reload", 
                 newDirWordList.contains(testD1));
      assertEquals("File state indexing on reload failed", 
                   0, newDirWordList.indexOf(testD1));
      
      assertTrue("File state failed on reload", 
                 newDirWordList.contains(testD2));
      assertEquals("File state indexing on reload failed", 
                   1, newDirWordList.indexOf(testD2));
      assertTrue("File state failed on reload", 
                 newDirWordList.contains(testD3));
      assertEquals("File state indexing on reload failed", 
                   2, newDirWordList.indexOf(testD3));
    } finally {
      lfs.delete(tempDir, true);
    }
  }
  
  /**
   * Test {@link NodeName#NodeName} persistence with hostnames only.
   */
  @Test
  public void testNodeNameStateWithHostNameOnly() throws Exception {
    // test persistence
    Configuration conf = new Configuration();
    FileSystem lfs = FileSystem.getLocal(conf);
    Path rootTempDir =
      new Path(System.getProperty("test.build.data", "/tmp"));

    Path tempDir = new Path(rootTempDir, "testNodeNameStateWithHostNameOnly");
    tempDir = lfs.makeQualified(tempDir);
    lfs.delete(tempDir, true);
    
    // set the persistence directory
    conf.set(StatePool.DIR_CONFIG, tempDir.toString());
    conf.setBoolean(StatePool.PERSIST_CONFIG, true);
    
    // define a state pool to help persist the hostnames
    StatePool pool = new StatePool();
    
    NodeNameState nState = new NodeNameState();
    
    // define the host names
    String test1 = "abc123";
    String test2 = "xyz789";
    
    // test hostname only
    WordList hostNameState = new WordList("hostname");
    hostNameState.add(test1);
    hostNameState.add(test2);
    
    // set the directory state
    nState.setHostNameState(hostNameState);
    
    try {
      // initialize the state-pool
      pool.initialize(conf);

      // add the wordlist to the pool
      pool.addState(getClass(), nState);

      // persist the state
      pool.persist();

      // now clear the pool state
      pool = new StatePool();
      
      // set reload to true
      conf.setBoolean(StatePool.RELOAD_CONFIG, true);
      
      // initialize the state-pool
      pool.initialize(conf);

      State state = pool.getState(getClass());
      assertNotNull("Missing state!", state);
      assertEquals("Incorrect state class!", 
                   NodeNameState.class, state.getClass());
      NodeNameState newNState = (NodeNameState) state;

      // check the state contents
      WordList newStateWordList = newNState.getHostNameState();
      assertTrue("Node state failed to store on reload", 
                 newStateWordList.contains(test1));
      assertEquals("Node state index on reload failed", 
                   0, newStateWordList.indexOf(test1));
      
      assertTrue("Node state failed to store on reload", 
                 newStateWordList.contains(test2));
      assertEquals("Node state index on reload failed", 
                   1, newStateWordList.indexOf(test2));
    } finally {
      lfs.delete(tempDir, true);
    }
  }
  
  /**
   * Test {@link NodeName#NodeNameState} persistence with racknames only.
   */
  @Test
  public void testNodeNameWithRackNamesOnly() throws Exception {
    // test persistence
    Configuration conf = new Configuration();
    FileSystem lfs = FileSystem.getLocal(conf);
    Path rootTempDir =
      new Path(System.getProperty("test.build.data", "/tmp"));

    Path tempDir = new Path(rootTempDir, "testNodeNameWithRackNamesOnly");
    tempDir = lfs.makeQualified(tempDir);
    lfs.delete(tempDir, true);
    
    // set the persistence directory
    conf.set(StatePool.DIR_CONFIG, tempDir.toString());
    conf.setBoolean(StatePool.PERSIST_CONFIG, true);
    
    // define a state pool to help persist the rack names
    StatePool pool = new StatePool();
    
    NodeNameState nState = new NodeNameState();
    
    // define the rack names
    String test1 = "rack1";
    String test2 = "rack2";
    
    // test filenames only
    WordList rackNameState = new WordList("racknames");
    rackNameState.add(test1);
    rackNameState.add(test2);
    
    // set the rackname state
    nState.setRackNameState(rackNameState);
    
    try {
      // initialize the state-pool
      pool.initialize(conf);

      // add the wordlist to the pool
      pool.addState(getClass(), nState);

      // persist the state
      pool.persist();

      // now clear the pool state
      pool = new StatePool();
      
      // set reload to true
      conf.setBoolean(StatePool.RELOAD_CONFIG, true);
      
      // initialize the state-pool
      pool.initialize(conf);

      State state = pool.getState(getClass());
      assertNotNull("Missing state!", state);
      assertEquals("Incorrect state class!", 
                   NodeNameState.class, state.getClass());
      NodeNameState newNState = (NodeNameState) state;

      // check the state contents
      WordList newFileWordList = newNState.getRackNameState();
      assertTrue("File state failed on reload", 
                 newFileWordList.contains(test1));
      assertEquals("File state indexing on reload failed", 
                   0, newFileWordList.indexOf(test1));
      
      assertTrue("File state failed on reload", 
                 newFileWordList.contains(test2));
      assertEquals("File state indexing on reload failed", 
                   1, newFileWordList.indexOf(test2));
    } finally {
      lfs.delete(tempDir, true);
    }
  }
  
  /**
   * Test {@link NodeName#NodeNameState} persistence with hosts and racks.
   */
  @Test
  public void testNodeNameState() throws Exception {
    // test persistence
    Configuration conf = new Configuration();
    FileSystem lfs = FileSystem.getLocal(conf);
    Path rootTempDir =
      new Path(System.getProperty("test.build.data", "/tmp"));

    Path tempDir = new Path(rootTempDir, "testNodeNameState");
    tempDir = lfs.makeQualified(tempDir);
    lfs.delete(tempDir, true);
    
    // set the persistence directory
    conf.set(StatePool.DIR_CONFIG, tempDir.toString());
    conf.setBoolean(StatePool.PERSIST_CONFIG, true);
    
    // define a state pool to help persist the node names.
    StatePool pool = new StatePool();
    
    NodeNameState nState = new NodeNameState();
    
    // define the rack names
    String testR1 = "rack1";
    String testR2 = "rack2";
    String testR3 = "rack3";
    
    WordList rackState = new WordList("rack");
    rackState.add(testR1);
    rackState.add(testR2);
    rackState.add(testR3);
    
    String testH1 = "host1";
    String testH2 = "host2";
    String testH3 = "host3";
    
    WordList hostNameState = new WordList("host");
    hostNameState.add(testH1);
    hostNameState.add(testH2);
    hostNameState.add(testH3);
    
    // set the filename state
    nState.setHostNameState(hostNameState);
    nState.setRackNameState(rackState);
    
    try {
      // initialize the state-pool
      pool.initialize(conf);

      // add the wordlist to the pool
      pool.addState(getClass(), nState);

      // persist the state
      pool.persist();

      // now clear the pool state
      pool = new StatePool();
      
      // set reload to true
      conf.setBoolean(StatePool.RELOAD_CONFIG, true);
      
      // initialize the state-pool
      pool.initialize(conf);

      State state = pool.getState(getClass());
      assertNotNull("Missing state!", state);
      assertEquals("Incorrect state class!", 
                   NodeNameState.class, state.getClass());
      NodeNameState newNState = (NodeNameState) state;

      // test nodenames
      WordList newHostWordList = newNState.getHostNameState();
      assertTrue("File state failed on reload", 
                 newHostWordList.contains(testH1));
      assertEquals("File state indexing on reload failed", 
                   0, newHostWordList.indexOf(testH1));
      
      assertTrue("File state failed on reload", 
                 newHostWordList.contains(testH2));
      assertEquals("File state indexing on reload failed", 
                   1, newHostWordList.indexOf(testH2));
      
      assertTrue("File state failed on reload", 
                 newHostWordList.contains(testH3));
      assertEquals("File state indexing on reload failed", 
                   2, newHostWordList.indexOf(testH3));
      
      // test racknames
      WordList newRackWordList = newNState.getRackNameState();
      assertTrue("File state failed on reload", 
                 newRackWordList.contains(testR1));
      assertEquals("File state indexing on reload failed", 
                   0, newRackWordList.indexOf(testR1));
      
      assertTrue("File state failed on reload", 
                 newRackWordList.contains(testR2));
      assertEquals("File state indexing on reload failed", 
                   1, newRackWordList.indexOf(testR2));
      assertTrue("File state failed on reload", 
                 newRackWordList.contains(testR3));
      assertEquals("File state indexing on reload failed", 
                   2, newRackWordList.indexOf(testR3));
    } finally {
      lfs.delete(tempDir, true);
    }
  }
}