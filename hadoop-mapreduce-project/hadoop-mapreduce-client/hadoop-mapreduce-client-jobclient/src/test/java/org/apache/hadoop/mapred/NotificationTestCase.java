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

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.MapReduceTestUtil;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpServlet;
import javax.servlet.ServletException;
import java.io.IOException;
import java.io.DataOutputStream;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.After;
import org.junit.Test;


/**
 * Base class to test Job end notification in local and cluster mode.
 *
 * Starts up hadoop on Local or Cluster mode (by extending of the
 * HadoopTestCase class) and it starts a servlet engine that hosts
 * a servlet that will receive the notification of job finalization.
 *
 * The notification servlet returns a HTTP 400 the first time is called
 * and a HTTP 200 the second time, thus testing retry.
 *
 * In both cases local file system is used (this is irrelevant for
 * the tested functionality)
 *
 * 
 */
public abstract class NotificationTestCase extends HadoopTestCase {

  protected NotificationTestCase(int mode) throws IOException {
    super(mode, HadoopTestCase.LOCAL_FS, 1, 1);
  }

  private int port;
  private String contextPath = "/notification";
  private String servletPath = "/mapred";
  private Server webServer;

  private void startHttpServer() throws Exception {

    // Create the webServer
    if (webServer != null) {
      webServer.stop();
      webServer = null;
    }
    webServer = new Server(0);

    ServletContextHandler context =
        new ServletContextHandler(webServer, contextPath);

    // create servlet handler
    context.addServlet(new ServletHolder(new NotificationServlet()),
                       servletPath);

    // Start webServer
    webServer.start();
    port = ((ServerConnector)webServer.getConnectors()[0]).getLocalPort();

  }

  private void stopHttpServer() throws Exception {
    if (webServer != null) {
      webServer.stop();
      webServer.destroy();
      webServer = null;
    }
  }

  public static class NotificationServlet extends HttpServlet {
    public static volatile int counter = 0;
    public static volatile int failureCounter = 0;
    private static final long serialVersionUID = 1L;
    
    protected void doGet(HttpServletRequest req, HttpServletResponse res)
      throws ServletException, IOException {
      String queryString = req.getQueryString();
      switch (counter) {
        case 0:
          verifyQuery(queryString, "SUCCEEDED");
          break;
        case 2:
          verifyQuery(queryString, "KILLED");
          break;
        case 4:
          verifyQuery(queryString, "FAILED");
          break;
      }
      if (counter % 2 == 0) {
        res.sendError(HttpServletResponse.SC_BAD_REQUEST, "forcing error");
      }
      else {
        res.setStatus(HttpServletResponse.SC_OK);
      }
      counter++;
    }

    protected void verifyQuery(String query, String expected) 
        throws IOException {
      if (query.contains(expected)) {
        return;
      }
      failureCounter++;
      assertTrue("The request (" + query + ") does not contain " + expected, false);
    }
  }

  private String getNotificationUrlTemplate() {
    return "http://localhost:" + port + contextPath + servletPath +
      "?jobId=$jobId&amp;jobStatus=$jobStatus";
  }

  protected JobConf createJobConf() {
    JobConf conf = super.createJobConf();
    conf.setJobEndNotificationURI(getNotificationUrlTemplate());
    conf.setInt(JobContext.MR_JOB_END_RETRY_ATTEMPTS, 3);
    conf.setInt(JobContext.MR_JOB_END_RETRY_INTERVAL, 200);
    return conf;
  }

  @Before
  public void setUp() throws Exception {
    super.setUp();
    startHttpServer();
  }

  @After
  public void tearDown() throws Exception {
    stopHttpServer();
    super.tearDown();
  }

  @Test
  public void testMR() throws Exception {

    System.out.println(launchWordCount(this.createJobConf(),
                                       "a b c d e f g h", 1, 1));
    boolean keepTrying = true;
    for (int tries = 0; tries < 30 && keepTrying; tries++) {
      Thread.sleep(50);
      keepTrying = !(NotificationServlet.counter == 2);
    }
    assertEquals(2, NotificationServlet.counter);
    assertEquals(0, NotificationServlet.failureCounter);
    
    Path inDir = new Path("notificationjob/input");
    Path outDir = new Path("notificationjob/output");

    // Hack for local FS that does not have the concept of a 'mounting point'
    if (isLocalFS()) {
      String localPathRoot = System.getProperty("test.build.data","/tmp")
        .toString().replace(' ', '+');;
      inDir = new Path(localPathRoot, inDir);
      outDir = new Path(localPathRoot, outDir);
    }

    // run a job with KILLED status
    System.out.println(UtilsForTests.runJobKill(this.createJobConf(), inDir,
                                                outDir).getID());
    keepTrying = true;
    for (int tries = 0; tries < 30 && keepTrying; tries++) {
      Thread.sleep(50);
      keepTrying = !(NotificationServlet.counter == 4);
    }
    assertEquals(4, NotificationServlet.counter);
    assertEquals(0, NotificationServlet.failureCounter);
    
    // run a job with FAILED status
    System.out.println(UtilsForTests.runJobFail(this.createJobConf(), inDir,
                                                outDir).getID());
    keepTrying = true;
    for (int tries = 0; tries < 30 && keepTrying; tries++) {
      Thread.sleep(50);
      keepTrying = !(NotificationServlet.counter == 6);
    }
    assertEquals(6, NotificationServlet.counter);
    assertEquals(0, NotificationServlet.failureCounter);
  }

  private String launchWordCount(JobConf conf,
                                 String input,
                                 int numMaps,
                                 int numReduces) throws IOException {
    Path inDir = new Path("testing/wc/input");
    Path outDir = new Path("testing/wc/output");

    // Hack for local FS that does not have the concept of a 'mounting point'
    if (isLocalFS()) {
      String localPathRoot = System.getProperty("test.build.data","/tmp")
        .toString().replace(' ', '+');;
      inDir = new Path(localPathRoot, inDir);
      outDir = new Path(localPathRoot, outDir);
    }

    FileSystem fs = FileSystem.get(conf);
    fs.delete(outDir, true);
    if (!fs.mkdirs(inDir)) {
      throw new IOException("Mkdirs failed to create " + inDir.toString());
    }
    {
      DataOutputStream file = fs.create(new Path(inDir, "part-0"));
      file.writeBytes(input);
      file.close();
    }
    conf.setJobName("wordcount");
    conf.setInputFormat(TextInputFormat.class);

    // the keys are words (strings)
    conf.setOutputKeyClass(Text.class);
    // the values are counts (ints)
    conf.setOutputValueClass(IntWritable.class);

    conf.setMapperClass(WordCount.MapClass.class);
    conf.setCombinerClass(WordCount.Reduce.class);
    conf.setReducerClass(WordCount.Reduce.class);

    FileInputFormat.setInputPaths(conf, inDir);
    FileOutputFormat.setOutputPath(conf, outDir);
    conf.setNumMapTasks(numMaps);
    conf.setNumReduceTasks(numReduces);
    JobClient.runJob(conf);
    return MapReduceTestUtil.readOutput(outDir, conf);
  }

}
