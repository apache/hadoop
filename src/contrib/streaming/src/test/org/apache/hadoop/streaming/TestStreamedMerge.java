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

package org.apache.hadoop.streaming;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.dfs.MiniDFSCluster;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.LineRecordReader.LineReader;
import org.apache.hadoop.util.ToolRunner;

/**
 * This JUnit test is not pure-Java and is not run as 
 * part of the standard ant test* targets.   
 * Two ways to run this:<pre>
 * 1. main(), a Java application.
 * 2. cd src/contrib/streaming/ 
 *    ant \
 *     [-Dfs.default.name=h:p] \ 
 *     [-Dhadoop.test.localoutputfile=/tmp/fifo] \ 
 *     test-unix 
 * </pre>
 */
public class TestStreamedMerge extends TestCase {

  public TestStreamedMerge() throws IOException {
    UtilTest utilTest = new UtilTest(getClass().getName());
    utilTest.checkUserDir();
    //  utilTest.redirectIfAntJunit();
  }

  final static int NAME_PORT = 8200;
  final static int SOC_PORT = 1888;

  void addInput(String path, String contents) throws IOException {
    OutputStream out = fs_.create(new Path(path));
    DataOutputStream dout = new DataOutputStream(out);
    dout.write(contents.getBytes("UTF-8"));
    dout.close();
    System.err.println("addInput done: " + path);
  }

  String createInputs(boolean tag) throws IOException {
    fs_.delete(new Path("/input/"));

    // i18n() replaces some ASCII with multibyte UTF-8 chars
    addInput("/input/part-00", i18n("k1\tv1\n" + "k3\tv5\n"));
    addInput("/input/part-01", i18n("k1\tv2\n" + "k2\tv4\n"));
    addInput("/input/part-02", i18n("k1\tv3\n"));
    addInput("/input/part-03", "");
    
    // tags are one-based: ">1" corresponds to part-00, etc.
    // Expected result it the merge-sort order of the records.
    // keys are compared as Strings and ties are broken by stream index
    // For example (k1; stream 2) < (k1; stream 3)
    String expect = i18n(
                         unt(">1\tk1\tv1\n", tag) + 
                         unt(">2\tk1\tv2\n", tag) + 
                         unt(">3\tk1\tv3\n", tag) + 
                         unt(">2\tk2\tv4\n", tag) +
                         unt(">1\tk3\tv5\n", tag)
                         );
    return expect;
  }
  
  String unt(String line, boolean keepTag)
  {
    return keepTag ? line : line.substring(line.indexOf('\t')+1);
  }
  String i18n(String c) {
    c = c.replace('k', '\u20ac'); // Euro sign, in UTF-8: E282AC
    c = c.replace('v', '\u00a2'); // Cent sign, in UTF-8: C2A2 ; UTF-16 contains null
    // "\ud800\udc00" // A surrogate pair, U+10000. OK also works
    return c;
  }

  void lsr() {
    try {
      System.out.println("lsr /");
      ToolRunner.run(conf_, new FsShell(), new String[]{ "-lsr", "/" });
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  void printSampleInput() {
    try {
      System.out.println("cat /input/part-00");
      String content = StreamUtil.slurpHadoop(new Path("/input/part-00"), fs_);
      System.out.println(content);
      System.out.println("cat done.");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  void callStreaming(String argSideOutput, boolean inputTagged) throws IOException {
    String[] testargs = new String[] {
      //"-jobconf", "stream.debug=1",
      "-verbose", 
      "-jobconf", "stream.testmerge=1", 
      "-input", "+/input/part-00 | /input/part-01 | /input/part-02", 
      "-mapper", StreamUtil.localizeBin("/bin/cat"), 
      "-reducer", "NONE", 
      "-output", "/my.output",
      "-mapsideoutput", argSideOutput, 
      "-dfs", conf_.get("fs.default.name"), 
      "-jt", "local",
      "-jobconf", "stream.sideoutput.localfs=true", 
      "-jobconf", "stream.tmpdir="+System.getProperty("test.build.data","/tmp")
    };
    ArrayList argList = new ArrayList();
    argList.addAll(Arrays.asList(testargs));
    if (inputTagged) {
      argList.add("-inputtagged");
    }
    testargs = (String[])argList.toArray(new String[0]);
    String sss = StreamUtil.collate(argList, " ");
    System.out.println("bin/hadoop jar build/hadoop-streaming.jar " + sss);
    //HadoopStreaming.main(testargs);
    StreamJob job = new StreamJob(testargs, false);
    job.go();
  }

  SideEffectConsumer startSideEffectConsumer(StringBuffer outBuf) {
    SideEffectConsumer t = new SideEffectConsumer(outBuf) {
        ServerSocket listen;
        Socket client;
        InputStream in;
      
        @Override
        InputStream connectInputStream() throws IOException {
          listen = new ServerSocket(SOC_PORT);
          client = listen.accept();
          in = client.getInputStream();
          return in;
        }
      
        @Override
        void close() throws IOException
        {
          listen.close();
          System.out.println("@@@listen closed");
        }
      };
    t.start();
    return t;
  }

  abstract class SideEffectConsumer extends Thread {

    SideEffectConsumer(StringBuffer buf) {
      buf_ = buf;
      setDaemon(true);
    }

    abstract InputStream connectInputStream() throws IOException;
    
    abstract void close() throws IOException;
    
    @Override
    public void run() {
      try {
        in_ = connectInputStream();
        LineReader lineReader = new LineReader((InputStream)in_, conf_);
        Text line = new Text();
        while (lineReader.readLine(line) > 0) {
          buf_.append(line.toString());
          buf_.append('\n');
          line.clear();
        }
        lineReader.close();
        in_.close();
      } catch (IOException io) {
        throw new RuntimeException(io);
      }
    }
    
    InputStream in_;
    StringBuffer buf_;
  }

  public void testMain() throws IOException {
    boolean success = false;
    String base = new File(".").getAbsolutePath();
    System.setProperty("hadoop.log.dir", base + "/logs");
    conf_ = new Configuration();
    String overrideFS = StreamUtil.getBoundAntProperty("fs.default.name", null);
    MiniDFSCluster cluster = null;
    try {
      if (overrideFS == null) {
        cluster = new MiniDFSCluster(conf_, 1, true, null);
        fs_ = cluster.getFileSystem();
      } else {
        System.out.println("overrideFS: " + overrideFS);
        FileSystem.setDefaultUri(conf_, overrideFS);
        fs_ = FileSystem.get(conf_);
      }
      doAllTestJobs();
      success = true;
    } catch (IOException io) {
      io.printStackTrace();
    } finally {
      try {
        fs_.close();
      } catch (IOException io) {
      }
      if (cluster != null) {
        cluster.shutdown();
        System.out.println("cluster.shutdown(); DONE");
      }
      System.out.println(getClass().getName() + ": success=" + success);
    }
  }

  void doAllTestJobs() throws IOException
  {
    goSocketTagged(true, false);
    goSocketTagged(false, false);
    goSocketTagged(true, true);
  }
  
  void goSocketTagged(boolean socket, boolean inputTagged) throws IOException {
    System.out.println("***** goSocketTagged: " + socket + ", " + inputTagged);
    String expect = createInputs(inputTagged);
    lsr();
    printSampleInput();

    StringBuffer outputBuf = new StringBuffer();
    String sideOutput = null;
    File f = null;
    SideEffectConsumer consumer = null;
    if (socket) {
      consumer = startSideEffectConsumer(outputBuf);
      sideOutput = "socket://localhost:" + SOC_PORT + "/";
    } else {
      String userOut = StreamUtil.getBoundAntProperty(
                                                      "hadoop.test.localoutputfile", null);
      if (userOut != null) {
        f = new File(userOut);
        // don't delete so they can mkfifo
        maybeFifoOutput_ = true;
      } else {
        f = new File("localoutputfile");
        f.delete();
        maybeFifoOutput_ = false;
      }
      String s = new Path(f.getAbsolutePath()).toString();
      if (!s.startsWith("/")) {
        s = "/" + s; // Windows "file:/C:/"
      }
      sideOutput = "file:" + s;
    }
    System.out.println("sideOutput=" + sideOutput);
    callStreaming(sideOutput, inputTagged);
    String output;
    if (socket) {
      try {
        consumer.join();
        consumer.close();
      } catch (InterruptedException e) {
        throw (IOException) new IOException().initCause(e);
      }
      output = outputBuf.toString();
    } else {
      if (maybeFifoOutput_) {
        System.out.println("assertEquals will fail.");
        output = "potential FIFO: not retrieving to avoid blocking on open() "
          + f.getAbsoluteFile();
      } else {
        output = StreamUtil.slurp(f.getAbsoluteFile());
      }
    }

    lsr();
    
    System.out.println("output=|" + output + "|");
    System.out.println("expect=|" + expect + "|");
    assertEquals(expect, output);
  }

  Configuration conf_;
  FileSystem fs_;
  boolean maybeFifoOutput_;

  public static void main(String[] args) throws Throwable {
    TestStreamedMerge test = new TestStreamedMerge();
    test.testMain();
  }
  
}
