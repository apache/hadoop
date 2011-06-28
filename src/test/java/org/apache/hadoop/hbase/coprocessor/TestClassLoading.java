/*
 * Copyright 2011 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.coprocessor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import javax.tools.*;
import java.io.*;
import java.util.*;
import java.util.jar.*;

import org.junit.*;

import static org.junit.Assert.assertTrue;

/**
 * Test coprocessors class loading.
 */
public class TestClassLoading {
  private static final Log LOG = LogFactory.getLog(TestClassLoading.class);
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static Configuration conf;
  private static MiniDFSCluster cluster;

  public static int BUFFER_SIZE = 4096;

  @Before
  public void setUp() throws Exception {
  }
  @After
  public void tearDown() throws Exception {
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(1);

    conf = TEST_UTIL.getConfiguration();
    cluster = TEST_UTIL.getDFSCluster();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  // generate jar file
  private boolean createJarArchive(File archiveFile, File[] tobeJared) {
    try {
      byte buffer[] = new byte[BUFFER_SIZE];
      // Open archive file
      FileOutputStream stream = new FileOutputStream(archiveFile);
      JarOutputStream out = new JarOutputStream(stream, new Manifest());

      for (int i = 0; i < tobeJared.length; i++) {
        if (tobeJared[i] == null || !tobeJared[i].exists()
            || tobeJared[i].isDirectory()) {
          continue;
        }

        // Add archive entry
        JarEntry jarAdd = new JarEntry(tobeJared[i].getName());
        jarAdd.setTime(tobeJared[i].lastModified());
        out.putNextEntry(jarAdd);

        // Write file to archive
        FileInputStream in = new FileInputStream(tobeJared[i]);
        while (true) {
          int nRead = in.read(buffer, 0, buffer.length);
          if (nRead <= 0)
            break;
          out.write(buffer, 0, nRead);
        }
        in.close();
      }
      out.close();
      stream.close();
      LOG.info("Adding classes to jar file completed");
      return true;
    } catch (Exception ex) {
      LOG.error("Error: " + ex.getMessage());
      return false;
    }
  }

  @Test
  // HBASE-3516: Test CP Class loading from HDFS
  public void testClassLoadingFromHDFS() throws Exception {
    FileSystem fs = cluster.getFileSystem();
    String className = "TestCP";

    // compose a java source file.
    String javaCode = "import org.apache.hadoop.hbase.coprocessor.*;" +
        "public class " + className + " extends BaseRegionObserver {}";

    Path baseDire = TEST_UTIL.getTestDir();
    Path srcDire = new Path(TEST_UTIL.getTestDir(), "src");

    File srcDirePath = new File(srcDire.toString());
    srcDirePath.mkdirs();

    File sourceCodeFile = new File(srcDire.toString(),
        className + ".java");

    BufferedWriter bw = new BufferedWriter(new FileWriter(sourceCodeFile));
    bw.write(javaCode);
    bw.close();

    // compile it by JavaCompiler
    JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
    ArrayList<String> srcFileNames = new ArrayList<String>();
    srcFileNames.add(sourceCodeFile.toString());

    StandardJavaFileManager fm = compiler.getStandardFileManager(null, null,
        null);
    Iterable<? extends JavaFileObject> cu =
        fm.getJavaFileObjects(sourceCodeFile);

    List<String> options = new ArrayList<String>();
    options.add("-classpath");

    // only add hbase classes to classpath. This is a little bit tricky: assume
    // the classpath is {hbaseSrc}/target/classes.
    String currentDir = new File(".").getAbsolutePath();
    options.add(currentDir + Path.SEPARATOR + "target"+ Path.SEPARATOR +
        "classes");

    JavaCompiler.CompilationTask task = compiler.getTask(
        null, fm, null, options, null, cu);

    assertTrue("Compile file " + sourceCodeFile + " failed.", task.call());

    // build a jar file by the classes files
    String jarFileName = className + ".jar";
    File jarFile = new File(baseDire.toString(), jarFileName);
    if (!createJarArchive(jarFile,
        new File[]{new File(srcDire.toString(), className + ".class")})){
      assertTrue("Build jar file failed.", false);
    }

    // copy the jar into dfs
    fs.copyFromLocalFile(new Path(jarFile.getPath()),
        new Path(fs.getUri().toString() + Path.SEPARATOR));
    String jarFileOnHDFS = fs.getUri().toString() + Path.SEPARATOR +
        jarFileName;

    assertTrue("Copy jar file to HDFS failed.",
        fs.exists(new Path(jarFileOnHDFS)));

    LOG.info("Copied jar file to HDFS: " + jarFileOnHDFS);

    // create a table that references the jar
    HTableDescriptor htd = new HTableDescriptor(className);

    htd.addFamily(new HColumnDescriptor("test"));
    htd.setValue("COPROCESSOR$1",
      jarFileOnHDFS.toString() +
      ":" + className + ":" + Coprocessor.Priority.USER);
    HBaseAdmin admin = new HBaseAdmin(this.conf);
    admin.createTable(htd);

    // verify that the coprocessor was loaded
    boolean found = false;
    MiniHBaseCluster hbase = TEST_UTIL.getHBaseCluster();
    for (HRegion region: hbase.getRegionServer(0).getOnlineRegionsLocalContext()) {
      if (region.getRegionNameAsString().startsWith(className)) {
        Coprocessor c = region.getCoprocessorHost().findCoprocessor(className);
        found = (c != null);
      }
    }
    assertTrue("Class " + className + " cannot be loaded.", found);
  }

  @Test
  // HBASE-3516: Test CP Class loading from local file system
  public void testClassLoadingFromLocalFS() throws Exception {
    FileSystem fs = cluster.getFileSystem();
    String className = "TestCP2";

    // compose a java source file.
    String javaCode = "import org.apache.hadoop.hbase.coprocessor.*;" +
        "public class " + className + " extends BaseRegionObserver {}";

    Path baseDire = TEST_UTIL.getTestDir();
    Path srcDire = new Path(TEST_UTIL.getTestDir(), "src");

    File srcDirePath = new File(srcDire.toString());
    srcDirePath.mkdirs();

    File sourceCodeFile = new File(srcDire.toString(),
        className + ".java");

    BufferedWriter bw = new BufferedWriter(new FileWriter(sourceCodeFile));
    bw.write(javaCode);
    bw.close();

    // compile it by JavaCompiler
    JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
    ArrayList<String> srcFileNames = new ArrayList<String>();
    srcFileNames.add(sourceCodeFile.toString());

    StandardJavaFileManager fm = compiler.getStandardFileManager(null, null,
        null);
    Iterable<? extends JavaFileObject> cu =
        fm.getJavaFileObjects(sourceCodeFile);

    List<String> options = new ArrayList<String>();
    options.add("-classpath");

    // only add hbase classes to classpath. This is a little bit tricky: assume
    // the classpath is {hbaseSrc}/target/classes.
    String currentDir = new File(".").getAbsolutePath();
    options.add(currentDir + Path.SEPARATOR + "target"+ Path.SEPARATOR +
        "classes");

    JavaCompiler.CompilationTask task = compiler.getTask(
        null, fm, null, options, null, cu);

    assertTrue("Compile file " + sourceCodeFile + " failed.", task.call());

    // build a jar file by the classes files
    String jarFileName = className + ".jar";
    File jarFile = new File(baseDire.toString(), jarFileName);
    if (!createJarArchive(jarFile,
        new File[]{new File(srcDire.toString(), className + ".class")})){
      assertTrue("Build jar file failed.", false);
    }

    // create a table that references the jar
    HTableDescriptor htd = new HTableDescriptor(className);

    htd.addFamily(new HColumnDescriptor("test"));
    htd.setValue("COPROCESSOR$1",
      jarFile.toString() +
      ":" + className + ":" + Coprocessor.Priority.USER);
    HBaseAdmin admin = new HBaseAdmin(this.conf);
    admin.createTable(htd);

    // verify that the coprocessor was loaded
    boolean found = false;
    MiniHBaseCluster hbase = TEST_UTIL.getHBaseCluster();
    for (HRegion region: hbase.getRegionServer(0).getOnlineRegionsLocalContext()) {
      if (region.getRegionNameAsString().startsWith(className)) {
        Coprocessor c = region.getCoprocessorHost().findCoprocessor(className);
        found = (c != null);
      }
    }
    assertTrue("Class " + className + " cannot be loaded.", found);
  }
}
