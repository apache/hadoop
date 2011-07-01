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

  static final int BUFFER_SIZE = 4096;
  static final String tableName = "TestClassLoading";
  static final String cpName1 = "TestCP1";
  static final String cpName2 = "TestCP2";
  static final String cpName3 = "TestCP3";

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

  private File buildCoprocessorJar(String className) throws Exception {
    // compose a java source file.
    String javaCode = "import org.apache.hadoop.hbase.coprocessor.*;" +
      "public class " + className + " extends BaseRegionObserver {}";
    Path baseDir = HBaseTestingUtility.getTestDir();
    Path srcDir = new Path(HBaseTestingUtility.getTestDir(), "src");
    File srcDirPath = new File(srcDir.toString());
    srcDirPath.mkdirs();
    File sourceCodeFile = new File(srcDir.toString(), className + ".java");
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
    JavaCompiler.CompilationTask task = compiler.getTask(null, fm, null,
      options, null, cu);
    assertTrue("Compile file " + sourceCodeFile + " failed.", task.call());

    // build a jar file by the classes files
    String jarFileName = className + ".jar";
    File jarFile = new File(baseDir.toString(), jarFileName);
    if (!createJarArchive(jarFile,
        new File[]{new File(srcDir.toString(), className + ".class")})){
      assertTrue("Build jar file failed.", false);
    }

    return jarFile;
  }

  @Test
  // HBASE-3516: Test CP Class loading from HDFS
  public void testClassLoadingFromHDFS() throws Exception {
    FileSystem fs = cluster.getFileSystem();

    File jarFile1 = buildCoprocessorJar(cpName1);
    File jarFile2 = buildCoprocessorJar(cpName2);

    // copy the jars into dfs
    fs.copyFromLocalFile(new Path(jarFile1.getPath()),
      new Path(fs.getUri().toString() + Path.SEPARATOR));
    String jarFileOnHDFS1 = fs.getUri().toString() + Path.SEPARATOR +
      jarFile1.getName();
    assertTrue("Copy jar file to HDFS failed.",
      fs.exists(new Path(jarFileOnHDFS1)));
    LOG.info("Copied jar file to HDFS: " + jarFileOnHDFS1);

    fs.copyFromLocalFile(new Path(jarFile2.getPath()),
      new Path(fs.getUri().toString() + Path.SEPARATOR));
    String jarFileOnHDFS2 = fs.getUri().toString() + Path.SEPARATOR +
      jarFile2.getName();
    assertTrue("Copy jar file to HDFS failed.",
      fs.exists(new Path(jarFileOnHDFS2)));
    LOG.info("Copied jar file to HDFS: " + jarFileOnHDFS2);

    // create a table that references the coprocessors
    HTableDescriptor htd = new HTableDescriptor(tableName);
    htd.addFamily(new HColumnDescriptor("test"));
      // without configuration values
    htd.setValue("COPROCESSOR$1", jarFileOnHDFS1.toString() + "|" + cpName1 +
      "|" + Coprocessor.PRIORITY_USER);
      // with configuration values
    htd.setValue("COPROCESSOR$2", jarFileOnHDFS2.toString() + "|" + cpName2 +
      "|" + Coprocessor.PRIORITY_USER + "|k1=v1,k2=v2,k3=v3");
    HBaseAdmin admin = new HBaseAdmin(this.conf);
    admin.createTable(htd);

    // verify that the coprocessors were loaded
    boolean found1 = false, found2 = false, found2_k1 = false, found2_k2 = false,
      found2_k3 = false;
    MiniHBaseCluster hbase = TEST_UTIL.getHBaseCluster();
    for (HRegion region: hbase.getRegionServer(0).getOnlineRegionsLocalContext()) {
      if (region.getRegionNameAsString().startsWith(tableName)) {
        CoprocessorEnvironment env;
        env = region.getCoprocessorHost().findCoprocessorEnvironment(cpName1);
        if (env != null) {
          found1 = true;
        }
        env = region.getCoprocessorHost().findCoprocessorEnvironment(cpName2);
        if (env != null) {
          found2 = true;
          Configuration conf = env.getConfiguration();
          found2_k1 = conf.get("k1") != null;
          found2_k2 = conf.get("k2") != null;
          found2_k3 = conf.get("k3") != null;
        }
      }
    }
    assertTrue("Class " + cpName1 + " was missing on a region", found1);
    assertTrue("Class " + cpName2 + " was missing on a region", found2);
    assertTrue("Configuration key 'k1' was missing on a region", found2_k1);
    assertTrue("Configuration key 'k2' was missing on a region", found2_k2);
    assertTrue("Configuration key 'k3' was missing on a region", found2_k3);
  }

  @Test
  // HBASE-3516: Test CP Class loading from local file system
  public void testClassLoadingFromLocalFS() throws Exception {
    File jarFile = buildCoprocessorJar(cpName3);

    // create a table that references the jar
    HTableDescriptor htd = new HTableDescriptor(cpName3);
    htd.addFamily(new HColumnDescriptor("test"));
    htd.setValue("COPROCESSOR$1", jarFile.toString() + "|" + cpName3 + "|" +
      Coprocessor.PRIORITY_USER);
    HBaseAdmin admin = new HBaseAdmin(this.conf);
    admin.createTable(htd);

    // verify that the coprocessor was loaded
    boolean found = false;
    MiniHBaseCluster hbase = TEST_UTIL.getHBaseCluster();
    for (HRegion region: hbase.getRegionServer(0).getOnlineRegionsLocalContext()) {
      if (region.getRegionNameAsString().startsWith(cpName3)) {
        found = (region.getCoprocessorHost().findCoprocessor(cpName3) != null);
      }
    }
    assertTrue("Class " + cpName3 + " was missing on a region", found);
  }
}
