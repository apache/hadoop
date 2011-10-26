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
import org.apache.hadoop.hbase.HServerLoad;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import javax.tools.*;
import java.io.*;
import java.util.*;
import java.util.Arrays;
import java.util.jar.*;

import org.junit.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

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
  static final String cpName4 = "TestCP4";
  static final String cpName5 = "TestCP5";

  private static Class regionCoprocessor1 = ColumnAggregationEndpoint.class;
  private static Class regionCoprocessor2 = GenericEndpoint.class;
  private static Class regionServerCoprocessor = SampleRegionWALObserver.class;
  private static Class masterCoprocessor = BaseMasterObserver.class;

  private static final String[] regionServerSystemCoprocessors =
      new String[]{
      regionCoprocessor1.getSimpleName(),
      regionServerCoprocessor.getSimpleName()
  };

  private static final String[] regionServerSystemAndUserCoprocessors =
      new String[] {
      regionCoprocessor1.getSimpleName(),
      regionCoprocessor2.getSimpleName(),
      regionServerCoprocessor.getSimpleName()
  };

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    conf = TEST_UTIL.getConfiguration();

    // regionCoprocessor1 will be loaded on all regionservers, since it is
    // loaded for any tables (user or meta).
    conf.setStrings(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
        regionCoprocessor1.getName());

    // regionCoprocessor2 will be loaded only on regionservers that serve a
    // user table region. Therefore, if there are no user tables loaded,
    // this coprocessor will not be loaded on any regionserver.
    conf.setStrings(CoprocessorHost.USER_REGION_COPROCESSOR_CONF_KEY,
        regionCoprocessor2.getName());

    conf.setStrings(CoprocessorHost.WAL_COPROCESSOR_CONF_KEY,
        regionServerCoprocessor.getName());
    conf.setStrings(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY,
        masterCoprocessor.getName());
    TEST_UTIL.startMiniCluster(1);
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
    Path baseDir = TEST_UTIL.getDataTestDir();
    Path srcDir = new Path(TEST_UTIL.getDataTestDir(), "src");
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
    String classpath =
        currentDir + Path.SEPARATOR + "target"+ Path.SEPARATOR + "classes" +
        System.getProperty("path.separator") +
        System.getProperty("surefire.test.class.path");
    options.add(classpath);
    LOG.debug("Setting classpath to: "+classpath);

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
    if (admin.tableExists(tableName)) {
      admin.disableTable(tableName);
      admin.deleteTable(tableName);
    }
    admin.createTable(htd);

    // verify that the coprocessors were loaded
    boolean found1 = false, found2 = false, found2_k1 = false,
        found2_k2 = false, found2_k3 = false;
    MiniHBaseCluster hbase = TEST_UTIL.getHBaseCluster();
    for (HRegion region:
        hbase.getRegionServer(0).getOnlineRegionsLocalContext()) {
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
    for (HRegion region:
        hbase.getRegionServer(0).getOnlineRegionsLocalContext()) {
      if (region.getRegionNameAsString().startsWith(cpName3)) {
        found = (region.getCoprocessorHost().findCoprocessor(cpName3) != null);
      }
    }
    assertTrue("Class " + cpName3 + " was missing on a region", found);
  }

  @Test
  // HBase-3810: Registering a Coprocessor at HTableDescriptor should be
  // less strict
  public void testHBase3810() throws Exception {
    // allowed value pattern: [path] | class name | [priority] | [key values]

    File jarFile1 = buildCoprocessorJar(cpName1);
    File jarFile2 = buildCoprocessorJar(cpName2);
    File jarFile4 = buildCoprocessorJar(cpName4);
    File jarFile5 = buildCoprocessorJar(cpName5);

    String cpKey1 = "COPROCESSOR$1";
    String cpKey2 = " Coprocessor$2 ";
    String cpKey3 = " coprocessor$03 ";

    String cpValue1 = jarFile1.toString() + "|" + cpName1 + "|" +
        Coprocessor.PRIORITY_USER;
    String cpValue2 = jarFile2.toString() + " | " + cpName2 + " | ";
    // load from default class loader
    String cpValue3 =
        " | org.apache.hadoop.hbase.coprocessor.SimpleRegionObserver | | k=v ";

    // create a table that references the jar
    HTableDescriptor htd = new HTableDescriptor(tableName);
    htd.addFamily(new HColumnDescriptor("test"));

    // add 3 coprocessors by setting htd attributes directly.
    htd.setValue(cpKey1, cpValue1);
    htd.setValue(cpKey2, cpValue2);
    htd.setValue(cpKey3, cpValue3);

    // add 2 coprocessor by using new htd.addCoprocessor() api
    htd.addCoprocessor(cpName4, new Path(jarFile4.getPath()),
        Coprocessor.PRIORITY_USER, null);
    Map<String, String> kvs = new HashMap<String, String>();
    kvs.put("k1", "v1");
    kvs.put("k2", "v2");
    kvs.put("k3", "v3");
    htd.addCoprocessor(cpName5, new Path(jarFile5.getPath()),
        Coprocessor.PRIORITY_USER, kvs);

    HBaseAdmin admin = new HBaseAdmin(this.conf);
    if (admin.tableExists(tableName)) {
      admin.disableTable(tableName);
      admin.deleteTable(tableName);
    }
    admin.createTable(htd);

    // verify that the coprocessor was loaded
    boolean found_2 = false, found_1 = false, found_3 = false,
        found_4 = false, found_5 = false;
    boolean found5_k1 = false, found5_k2 = false, found5_k3 = false,
        found5_k4 = false;

    MiniHBaseCluster hbase = TEST_UTIL.getHBaseCluster();
    for (HRegion region:
        hbase.getRegionServer(0).getOnlineRegionsLocalContext()) {
      if (region.getRegionNameAsString().startsWith(tableName)) {
        found_1 = found_1 ||
            (region.getCoprocessorHost().findCoprocessor(cpName1) != null);
        found_2 = found_2 ||
            (region.getCoprocessorHost().findCoprocessor(cpName2) != null);
        found_3 = found_3 ||
            (region.getCoprocessorHost().findCoprocessor("SimpleRegionObserver")
                != null);
        found_4 = found_4 ||
            (region.getCoprocessorHost().findCoprocessor(cpName4) != null);

        CoprocessorEnvironment env =
            region.getCoprocessorHost().findCoprocessorEnvironment(cpName5);
        if (env != null) {
          found_5 = true;
          Configuration conf = env.getConfiguration();
          found5_k1 = conf.get("k1") != null;
          found5_k2 = conf.get("k2") != null;
          found5_k3 = conf.get("k3") != null;
        }
      }
    }

    assertTrue("Class " + cpName1 + " was missing on a region", found_1);
    assertTrue("Class " + cpName2 + " was missing on a region", found_2);
    assertTrue("Class SimpleRegionObserver was missing on a region", found_3);
    assertTrue("Class " + cpName4 + " was missing on a region", found_4);
    assertTrue("Class " + cpName5 + " was missing on a region", found_5);

    assertTrue("Configuration key 'k1' was missing on a region", found5_k1);
    assertTrue("Configuration key 'k2' was missing on a region", found5_k2);
    assertTrue("Configuration key 'k3' was missing on a region", found5_k3);
    assertFalse("Configuration key 'k4' wasn't configured", found5_k4);
  }

  @Test
  public void testRegionServerCoprocessorsReported() throws Exception {
    // HBASE 4070: Improve region server metrics to report loaded coprocessors
    // to master: verify that each regionserver is reporting the correct set of
    // loaded coprocessors.

    // We rely on the fact that getCoprocessors() will return a sorted
    // display of the coprocessors' names, so for example, regionCoprocessor1's
    // name "ColumnAggregationEndpoint" will appear before regionCoprocessor2's
    // name "GenericEndpoint" because "C" is before "G" lexicographically.

    HBaseAdmin admin = new HBaseAdmin(this.conf);

    // disable all user tables, if any are loaded.
    for (HTableDescriptor htd: admin.listTables()) {
      if (!htd.isMetaTable()) {
        String tableName = htd.getNameAsString();
        if (admin.isTableEnabled(tableName)) {
          try {
            admin.disableTable(htd.getNameAsString());
          } catch (TableNotEnabledException e) {
            // ignoring this exception for now : not sure why it's happening.
          }
        }
      }
    }

    // should only be system coprocessors loaded at this point.
    assertAllRegionServers(regionServerSystemCoprocessors,null);

    // The next two tests enable and disable user tables to see if coprocessor
    // load reporting changes as coprocessors are loaded and unloaded.
    //

    // Create a table.
    // should cause regionCoprocessor2 to be loaded, since we've specified it
    // for loading on any user table with USER_REGION_COPROCESSOR_CONF_KEY
    // in setUpBeforeClass().
    String userTable1 = "userTable1";
    HTableDescriptor userTD1 = new HTableDescriptor(userTable1);
    admin.createTable(userTD1);
    // table should be enabled now.
    assertTrue(admin.isTableEnabled(userTable1));
    assertAllRegionServers(regionServerSystemAndUserCoprocessors, userTable1);

    // unload and make sure we're back to only system coprocessors again.
    admin.disableTable(userTable1);
    assertAllRegionServers(regionServerSystemCoprocessors,null);

    // create another table, with its own specified coprocessor.
    String userTable2 = "userTable2";
    HTableDescriptor htd2 = new HTableDescriptor(userTable2);

    String userTableCP = "userTableCP";
    File jarFile1 = buildCoprocessorJar(userTableCP);
    htd2.addFamily(new HColumnDescriptor("myfamily"));
    htd2.setValue("COPROCESSOR$1", jarFile1.toString() + "|" + userTableCP +
      "|" + Coprocessor.PRIORITY_USER);
    admin.createTable(htd2);
    // table should be enabled now.
    assertTrue(admin.isTableEnabled(userTable2));

    ArrayList<String> existingCPsPlusNew =
        new ArrayList<String>(Arrays.asList(regionServerSystemAndUserCoprocessors));
    existingCPsPlusNew.add(userTableCP);
    String[] existingCPsPlusNewArray = new String[existingCPsPlusNew.size()];
    assertAllRegionServers(existingCPsPlusNew.toArray(existingCPsPlusNewArray),
        userTable2);

    admin.disableTable(userTable2);
    assertTrue(admin.isTableDisabled(userTable2));

    // we should be back to only system coprocessors again.
    assertAllRegionServers(regionServerSystemCoprocessors, null);

  }

  /**
   * return the subset of all regionservers
   * (actually returns set of HServerLoads)
   * which host some region in a given table.
   * used by assertAllRegionServers() below to
   * test reporting of loaded coprocessors.
   * @param tableName : given table.
   * @return subset of all servers.
   */
  Map<ServerName, HServerLoad> serversForTable(String tableName) {
    Map<ServerName, HServerLoad> serverLoadHashMap =
        new HashMap<ServerName, HServerLoad>();
    for(Map.Entry<ServerName,HServerLoad> server:
        TEST_UTIL.getMiniHBaseCluster().getMaster().getServerManager().
            getOnlineServers().entrySet()) {
      for(Map.Entry<byte[], HServerLoad.RegionLoad> region:
          server.getValue().getRegionsLoad().entrySet()) {
        if (region.getValue().getNameAsString().equals(tableName)) {
          // this server server hosts a region of tableName: add this server..
          serverLoadHashMap.put(server.getKey(),server.getValue());
          // .. and skip the rest of the regions that it hosts.
          break;
        }
      }
    }
    return serverLoadHashMap;
  }

  void assertAllRegionServers(String[] expectedCoprocessors, String tableName)
      throws InterruptedException {
    Map<ServerName, HServerLoad> servers;
    String[] actualCoprocessors = null;
    boolean success = false;
    for(int i = 0; i < 5; i++) {
      if (tableName == null) {
        //if no tableName specified, use all servers.
        servers =
            TEST_UTIL.getMiniHBaseCluster().getMaster().getServerManager().
                getOnlineServers();
      } else {
        servers = serversForTable(tableName);
      }
      boolean any_failed = false;
      for(Map.Entry<ServerName,HServerLoad> server: servers.entrySet()) {
        actualCoprocessors = server.getValue().getCoprocessors();
        if (!Arrays.equals(actualCoprocessors, expectedCoprocessors)) {
          LOG.debug("failed comparison: actual: " +
              Arrays.toString(actualCoprocessors) +
              " ; expected: " + Arrays.toString(expectedCoprocessors));
          any_failed = true;
          break;
        }
      }
      if (any_failed == false) {
        success = true;
        break;
      }
      LOG.debug("retrying after failed comparison: " + i);
      Thread.sleep(1000);
    }
    assertTrue(success);
  }

  @Test
  public void testMasterCoprocessorsReported() {
    // HBASE 4070: Improve region server metrics to report loaded coprocessors
    // to master: verify that the master is reporting the correct set of
    // loaded coprocessors.
    final String loadedMasterCoprocessorsVerify =
        "[" + masterCoprocessor.getSimpleName() + "]";
    String loadedMasterCoprocessors =
        java.util.Arrays.toString(
            TEST_UTIL.getHBaseCluster().getMaster().getCoprocessors());
    assertEquals(loadedMasterCoprocessorsVerify, loadedMasterCoprocessors);
  }
}
