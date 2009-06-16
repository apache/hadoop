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

package org.apache.hadoop.sqoop.orm;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.hadoop.sqoop.ImportOptions;
import org.apache.hadoop.sqoop.ImportOptions.InvalidOptionsException;
import org.apache.hadoop.sqoop.manager.ConnManager;
import org.apache.hadoop.sqoop.testutil.DirUtil;
import org.apache.hadoop.sqoop.testutil.HsqldbTestServer;
import org.apache.hadoop.sqoop.testutil.ImportJobTestCase;

/**
 * Test that the ClassWriter generates Java classes based on the given table,
 * which compile.
 *
 * 
 */
public class TestClassWriter extends TestCase {

  public static final Log LOG =
      LogFactory.getLog(TestClassWriter.class.getName());

  // instance variables populated during setUp, used during tests
  private HsqldbTestServer testServer;
  private ConnManager manager;
  private ImportOptions options;

  @Before
  public void setUp() {
    testServer = new HsqldbTestServer();
    try {
      testServer.resetServer();
    } catch (SQLException sqlE) {
      LOG.error("Got SQLException: " + sqlE.toString());
      fail("Got SQLException: " + sqlE.toString());
    } catch (ClassNotFoundException cnfe) {
      LOG.error("Could not find class for db driver: " + cnfe.toString());
      fail("Could not find class for db driver: " + cnfe.toString());
    }

    manager = testServer.getManager();
    options = testServer.getImportOptions();
  }

  @After
  public void tearDown() {
    try {
      manager.close();
    } catch (SQLException sqlE) {
      LOG.error("Got SQLException: " + sqlE.toString());
      fail("Got SQLException: " + sqlE.toString());
    }
  }

  static final String CODE_GEN_DIR = ImportJobTestCase.TEMP_BASE_DIR + "sqoop/test/codegen";
  static final String JAR_GEN_DIR = ImportJobTestCase.TEMP_BASE_DIR + "sqoop/test/jargen";

  /**
   * Test that we can generate code. Test that we can redirect the --outdir and --bindir too.
   */
  @Test
  public void testCodeGen() {

    // sanity check: make sure we're in a tmp dir before we blow anything away.
    assertTrue("Test generates code in non-tmp dir!",
        CODE_GEN_DIR.startsWith(ImportJobTestCase.TEMP_BASE_DIR));
    assertTrue("Test generates jars in non-tmp dir!",
        JAR_GEN_DIR.startsWith(ImportJobTestCase.TEMP_BASE_DIR));

    // start out by removing these directories ahead of time
    // to ensure that this is truly generating the code.
    File codeGenDirFile = new File(CODE_GEN_DIR);
    File classGenDirFile = new File(JAR_GEN_DIR);

    if (codeGenDirFile.exists()) {
      DirUtil.deleteDir(codeGenDirFile);
    }

    if (classGenDirFile.exists()) {
      DirUtil.deleteDir(classGenDirFile);
    }

    // Set the option strings in an "argv" to redirect our srcdir and bindir
    String [] argv = {
        "--bindir",
        JAR_GEN_DIR,
        "--outdir",
        CODE_GEN_DIR
    };

    try {
      options.parse(argv);
    } catch (InvalidOptionsException ioe) {
      LOG.error("Could not parse options: " + ioe.toString());
    }

    CompilationManager compileMgr = new CompilationManager(options);
    ClassWriter writer = new ClassWriter(options, manager, HsqldbTestServer.getTableName(),
        compileMgr);

    try {
      writer.generate();
      compileMgr.compile();
      compileMgr.jar();
    } catch (IOException ioe) {
      LOG.error("Got IOException: " + ioe.toString());
      fail("Got IOException: " + ioe.toString());
    }

    File tableFile = new File(codeGenDirFile, HsqldbTestServer.getTableName() + ".java");
    assertTrue("Cannot find generated source file for table!", tableFile.exists());

    File tableClassFile = new File(classGenDirFile, HsqldbTestServer.getTableName() + ".class");
    assertTrue("Cannot find generated class file for table!", tableClassFile.exists());

    File jarFile = new File(compileMgr.getJarFilename());
    assertTrue("Cannot find compiled jar", jarFile.exists());
  }
}

