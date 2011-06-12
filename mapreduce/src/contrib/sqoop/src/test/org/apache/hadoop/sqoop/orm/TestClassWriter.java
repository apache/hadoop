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
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Enumeration;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;

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
    org.apache.log4j.Logger root = org.apache.log4j.Logger.getRootLogger();
    root.setLevel(org.apache.log4j.Level.DEBUG);
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
      LOG.debug("Removing code gen dir: " + codeGenDirFile);
      if (!DirUtil.deleteDir(codeGenDirFile)) {
        LOG.warn("Could not delete " + codeGenDirFile + " prior to test");
      }
    }

    if (classGenDirFile.exists()) {
      LOG.debug("Removing class gen dir: " + classGenDirFile);
      if (!DirUtil.deleteDir(classGenDirFile)) {
        LOG.warn("Could not delete " + classGenDirFile + " prior to test");
      }
    }
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
   * Run a test to verify that we can generate code and it emits the output files
   * where we expect them.
   */
  private void runGenerationTest(String [] argv, String classNameToCheck) {
    File codeGenDirFile = new File(CODE_GEN_DIR);
    File classGenDirFile = new File(JAR_GEN_DIR);

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

    String classFileNameToCheck = classNameToCheck.replace('.', File.separatorChar);
    LOG.debug("Class file to check for: " + classFileNameToCheck);

    // check that all the files we expected to generate (.java, .class, .jar) exist.
    File tableFile = new File(codeGenDirFile, classFileNameToCheck + ".java");
    assertTrue("Cannot find generated source file for table!", tableFile.exists());
    LOG.debug("Found generated source: " + tableFile);

    File tableClassFile = new File(classGenDirFile, classFileNameToCheck + ".class");
    assertTrue("Cannot find generated class file for table!", tableClassFile.exists());
    LOG.debug("Found generated class: " + tableClassFile);

    File jarFile = new File(compileMgr.getJarFilename());
    assertTrue("Cannot find compiled jar", jarFile.exists());
    LOG.debug("Found generated jar: " + jarFile);

    // check that the .class file made it into the .jar by enumerating 
    // available entries in the jar file.
    boolean foundCompiledClass = false;
    try {
      JarInputStream jis = new JarInputStream(new FileInputStream(jarFile));

      LOG.debug("Jar file has entries:");
      while (true) {
        JarEntry entry = jis.getNextJarEntry();
        if (null == entry) {
          // no more entries.
          break;
        }

        if (entry.getName().equals(classFileNameToCheck + ".class")) {
          foundCompiledClass = true;
          LOG.debug(" * " + entry.getName());
        } else {
          LOG.debug("   " + entry.getName());
        }
      }

      jis.close();
    } catch (IOException ioe) {
      fail("Got IOException iterating over Jar file: " + ioe.toString());
    }

    assertTrue("Cannot find .class file " + classFileNameToCheck + ".class in jar file",
        foundCompiledClass);

    LOG.debug("Found class in jar - test success!");
  }

  /**
   * Test that we can generate code. Test that we can redirect the --outdir and --bindir too.
   */
  @Test
  public void testCodeGen() {

    // Set the option strings in an "argv" to redirect our srcdir and bindir
    String [] argv = {
        "--bindir",
        JAR_GEN_DIR,
        "--outdir",
        CODE_GEN_DIR
    };

    runGenerationTest(argv, HsqldbTestServer.getTableName());
  }

  private static final String OVERRIDE_CLASS_NAME = "override";

  /**
   * Test that we can generate code with a custom class name
   */
  @Test
  public void testSetClassName() {

    // Set the option strings in an "argv" to redirect our srcdir and bindir
    String [] argv = {
        "--bindir",
        JAR_GEN_DIR,
        "--outdir",
        CODE_GEN_DIR,
        "--class-name",
        OVERRIDE_CLASS_NAME
    };

    runGenerationTest(argv, OVERRIDE_CLASS_NAME);
  }

  private static final String OVERRIDE_CLASS_AND_PACKAGE_NAME = "override.pkg.prefix.classname";

  /**
   * Test that we can generate code with a custom class name that includes a package
   */
  @Test
  public void testSetClassAndPackageName() {

    // Set the option strings in an "argv" to redirect our srcdir and bindir
    String [] argv = {
        "--bindir",
        JAR_GEN_DIR,
        "--outdir",
        CODE_GEN_DIR,
        "--class-name",
        OVERRIDE_CLASS_AND_PACKAGE_NAME
    };

    runGenerationTest(argv, OVERRIDE_CLASS_AND_PACKAGE_NAME);
  }
 
  private static final String OVERRIDE_PACKAGE_NAME = "special.userpackage.name";

  /**
   * Test that we can generate code with a custom class name that includes a package
   */
  @Test
  public void testSetPackageName() {

    // Set the option strings in an "argv" to redirect our srcdir and bindir
    String [] argv = {
        "--bindir",
        JAR_GEN_DIR,
        "--outdir",
        CODE_GEN_DIR,
        "--package-name",
        OVERRIDE_PACKAGE_NAME
    };

    runGenerationTest(argv, OVERRIDE_PACKAGE_NAME + "." + HsqldbTestServer.getTableName());
  }
}

