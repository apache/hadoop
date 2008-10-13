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

package org.apache.hadoop.cli;

import java.io.File;
import java.util.ArrayList;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import junit.framework.TestCase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.cli.util.ComparatorBase;
import org.apache.hadoop.cli.util.ComparatorData;
import org.apache.hadoop.cli.util.CLITestData;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import org.apache.hadoop.cli.util.CommandExecutor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.fs.FileSystem;

/**
 * Tests for the Command Line Interface (CLI)
 */
public class TestCLI extends TestCase {
  private static final Log LOG =
    LogFactory.getLog(TestCLI.class.getName());
  
  // In this mode, it runs the command and compares the actual output
  // with the expected output  
  public static final String TESTMODE_TEST = "test"; // Run the tests
  
  // If it is set to nocompare, run the command and do not compare.
  // This can be useful populate the testConfig.xml file the first time
  // a new command is added
  public static final String TESTMODE_NOCOMPARE = "nocompare";
  public static final String TEST_CACHE_DATA_DIR =
    System.getProperty("test.cache.data", "build/test/cache");
  
  //By default, run the tests. The other mode is to run the commands and not
  // compare the output
  public static String testMode = TESTMODE_TEST;
  
  // Storage for tests read in from the config file
  static ArrayList<CLITestData> testsFromConfigFile = null;
  static ArrayList<ComparatorData> testComparators = null;
  static String testConfigFile = "testConf.xml";
  String thisTestCaseName = null;
  static ComparatorData comparatorData = null;
  
  private static Configuration conf = null;
  private static MiniDFSCluster cluster = null;
  private static DistributedFileSystem dfs = null;
  private static String namenode = null;
  private static String clitestDataDir = null;
  private static String username = null;
  
  /**
   * Read the test config file - testConfig.xml
   */
  private void readTestConfigFile() {
    
    if (testsFromConfigFile == null) {
      boolean success = false;
      testConfigFile = TEST_CACHE_DATA_DIR + File.separator + testConfigFile;
      try {
        SAXParser p = (SAXParserFactory.newInstance()).newSAXParser();
        p.parse(testConfigFile, new TestConfigFileParser());
        success = true;
      } catch (Exception e) {
        LOG.info("File: " + testConfigFile + " not found");
        success = false;
      }
      assertTrue("Error reading test config file", success);
    }
  }
  
  /*
   * Setup
   */
  public void setUp() throws Exception {
    // Read the testConfig.xml file
    readTestConfigFile();
    
    // Start up the mini dfs cluster
    boolean success = false;
    conf = new Configuration();
    cluster = new MiniDFSCluster(conf, 1, true, null);
    namenode = conf.get("fs.default.name", "file:///");
    clitestDataDir = new File(TEST_CACHE_DATA_DIR).
      toURI().toString().replace(' ', '+');
    username = System.getProperty("user.name");

    FileSystem fs = cluster.getFileSystem();
    assertTrue("Not a HDFS: "+fs.getUri(),
               fs instanceof DistributedFileSystem);
    dfs = (DistributedFileSystem) fs;
    success = true;

    assertTrue("Error setting up Mini DFS cluster", success);
  }
  
  /**
   * Tear down
   */
  public void tearDown() throws Exception {
    boolean success = false;
    dfs.close();
    cluster.shutdown();
    success = true;
    Thread.sleep(2000);

    assertTrue("Error tearing down Mini DFS cluster", success);
    
    displayResults();
  }
  
  /**
   * Expand the commands from the test config xml file
   * @param cmd
   * @return String expanded command
   */
  private String expandCommand(final String cmd) {
    String expCmd = cmd;
    expCmd = expCmd.replaceAll("NAMENODE", namenode);
    expCmd = expCmd.replaceAll("CLITEST_DATA", clitestDataDir);
    expCmd = expCmd.replaceAll("USERNAME", username);
    
    return expCmd;
  }
  
  /**
   * Display the summarized results
   */
  private void displayResults() {
    LOG.info("Detailed results:");
    LOG.info("----------------------------------\n");
    
    for (int i = 0; i < testsFromConfigFile.size(); i++) {
      CLITestData td = testsFromConfigFile.get(i);
      
      boolean testResult = td.getTestResult();
      
      // Display the details only if there is a failure
      if (!testResult) {
        LOG.info("-------------------------------------------");
        LOG.info("                    Test ID: [" + (i + 1) + "]");
        LOG.info("           Test Description: [" + td.getTestDesc() + "]");
        LOG.info("");

        ArrayList<String> testCommands = td.getTestCommands();
        for (int j = 0; j < testCommands.size(); j++) {
          LOG.info("              Test Commands: [" + 
              expandCommand((String) testCommands.get(j)) + "]");
        }

        LOG.info("");
        ArrayList<String> cleanupCommands = td.getCleanupCommands();
        for (int j = 0; j < cleanupCommands.size(); j++) {
          LOG.info("           Cleanup Commands: [" +
              expandCommand((String) cleanupCommands.get(j)) + "]");
        }

        LOG.info("");
        ArrayList<ComparatorData> compdata = td.getComparatorData();
        for (int j = 0; j < compdata.size(); j++) {
          boolean resultBoolean = compdata.get(j).getTestResult();
          LOG.info("                 Comparator: [" + 
              compdata.get(j).getComparatorType() + "]");
          LOG.info("         Comparision result:   [" + 
              (resultBoolean ? "pass" : "fail") + "]");
          LOG.info("            Expected output:   [" + 
              compdata.get(j).getExpectedOutput() + "]");
          LOG.info("              Actual output:   [" + 
              compdata.get(j).getActualOutput() + "]");
        }
        LOG.info("");
      }
    }
    
    LOG.info("Summary results:");
    LOG.info("----------------------------------\n");
    
    boolean overallResults = true;
    int totalPass = 0;
    int totalFail = 0;
    int totalComparators = 0;
    for (int i = 0; i < testsFromConfigFile.size(); i++) {
      CLITestData td = testsFromConfigFile.get(i);
      totalComparators += 
    	  testsFromConfigFile.get(i).getComparatorData().size();
      boolean resultBoolean = td.getTestResult();
      if (resultBoolean) {
        totalPass ++;
      } else {
        totalFail ++;
      }
      overallResults &= resultBoolean;
    }
    
    
    LOG.info("               Testing mode: " + testMode);
    LOG.info("");
    LOG.info("             Overall result: " + 
    		(overallResults ? "+++ PASS +++" : "--- FAIL ---"));
    LOG.info("               # Tests pass: " + totalPass +
    		" (" + (100 * totalPass / (totalPass + totalFail)) + "%)");
    LOG.info("               # Tests fail: " + totalFail + 
    		" (" + (100 * totalFail / (totalPass + totalFail)) + "%)");
    LOG.info("         # Validations done: " + totalComparators + 
    		" (each test may do multiple validations)");
    
    LOG.info("");
    LOG.info("Failing tests:");
    LOG.info("--------------");
    int i = 0;
    boolean foundTests = false;
    for (i = 0; i < testsFromConfigFile.size(); i++) {
      boolean resultBoolean = testsFromConfigFile.get(i).getTestResult();
      if (!resultBoolean) {
        LOG.info((i + 1) + ": " + 
        		testsFromConfigFile.get(i).getTestDesc());
        foundTests = true;
      }
    }
    if (!foundTests) {
    	LOG.info("NONE");
    }
    
    foundTests = false;
    LOG.info("");
    LOG.info("Passing tests:");
    LOG.info("--------------");
    for (i = 0; i < testsFromConfigFile.size(); i++) {
      boolean resultBoolean = testsFromConfigFile.get(i).getTestResult();
      if (resultBoolean) {
        LOG.info((i + 1) + ": " + 
        		testsFromConfigFile.get(i).getTestDesc());
        foundTests = true;
      }
    }
    if (!foundTests) {
    	LOG.info("NONE");
    }

    assertTrue("One of the tests failed. " +
    		"See the Detailed results to identify " +
    		"the command that failed", overallResults);
    
  }
  
  /**
   * Compare the actual output with the expected output
   * @param compdata
   * @return
   */
  private boolean compareTestOutput(ComparatorData compdata) {
    // Compare the output based on the comparator
    String comparatorType = compdata.getComparatorType();
    Class<?> comparatorClass = null;
    
    // If testMode is "test", then run the command and compare the output
    // If testMode is "nocompare", then run the command and dump the output.
    // Do not compare
    
    boolean compareOutput = false;
    
    if (testMode.equals(TESTMODE_TEST)) {
      try {
    	// Initialize the comparator class and run its compare method
        comparatorClass = Class.forName("org.apache.hadoop.cli.util." + 
          comparatorType);
        ComparatorBase comp = (ComparatorBase) comparatorClass.newInstance();
        compareOutput = comp.compare(CommandExecutor.getLastCommandOutput(), 
          compdata.getExpectedOutput());
      } catch (Exception e) {
        LOG.info("Error in instantiating the comparator" + e);
      }
    }
    
    return compareOutput;
  }
  
  /***********************************
   ************* TESTS
   *********************************/
  
  public void testAll() {
    LOG.info("TestAll");
    
    // Run the tests defined in the testConf.xml config file.
    for (int index = 0; index < testsFromConfigFile.size(); index++) {
      
      CLITestData testdata = (CLITestData) testsFromConfigFile.get(index);
   
      // Execute the test commands
      ArrayList<String> testCommands = testdata.getTestCommands();
      for (int i = 0; i < testCommands.size(); i++) {
        CommandExecutor.executeFSCommand(testCommands.get(i), 
        		namenode);
      }
      
      boolean overallTCResult = true;
      // Run comparators
      ArrayList<ComparatorData> compdata = testdata.getComparatorData();
      for (int i = 0; i < compdata.size(); i++) {
        final String comptype = compdata.get(i).getComparatorType();
        
        boolean compareOutput = false;
        
        if (! comptype.equalsIgnoreCase("none")) {
          compareOutput = compareTestOutput(compdata.get(i));
          overallTCResult &= compareOutput;
        }
        
        compdata.get(i).setExitCode(CommandExecutor.getLastExitCode());
        compdata.get(i).setActualOutput(
          CommandExecutor.getLastCommandOutput());
        compdata.get(i).setTestResult(compareOutput);
      }
      testdata.setTestResult(overallTCResult);
      
      // Execute the cleanup commands
      ArrayList<String> cleanupCommands = testdata.getCleanupCommands();
      for (int i = 0; i < cleanupCommands.size(); i++) {
        CommandExecutor.executeFSCommand(cleanupCommands.get(i), 
        		namenode);
      }
    }
  }
  
  /*
   * Parser class for the test config xml file
   */
  static class TestConfigFileParser extends DefaultHandler {
    String charString = null;
    CLITestData td = null;
    ArrayList<String> testCommands = null;
    ArrayList<String> cleanupCommands = null;
    
    @Override
    public void startDocument() throws SAXException {
      testsFromConfigFile = new ArrayList<CLITestData>();
    }
    
    @Override
    public void startElement(String uri, 
    		String localName, 
    		String qName, 
    		Attributes attributes) throws SAXException {
      if (qName.equals("test")) {
        td = new CLITestData();
      } else if (qName.equals("test-commands")) {
        testCommands = new ArrayList<String>();
      } else if (qName.equals("cleanup-commands")) {
        cleanupCommands = new ArrayList<String>();
      } else if (qName.equals("comparators")) {
        testComparators = new ArrayList<ComparatorData>();
      } else if (qName.equals("comparator")) {
        comparatorData = new ComparatorData();
      }
      charString = "";
    }
    
    @Override
    public void endElement(String uri, 
    		String localName, 
    		String qName) throws SAXException {
      if (qName.equals("description")) {
        td.setTestDesc(charString);
      } else if (qName.equals("test-commands")) {
        td.setTestCommands(testCommands);
        testCommands = null;
      } else if (qName.equals("cleanup-commands")) {
        td.setCleanupCommands(cleanupCommands);
        cleanupCommands = null;
      } else if (qName.equals("command")) {
        if (testCommands != null) {
          testCommands.add(charString);
        } else if (cleanupCommands != null) {
          cleanupCommands.add(charString);
        }
      } else if (qName.equals("comparators")) {
        td.setComparatorData(testComparators);
      } else if (qName.equals("comparator")) {
        testComparators.add(comparatorData);
      } else if (qName.equals("type")) {
        comparatorData.setComparatorType(charString);
      } else if (qName.equals("expected-output")) {
        comparatorData.setExpectedOutput(charString);
      } else if (qName.equals("test")) {
        testsFromConfigFile.add(td);
        td = null;
      } else if (qName.equals("mode")) {
        testMode = charString;
        if (!testMode.equals(TESTMODE_NOCOMPARE) &&
            !testMode.equals(TESTMODE_TEST)) {
          testMode = TESTMODE_TEST;
        }
      }
    }
    
    @Override
    public void characters(char[] ch, 
    		int start, 
    		int length) throws SAXException {
      String s = new String(ch, start, length);
      charString += s;
    }
  }
}
