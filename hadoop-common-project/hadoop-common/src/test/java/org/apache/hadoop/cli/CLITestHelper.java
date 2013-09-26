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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.cli.util.*;
import org.apache.hadoop.cli.util.CommandExecutor.Result;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.StringUtils;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.File;
import java.util.ArrayList;

/**
 * Tests for the Command Line Interface (CLI)
 */
public class CLITestHelper {
  private static final Log LOG =
    LogFactory.getLog(CLITestHelper.class.getName());
  
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
  protected String testMode = TESTMODE_TEST;
  
  // Storage for tests read in from the config file
  protected ArrayList<CLITestData> testsFromConfigFile = null;
  protected ArrayList<ComparatorData> testComparators = null;
  protected String thisTestCaseName = null;
  protected ComparatorData comparatorData = null;
  protected Configuration conf = null;
  protected String clitestDataDir = null;
  protected String username = null;
  /**
   * Read the test config file - testConfig.xml
   */
  protected void readTestConfigFile() {
    String testConfigFile = getTestFile();
    if (testsFromConfigFile == null) {
      boolean success = false;
      testConfigFile = TEST_CACHE_DATA_DIR + File.separator + testConfigFile;
      try {
        SAXParser p = (SAXParserFactory.newInstance()).newSAXParser();
        p.parse(testConfigFile, getConfigParser());
        success = true;
      } catch (Exception e) {
        LOG.info("File: " + testConfigFile + " not found");
        success = false;
      }
      assertTrue("Error reading test config file", success);
    }
  }

  /**
   * Method decides what is a proper configuration file parser for this type
   * of CLI tests.
   * Ancestors need to override the implementation if a parser with additional
   * features is needed. Also, such ancestor has to provide its own
   * TestConfigParser implementation
   * @return an instance of TestConfigFileParser class
   */
  protected TestConfigFileParser getConfigParser () {
    return new TestConfigFileParser();
  }

  protected String getTestFile() {
    return "";
  }
  
  /*
   * Setup
   */
  public void setUp() throws Exception {
    // Read the testConfig.xml file
    readTestConfigFile();
    
    conf = new Configuration();
    conf.setBoolean(CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION, 
                    true);

    clitestDataDir = new File(TEST_CACHE_DATA_DIR).
    toURI().toString().replace(' ', '+');
  }
  
  /**
   * Tear down
   */
  public void tearDown() throws Exception {
    displayResults();
  }
  
  /**
   * Expand the commands from the test config xml file
   * @param cmd
   * @return String expanded command
   */
  protected String expandCommand(final String cmd) {
    String expCmd = cmd;
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

        ArrayList<CLICommand> testCommands = td.getTestCommands();
        for (CLICommand cmd : testCommands) {
          LOG.info("              Test Commands: [" + 
                   expandCommand(cmd.getCmd()) + "]");
        }

        LOG.info("");
        ArrayList<CLICommand> cleanupCommands = td.getCleanupCommands();
        for (CLICommand cmd : cleanupCommands) {
          LOG.info("           Cleanup Commands: [" +
                   expandCommand(cmd.getCmd()) + "]");
        }

        LOG.info("");
        ArrayList<ComparatorData> compdata = td.getComparatorData();
        for (ComparatorData cd : compdata) {
          boolean resultBoolean = cd.getTestResult();
          LOG.info("                 Comparator: [" + 
                   cd.getComparatorType() + "]");
          LOG.info("         Comparision result:   [" + 
                   (resultBoolean ? "pass" : "fail") + "]");
          LOG.info("            Expected output:   [" + 
                   expandCommand(cd.getExpectedOutput()) + "]");
          LOG.info("              Actual output:   [" + 
                   cd.getActualOutput() + "]");
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
    if ((totalPass + totalFail) == 0) {
      LOG.info("               # Tests pass: " + 0);
      LOG.info("               # Tests fail: " + 0);
    }
    else 
    {
      LOG.info("               # Tests pass: " + totalPass +
          " (" + (100 * totalPass / (totalPass + totalFail)) + "%)");
      LOG.info("               # Tests fail: " + totalFail + 
          " (" + (100 * totalFail / (totalPass + totalFail)) + "%)");
    }
    
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
  private boolean compareTestOutput(ComparatorData compdata, Result cmdResult) {
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
        compareOutput = comp.compare(cmdResult.getCommandOutput(), 
          expandCommand(compdata.getExpectedOutput()));
      } catch (Exception e) {
        LOG.info("Error in instantiating the comparator" + e);
      }
    }
    
    return compareOutput;
  }
  
  /***********************************
   ************* TESTS RUNNER
   *********************************/
  
  public void testAll() {
    assertTrue("Number of tests has to be greater then zero",
      testsFromConfigFile.size() > 0);
    LOG.info("TestAll");
    // Run the tests defined in the testConf.xml config file.
    for (int index = 0; index < testsFromConfigFile.size(); index++) {
      
      CLITestData testdata = testsFromConfigFile.get(index);
   
      // Execute the test commands
      ArrayList<CLICommand> testCommands = testdata.getTestCommands();
      Result cmdResult = null;
      for (CLICommand cmd : testCommands) {
      try {
        cmdResult = execute(cmd);
      } catch (Exception e) {
        fail(StringUtils.stringifyException(e));
      }
      }
      
      boolean overallTCResult = true;
      // Run comparators
      ArrayList<ComparatorData> compdata = testdata.getComparatorData();
      for (ComparatorData cd : compdata) {
        final String comptype = cd.getComparatorType();
        
        boolean compareOutput = false;
        
        if (! comptype.equalsIgnoreCase("none")) {
          compareOutput = compareTestOutput(cd, cmdResult);
          overallTCResult &= compareOutput;
        }
        
        cd.setExitCode(cmdResult.getExitCode());
        cd.setActualOutput(cmdResult.getCommandOutput());
        cd.setTestResult(compareOutput);
      }
      testdata.setTestResult(overallTCResult);
      
      // Execute the cleanup commands
      ArrayList<CLICommand> cleanupCommands = testdata.getCleanupCommands();
      for (CLICommand cmd : cleanupCommands) {
      try { 
        execute(cmd);
      } catch (Exception e) {
        fail(StringUtils.stringifyException(e));
      }
      }
    }
  }

  /**
   * this method has to be overridden by an ancestor
   */
  protected CommandExecutor.Result execute(CLICommand cmd) throws Exception {
    throw new Exception("Unknown type of test command:"+ cmd.getType());
  }
  
  /*
   * Parser class for the test config xml file
   */
  class TestConfigFileParser extends DefaultHandler {
    String charString = null;
    CLITestData td = null;
    ArrayList<CLICommand> testCommands = null;
    ArrayList<CLICommand> cleanupCommands = null;
    boolean runOnWindows = true;
    
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
        testCommands = new ArrayList<CLICommand>();
      } else if (qName.equals("cleanup-commands")) {
        cleanupCommands = new ArrayList<CLICommand>();
      } else if (qName.equals("comparators")) {
        testComparators = new ArrayList<ComparatorData>();
      } else if (qName.equals("comparator")) {
        comparatorData = new ComparatorData();
      }
      charString = "";
    }
    
    @Override
    public void endElement(String uri, String localName,String qName)
        throws SAXException {
      if (qName.equals("description")) {
        td.setTestDesc(charString);
      } else if (qName.equals("windows")) {
          runOnWindows = Boolean.parseBoolean(charString);
      } else if (qName.equals("test-commands")) {
        td.setTestCommands(testCommands);
        testCommands = null;
      } else if (qName.equals("cleanup-commands")) {
        td.setCleanupCommands(cleanupCommands);
        cleanupCommands = null;
      } else if (qName.equals("command")) {
        if (testCommands != null) {
          testCommands.add(new CLITestCmd(charString, new CLICommandFS()));
        } else if (cleanupCommands != null) {
          cleanupCommands.add(new CLITestCmd(charString, new CLICommandFS()));
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
        if (!Shell.WINDOWS || runOnWindows) {
          testsFromConfigFile.add(td);
        }
        td = null;
        runOnWindows = true;
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
