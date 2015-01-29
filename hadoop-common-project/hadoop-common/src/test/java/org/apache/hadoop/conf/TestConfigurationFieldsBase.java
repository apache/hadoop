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

package org.apache.hadoop.conf;

import java.lang.Class;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;

/**
 * Base class for comparing fields in one or more Configuration classes
 * against a corresponding .xml file.  Usage is intended as follows:
 * <p></p>
 * <ol>
 * <li> Create a subclass to TestConfigurationFieldsBase
 * <li> Define <code>initializeMemberVariables</code> method in the
 *      subclass.  In this class, do the following:
 * <p></p>
 *   <ol>
 *   <li> <b>Required</b> Set the variable <code>xmlFilename</code> to
 *        the appropriate xml definition file
 *   <li> <b>Required</b> Set the variable <code>configurationClasses</code>
 *        to an array of the classes which define the constants used by the
 *        code corresponding to the xml files
 *   <li> <b>Optional</b> Set <code>errorIfMissingConfigProps</code> if the
 *        subclass should throw an error in the method
 *        <code>testCompareXmlAgainstConfigurationClass</code>
 *   <li> <b>Optional</b> Set <code>errorIfMissingXmlProps</code> if the
 *        subclass should throw an error in the method
 *        <code>testCompareConfigurationClassAgainstXml</code>
 *   <li> <b>Optional</b> Instantiate and populate strings into one or
 *        more of the following variables:
 *        <br><code>configurationPropsToSkipCompare</code>
 *        <br><code>configurationPrefixToSkipCompare</code>
 *        <br><code>xmlPropsToSkipCompare</code>
 *        <br><code>xmlPrefixToSkipCompare</code>
 *        <br>
 *        in order to get comparisons clean
 *   </ol>
 * </ol>
 * <p></p>
 * The tests to do class-to-file and file-to-class should automatically
 * run.  This class (and its subclasses) are mostly not intended to be
 * overridden, but to do a very specific form of comparison testing.
 */
@Ignore
public abstract class TestConfigurationFieldsBase {

  /**
   * Member variable for storing xml filename.
   */
  protected String xmlFilename = null;

  /**
   * Member variable for storing all related Configuration classes.
   */
  protected Class[] configurationClasses = null;

  /**
   * Throw error during comparison if missing configuration properties.
   * Intended to be set by subclass.
   */
  protected boolean errorIfMissingConfigProps = false;

  /**
   * Throw error during comparison if missing xml properties.  Intended
   * to be set by subclass.
   */
  protected boolean errorIfMissingXmlProps = false;

  /**
   * Set of properties to skip extracting (and thus comparing later) in 
   * extractMemberVariablesFromConfigurationFields.
   */
  protected Set<String> configurationPropsToSkipCompare = null;

  /**
   * Set of property prefixes to skip extracting (and thus comparing later)
   * in * extractMemberVariablesFromConfigurationFields.
   */
  protected Set<String> configurationPrefixToSkipCompare = null;

  /**
   * Set of properties to skip extracting (and thus comparing later) in 
   * extractPropertiesFromXml.
   */
  protected Set<String> xmlPropsToSkipCompare = null;

  /**
   * Set of property prefixes to skip extracting (and thus comparing later)
   * in extractPropertiesFromXml.
   */
  protected Set<String> xmlPrefixToSkipCompare = null;

  /**
   * Member variable to store Configuration variables for later comparison.
   */
  private Map<String,String> configurationMemberVariables = null;

  /**
   * Member variable to store XML properties for later comparison.
   */
  private Map<String,String> xmlKeyValueMap = null;

  /**
   * Member variable to store Configuration variables that are not in the
   * corresponding XML file.
   */
  private Set<String> configurationFieldsMissingInXmlFile = null;

  /**
   * Member variable to store XML variables that are not in the
   * corresponding Configuration class(es).
   */
  private Set<String> xmlFieldsMissingInConfiguration = null;

  /**
   * Abstract method to be used by subclasses for initializing base
   * members.
   */
  public abstract void initializeMemberVariables();
 
  /**
   * Utility function to extract &quot;public static final&quot; member
   * variables from a Configuration type class.
   *
   * @param fields The class member variables
   * @return HashMap containing <StringValue,MemberVariableName> entries
   */
  private HashMap<String,String>
      extractMemberVariablesFromConfigurationFields(Field[] fields) {
    // Sanity Check
    if (fields==null)
      return null;

    HashMap<String,String> retVal = new HashMap<String,String>();

    // Setup regexp for valid properties
    String propRegex = "^[A-Za-z_-]+(\\.[A-Za-z_-]+)+$";
    Pattern p = Pattern.compile(propRegex);

    // Iterate through class member variables
    int totalFields = 0;
    String value;
    for (Field f : fields) {
      // Filter out anything that isn't "public static final"
      if (!Modifier.isStatic(f.getModifiers()) ||
          !Modifier.isPublic(f.getModifiers()) ||
          !Modifier.isFinal(f.getModifiers())) {
        continue;
      }
      // Filter out anything that isn't a string.  int/float are generally
      // default values
      if (!f.getType().getName().equals("java.lang.String")) {
        continue;
      }
      // Convert found member into String
      try {
        value = (String) f.get(null);
      } catch (IllegalAccessException iaException) {
        continue;
      }
      // Special Case: Detect and ignore partial properties (ending in x)
      //               or file properties (ending in .xml)
      if (value.endsWith(".xml") ||
          value.endsWith(".")    ||
          value.endsWith("-"))
        continue;
      // Ignore known configuration props
      if (configurationPropsToSkipCompare != null) {
        if (configurationPropsToSkipCompare.contains(value)) {
          continue;
        }
      }
      // Ignore known configuration prefixes
      boolean skipPrefix = false;
      if (configurationPrefixToSkipCompare != null) {
        for (String cfgPrefix : configurationPrefixToSkipCompare) {
	  if (value.startsWith(cfgPrefix)) {
            skipPrefix = true;
            break;
	  }
	}
      }
      if (skipPrefix) {
        continue;
      }
      // Positive Filter: Look only for property values.  Expect it to look
      //                  something like: blah.blah2(.blah3.blah4...)
      Matcher m = p.matcher(value);
      if (!m.find()) {
        continue;
      }

      // Save member variable/value as hash
      retVal.put(value,f.getName());
    }

    return retVal;
  }

  /**
   * Pull properties and values from filename.
   *
   * @param filename XML filename
   * @return HashMap containing <Property,Value> entries from XML file
   */
  private HashMap<String,String> extractPropertiesFromXml
      (String filename) {
    if (filename==null) {
      return null;
    }

    // Iterate through XML file for name/value pairs
    Configuration conf = new Configuration(false);
    conf.setAllowNullValueProperties(true);
    conf.addResource(filename);

    HashMap<String,String> retVal = new HashMap<String,String>();
    Iterator<Map.Entry<String,String>> kvItr = conf.iterator();
    while (kvItr.hasNext()) {
      Map.Entry<String,String> entry = kvItr.next();
      String key = entry.getKey();
      // Ignore known xml props
      if (xmlPropsToSkipCompare != null) {
        if (xmlPropsToSkipCompare.contains(key)) {
          continue;
        }
      }
      // Ignore known xml prefixes
      boolean skipPrefix = false;
      if (xmlPrefixToSkipCompare != null) {
        for (String xmlPrefix : xmlPrefixToSkipCompare) {
	  if (key.startsWith(xmlPrefix)) {
	    skipPrefix = true;
            break;
	  }
	}
      }
      if (skipPrefix) {
        continue;
      }
      if (conf.onlyKeyExists(key)) {
        retVal.put(key,null);
      } else {
        String value = conf.get(key);
        if (value!=null) {
          retVal.put(key,entry.getValue());
        }
      }
      kvItr.remove();
    }
    return retVal;
  }

  /**
   * Perform set difference operation on keyMap2 from keyMap1.
   *
   * @param keyMap1 The initial set
   * @param keyMap2 The set to subtract
   * @return Returns set operation keyMap1-keyMap2
   */
  private static Set<String> compareConfigurationToXmlFields(Map<String,String> keyMap1, Map<String,String> keyMap2) {
    Set<String> retVal = new HashSet<String>(keyMap1.keySet());
    retVal.removeAll(keyMap2.keySet());
    return retVal;
  }

  /**
   * Initialize the four variables corresponding the Configuration
   * class and the XML properties file.
   */
  @Before
  public void setupTestConfigurationFields() throws Exception {
    initializeMemberVariables();

    // Error if subclass hasn't set class members
    assertTrue(xmlFilename!=null);
    assertTrue(configurationClasses!=null);

    // Create class member/value map
    configurationMemberVariables = new HashMap<String,String>();
    for (Class c : configurationClasses) {
      Field[] fields = c.getDeclaredFields();
      Map<String,String> memberMap =
          extractMemberVariablesFromConfigurationFields(fields);
      if (memberMap!=null) {
        configurationMemberVariables.putAll(memberMap);
      }
    }

    // Create XML key/value map
    xmlKeyValueMap = extractPropertiesFromXml(xmlFilename);

    // Find class members not in the XML file
    configurationFieldsMissingInXmlFile = compareConfigurationToXmlFields
        (configurationMemberVariables, xmlKeyValueMap);

    // Find XML properties not in the class
    xmlFieldsMissingInConfiguration = compareConfigurationToXmlFields
        (xmlKeyValueMap, configurationMemberVariables);
  }

  /**
   * Compares the properties that are in the Configuration class, but not
   * in the XML properties file.
   */
  @Test
  public void testCompareConfigurationClassAgainstXml() {
    // Error if subclass hasn't set class members
    assertTrue(xmlFilename!=null);
    assertTrue(configurationClasses!=null);

    final int missingXmlSize = configurationFieldsMissingInXmlFile.size();

    for (Class c : configurationClasses) {
      System.out.println(c);
    }
    System.out.println("  (" + configurationMemberVariables.size() + " member variables)");
    System.out.println();
    StringBuffer xmlErrorMsg = new StringBuffer();
    for (Class c : configurationClasses) {
      xmlErrorMsg.append(c);
      xmlErrorMsg.append(" ");
    }
    xmlErrorMsg.append("has ");
    xmlErrorMsg.append(missingXmlSize);
    xmlErrorMsg.append(" variables missing in ");
    xmlErrorMsg.append(xmlFilename);
    System.out.println(xmlErrorMsg.toString());
    System.out.println();
    if (missingXmlSize==0) {
      System.out.println("  (None)");
    } else {
      for (String missingField : configurationFieldsMissingInXmlFile) {
        System.out.println("  " + missingField);
      }
    }
    System.out.println();
    System.out.println("=====");
    System.out.println();
    if (errorIfMissingXmlProps) {
      assertTrue(xmlErrorMsg.toString(), missingXmlSize==0);
    }
  }

  /**
   * Compares the properties that are in the XML properties file, but not
   * in the Configuration class.
   */
  @Test
  public void testCompareXmlAgainstConfigurationClass() {
    // Error if subclass hasn't set class members
    assertTrue(xmlFilename!=null);
    assertTrue(configurationClasses!=null);

    final int missingConfigSize = xmlFieldsMissingInConfiguration.size();

    System.out.println("File " + xmlFilename + " (" + xmlKeyValueMap.size() + " properties)");
    System.out.println();
    StringBuffer configErrorMsg = new StringBuffer();
    configErrorMsg.append(xmlFilename);
    configErrorMsg.append(" has ");
    configErrorMsg.append(missingConfigSize);
    configErrorMsg.append(" properties missing in");
    for (Class c : configurationClasses) {
      configErrorMsg.append("  " + c);
    }
    System.out.println(configErrorMsg.toString());
    System.out.println();
    if (missingConfigSize==0) {
      System.out.println("  (None)");
    } else {
      for (String missingField : xmlFieldsMissingInConfiguration) {
        System.out.println("  " + missingField);
      }
    }
    System.out.println();
    System.out.println("=====");
    System.out.println();
    if ( errorIfMissingConfigProps ) {
      assertTrue(configErrorMsg.toString(), missingConfigSize==0);
    }
  }
}
