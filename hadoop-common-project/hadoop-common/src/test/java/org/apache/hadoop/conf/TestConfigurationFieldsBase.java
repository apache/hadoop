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

import org.apache.commons.lang.StringUtils;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertTrue;

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
   * Member variable to store Configuration variables for later reference.
   */
  private Map<String,String> configurationDefaultVariables = null;

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
   * A set of strings used to check for collision of default values.
   * For each of the set's strings, the default values containing that string
   * in their name should not coincide.
   */
  @SuppressWarnings("checkstyle:visibilitymodifier")
  protected Set<String> filtersForDefaultValueCollisionCheck = new HashSet<>();

  /**
   * Member variable for debugging base class operation
   */
  protected boolean configDebug = false;
  protected boolean xmlDebug = false;
  protected boolean defaultDebug = false;

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
    String propRegex = "^[A-Za-z][A-Za-z0-9_-]+(\\.[A-Za-z0-9_-]+)+$";
    Pattern p = Pattern.compile(propRegex);

    // Iterate through class member variables
    int totalFields = 0;
    String value;
    for (Field f : fields) {
      if (configDebug) {
        System.out.println("Field: " + f);
      }
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

      // filter out default-value fields
      if (isFieldADefaultValue(f)) {
        continue;
      }

      // Convert found member into String
      try {
        value = (String) f.get(null);
      } catch (IllegalAccessException iaException) {
        continue;
      }
      if (configDebug) {
        System.out.println("  Value: " + value);
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
        if (configDebug) {
          System.out.println("  Passes Regex: false");
        }
        continue;
      }
      if (configDebug) {
        System.out.println("  Passes Regex: true");
      }

      // Save member variable/value as hash
      if (!retVal.containsKey(value)) {
        retVal.put(value,f.getName());
      } else {
        if (configDebug) {
          System.out.println("ERROR: Already found key for property " + value);
        }
      }
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
          if (xmlDebug) {
            System.out.println("  Skipping Full Key: " + key);
          }
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
        if (xmlDebug) {
          System.out.println("  Skipping Prefix Key: " + key);
        }
        continue;
      }
      if (conf.onlyKeyExists(key)) {
        retVal.put(key,null);
        if (xmlDebug) {
          System.out.println("  XML Key,Null Value: " + key);
        }
      } else {
        String value = conf.get(key);
        if (value!=null) {
          retVal.put(key,entry.getValue());
          if (xmlDebug) {
            System.out.println("  XML Key,Valid Value: " + key);
          }
        }
      }
      kvItr.remove();
    }
    return retVal;
  }

  /**
   * Test if a field is a default value of another property by
   * checking if its name starts with "DEFAULT_" or ends with
   * "_DEFAULT".
   * @param field the field to check
   */
  private static boolean isFieldADefaultValue(Field field) {
    return field.getName().startsWith("DEFAULT_") ||
        field.getName().endsWith("_DEFAULT");
  }

  /**
   * Utility function to extract &quot;public static final&quot; default
   * member variables from a Configuration type class.
   *
   * @param fields The class member variables
   * @return HashMap containing <DefaultVariableName,DefaultValue> entries
   */
  private HashMap<String,String>
      extractDefaultVariablesFromConfigurationFields(Field[] fields) {
    // Sanity Check
    if (fields==null) {
      return null;
    }

    HashMap<String,String> retVal = new HashMap<String,String>();

    // Setup regexp for valid properties
    String propRegex = "^[A-Za-z][A-Za-z0-9_-]+(\\.[A-Za-z0-9_-]+)+$";
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
      // Special: Stuff any property beginning with "DEFAULT_" into a
      // different hash for later processing
      if (isFieldADefaultValue(f)) {
        if (retVal.containsKey(f.getName())) {
          continue;
        }
        try {
          if (f.getType().getName().equals("java.lang.String")) {
            String sValue = (String) f.get(null);
            retVal.put(f.getName(),sValue);
          } else if (f.getType().getName().equals("short")) {
            short shValue = (short) f.get(null);
            retVal.put(f.getName(),Integer.toString(shValue));
          } else if (f.getType().getName().equals("int")) {
            int iValue = (int) f.get(null);
            retVal.put(f.getName(),Integer.toString(iValue));
          } else if (f.getType().getName().equals("long")) {
            long lValue = (long) f.get(null);
            retVal.put(f.getName(),Long.toString(lValue));
          } else if (f.getType().getName().equals("float")) {
            float fValue = (float) f.get(null);
            retVal.put(f.getName(),Float.toString(fValue));
          } else if (f.getType().getName().equals("double")) {
            double dValue = (double) f.get(null);
            retVal.put(f.getName(),Double.toString(dValue));
          } else if (f.getType().getName().equals("boolean")) {
            boolean bValue = (boolean) f.get(null);
            retVal.put(f.getName(),Boolean.toString(bValue));
          } else {
            if (defaultDebug) {
              System.out.println("Config variable " + f.getName() + " has unknown type " + f.getType().getName());
            }
          }
        } catch (IllegalAccessException iaException) {
          iaException.printStackTrace();
        }
      }
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
    if (configDebug) {
      System.out.println("Reading configuration classes");
      System.out.println("");
    }
    for (Class c : configurationClasses) {
      Field[] fields = c.getDeclaredFields();
      Map<String,String> memberMap =
          extractMemberVariablesFromConfigurationFields(fields);
      if (memberMap!=null) {
        configurationMemberVariables.putAll(memberMap);
      }
    }
    if (configDebug) {
      System.out.println("");
      System.out.println("=====");
      System.out.println("");
    }

    // Create XML key/value map
    if (xmlDebug) {
      System.out.println("Reading XML property files");
      System.out.println("");
    }
    xmlKeyValueMap = extractPropertiesFromXml(xmlFilename);
    if (xmlDebug) {
      System.out.println("");
      System.out.println("=====");
      System.out.println("");
    }

    // Create default configuration variable key/value map
    if (defaultDebug) {
      System.out.println("Reading Config property files for defaults");
      System.out.println("");
    }
    configurationDefaultVariables = new HashMap<String,String>();
    for (Class c : configurationClasses) {
      Field[] fields = c.getDeclaredFields();
      Map<String,String> defaultMap =
          extractDefaultVariablesFromConfigurationFields(fields);
      if (defaultMap!=null) {
        configurationDefaultVariables.putAll(defaultMap);
      }
    }
    if (defaultDebug) {
      System.out.println("");
      System.out.println("=====");
      System.out.println("");
    }

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

  /**
   * For each property in the XML file, verify that the value matches
   * up to the default if one exists.
   */
  @Test
  public void testXmlAgainstDefaultValuesInConfigurationClass() {
    // Error if subclass hasn't set class members
    assertTrue(xmlFilename!=null);
    assertTrue(configurationMemberVariables!=null);
    assertTrue(configurationDefaultVariables!=null);

    HashSet<String> xmlPropertiesWithEmptyValue = new HashSet<String>();
    HashSet<String> configPropertiesWithNoDefaultConfig = new HashSet<String>();
    HashMap<String,String> xmlPropertiesMatchingConfigDefault =
        new HashMap<String,String>();
    // Ugly solution.  Should have tuple-based solution.
    HashMap<HashMap<String,String>,HashMap<String,String>> mismatchingXmlConfig =
        new HashMap<HashMap<String,String>,HashMap<String,String>>();

    for (Map.Entry<String,String> xEntry : xmlKeyValueMap.entrySet()) {
      String xmlProperty = xEntry.getKey();
      String xmlDefaultValue = xEntry.getValue();
      String configProperty = configurationMemberVariables.get(xmlProperty);
      if (configProperty!=null) {
        String defaultConfigName = null;
        String defaultConfigValue = null;

        // Type 1: Prepend DEFAULT_
        String defaultNameCheck1 = "DEFAULT_" + configProperty;
        String defaultValueCheck1 = configurationDefaultVariables
            .get(defaultNameCheck1);
        // Type 2: Swap _KEY suffix with _DEFAULT suffix
        String defaultNameCheck2 = null;
        if (configProperty.endsWith("_KEY")) {
          defaultNameCheck2 = configProperty
              .substring(0,configProperty.length()-4) + "_DEFAULT";
        }
        String defaultValueCheck2 = configurationDefaultVariables
            .get(defaultNameCheck2);
        // Type Last: Append _DEFAULT suffix
        String defaultNameCheck3 = configProperty + "_DEFAULT";
        String defaultValueCheck3 = configurationDefaultVariables
            .get(defaultNameCheck3);

        // Pick the default value that exists
        if (defaultValueCheck1!=null) {
          defaultConfigName = defaultNameCheck1;
          defaultConfigValue = defaultValueCheck1;
        } else if (defaultValueCheck2!=null) {
          defaultConfigName = defaultNameCheck2;
          defaultConfigValue = defaultValueCheck2;
        } else if (defaultValueCheck3!=null) {
          defaultConfigName = defaultNameCheck3;
          defaultConfigValue = defaultValueCheck3;
        }

        if (defaultConfigValue!=null) {
          if (xmlDefaultValue==null) {
            xmlPropertiesWithEmptyValue.add(xmlProperty);
          } else if (!xmlDefaultValue.equals(defaultConfigValue)) {
            HashMap<String,String> xmlEntry =
                new HashMap<String,String>();
            xmlEntry.put(xmlProperty,xmlDefaultValue);
            HashMap<String,String> configEntry =
                new HashMap<String,String>();
            configEntry.put(defaultConfigName,defaultConfigValue);
            mismatchingXmlConfig.put(xmlEntry,configEntry);
           } else {
            xmlPropertiesMatchingConfigDefault
                .put(xmlProperty, defaultConfigName);
          }
        } else {
          configPropertiesWithNoDefaultConfig.add(configProperty);
        }
      } else {
      }
    }

    // Print out any unknown mismatching XML value/Config default value
    System.out.println(this.xmlFilename + " has " +
        mismatchingXmlConfig.size() +
        " properties that do not match the default Config value");
    if (mismatchingXmlConfig.size()==0) {
      System.out.println("  (None)");
    } else {
       for (Map.Entry<HashMap<String,String>,HashMap<String,String>> xcEntry :
           mismatchingXmlConfig.entrySet()) {
         HashMap<String,String> xmlMap = xcEntry.getKey();
         HashMap<String,String> configMap = xcEntry.getValue();
         for (Map.Entry<String,String> xmlEntry : xmlMap.entrySet()) {
           System.out.println("  XML Property: " + xmlEntry.getKey());
           System.out.println("  XML Value:    " + xmlEntry.getValue());
         }
         for (Map.Entry<String,String> configEntry : configMap.entrySet()) {
           System.out.println("  Config Name:  " + configEntry.getKey());
           System.out.println("  Config Value: " + configEntry.getValue());
         }
         System.out.println("");
       }
    }
    System.out.println();

    // Print out Config properties that have no corresponding DEFAULT_*
    // variable and cannot do any XML comparison (i.e. probably needs to
    // be checked by hand)
    System.out.println("Configuration(s) have " +
        configPropertiesWithNoDefaultConfig.size() +
        " properties with no corresponding default member variable.  These" +
        " will need to be verified manually.");
    if (configPropertiesWithNoDefaultConfig.size()==0) {
      System.out.println("  (None)");
    } else {
      Iterator<String> cItr = configPropertiesWithNoDefaultConfig.iterator();
      while (cItr.hasNext()) {
        System.out.println("  " + cItr.next());
      }
    }
    System.out.println();

    // MAYBE TODO Print out any known mismatching XML value/Config default

    // Print out XML properties that have empty values (i.e. should result
    // in code-based default)
    System.out.println(this.xmlFilename + " has " +
        xmlPropertiesWithEmptyValue.size() + " properties with empty values");
    if (xmlPropertiesWithEmptyValue.size()==0) {
      System.out.println("  (None)");
    } else {
      Iterator<String> xItr = xmlPropertiesWithEmptyValue.iterator();
      while (xItr.hasNext()) {
        System.out.println("  " + xItr.next());
      }
    }
    System.out.println();

    // Print out any matching XML value/Config default value
    System.out.println(this.xmlFilename + " has " +
        xmlPropertiesMatchingConfigDefault.size() +
        " properties which match a corresponding Config variable");
    if (xmlPropertiesMatchingConfigDefault.size()==0) {
      System.out.println("  (None)");
    } else {
      for (Map.Entry<String,String> xcEntry :
          xmlPropertiesMatchingConfigDefault.entrySet()) {
        System.out.println("  " + xcEntry.getKey() + " / " +
            xcEntry.getValue());
      }
    }
    System.out.println();

    // Test separator
    System.out.println();
    System.out.println("=====");
    System.out.println();
  }

  /**
   * For each specified string, get the default parameter values whose names
   * contain the string. Then check whether any of these default values collide.
   * This is, for example, useful to make sure there is no collision of default
   * ports across different services.
   */
  @Test
  public void testDefaultValueCollision() {
    for (String filter : filtersForDefaultValueCollisionCheck) {
      System.out.println("Checking if any of the default values whose name " +
          "contains string \"" + filter + "\" collide.");

      // Map from filtered default value to name of the corresponding parameter.
      Map<String, String> filteredValues = new HashMap<>();

      int valuesChecked = 0;
      for (Map.Entry<String, String> ent :
          configurationDefaultVariables.entrySet()) {
        // Apply the name filter to the default parameters.
        if (ent.getKey().contains(filter)) {
          // Check only for numerical values.
          if (StringUtils.isNumeric(ent.getValue())) {
            String crtValue =
                filteredValues.putIfAbsent(ent.getValue(), ent.getKey());
            assertTrue("Parameters " + ent.getKey() + " and " + crtValue +
                " are using the same default value!", crtValue == null);
          }
          valuesChecked++;
        }
      }

      System.out.println(
          "Checked " + valuesChecked + " default values for collision.");
    }


  }
}
