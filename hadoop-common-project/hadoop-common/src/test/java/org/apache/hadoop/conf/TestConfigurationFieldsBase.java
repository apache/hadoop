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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.test.ReflectionUtils;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

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

  private static final Logger LOG = LoggerFactory.getLogger(
      TestConfigurationFieldsBase.class);

  private static final Logger LOG_CONFIG = LoggerFactory.getLogger(
      "org.apache.hadoop.conf.TestConfigurationFieldsBase.config");

  private static final Logger LOG_XML = LoggerFactory.getLogger(
      "org.apache.hadoop.conf.TestConfigurationFieldsBase.xml");
  private static final String VALID_PROP_REGEX = "^[A-Za-z][A-Za-z0-9_-]+(\\.[A-Za-z%s0-9_-]+)+$";
  private static final Pattern validPropertiesPattern = Pattern.compile(VALID_PROP_REGEX);

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
   * {@link #extractMemberVariablesFromConfigurationFields(Field[])}.
   */
  protected Set<String> configurationPropsToSkipCompare = new HashSet<>();

  /**
   * Set of property prefixes to skip extracting (and thus comparing later)
   * in * extractMemberVariablesFromConfigurationFields.
   */
  protected Set<String> configurationPrefixToSkipCompare = new HashSet<>();

  /**
   * Set of properties to skip extracting (and thus comparing later) in 
   * extractPropertiesFromXml.
   */
  protected Set<String> xmlPropsToSkipCompare = new HashSet<>();

  /**
   * Set of property prefixes to skip extracting (and thus comparing later)
   * in extractPropertiesFromXml.
   */
  protected Set<String> xmlPrefixToSkipCompare = new HashSet<>();

  /**
   * Member variable to store Configuration variables for later comparison.
   */
  private Map<String, String> configurationMemberVariables = null;

  /**
   * Member variable to store Configuration variables for later reference.
   */
  private Map<String, String> configurationDefaultVariables = null;

  /**
   * Member variable to store XML properties for later comparison.
   */
  private Map<String, String> xmlKeyValueMap = null;

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
   * Abstract method to be used by subclasses for initializing base
   * members.
   */
  public abstract void initializeMemberVariables();
 
  /**
   * Utility function to extract &quot;public static final&quot; member
   * variables from a Configuration type class.
   *
   * @param fields The class member variables
   * @return HashMap containing (StringValue, MemberVariableName) entries
   */
  private Map<String, String>
      extractMemberVariablesFromConfigurationFields(Field[] fields) {
    // Sanity Check
    if (fields == null) {
      return null;
    }

    Map<String, String> validConfigProperties = new HashMap<>();


    // Iterate through class member variables
    String value;
    Set<String> fieldsNotPassedRegex = new HashSet<>();
    for (Field f : fields) {
      LOG_CONFIG.debug("Field: {}", f);
      // Filter out anything that isn't "public static final"
      if (!Modifier.isStatic(f.getModifiers()) ||
          !Modifier.isPublic(f.getModifiers()) ||
          !Modifier.isFinal(f.getModifiers())) {
        LOG_CONFIG.debug("  Is skipped as it is not public static final");
        continue;
      }
      // Filter out anything that isn't a string.  int/float are generally
      // default values
      if (!f.getType().getName().equals("java.lang.String")) {
        LOG_CONFIG.debug("  Is skipped as it is not type of String");
        continue;
      }

      // filter out default-value fields
      if (isFieldADefaultValue(f)) {
        LOG_CONFIG.debug("  Is skipped as it is a 'default value field'");
        continue;
      }

      // Convert found member into String
      try {
        value = (String) f.get(null);
      } catch (IllegalAccessException iaException) {
        LOG_CONFIG.debug("  Is skipped as it cannot be converted to a String");
        continue;
      }
      LOG_CONFIG.debug("  Value: {}", value);
      // Special Case: Detect and ignore partial properties (ending in x)
      //               or file properties (ending in .xml)
      if (value.endsWith(".xml") ||
          value.endsWith(".")    ||
          value.endsWith("-")) {
        LOG_CONFIG.debug("  Is skipped as it a 'partial property'");
        continue;
      }
      // Ignore known configuration props
      if (configurationPropsToSkipCompare.contains(value)) {
        LOG_CONFIG.debug("  Is skipped as it is registered as a property to be skipped");
        continue;
      }
      // Ignore known configuration prefixes
      boolean skipPrefix = false;
      for (String cfgPrefix : configurationPrefixToSkipCompare) {
        if (value.startsWith(cfgPrefix)) {
          skipPrefix = true;
          LOG_CONFIG.debug("  Is skipped as it is starts with a " +
              "registered property prefix to skip: {}", cfgPrefix);
          break;
        }
      }
      if (skipPrefix) {
        continue;
      }
      // Positive Filter: Look only for property values.  Expect it to look
      //                  something like: blah.blah2(.blah3.blah4...)
      Matcher m = validPropertiesPattern.matcher(value);
      if (!m.find()) {
        LOG_CONFIG.debug("  Passes Regex: false");
        fieldsNotPassedRegex.add(f.getName());
        continue;
      }
      LOG_CONFIG.debug("  Passes Regex: true");

      if (!validConfigProperties.containsKey(value)) {
        validConfigProperties.put(value, f.getName());
      } else {
        LOG_CONFIG.debug("ERROR: Already found key for property " + value);
      }
    }

    LOG_CONFIG.debug("Listing fields did not pass regex pattern: {}", fieldsNotPassedRegex);
    return validConfigProperties;
  }

  /**
   * Pull properties and values from filename.
   *
   * @param filename XML filename
   * @return HashMap containing &lt;Property,Value&gt; entries from XML file
   */
  private Map<String, String> extractPropertiesFromXml(String filename) {
    if (filename == null) {
      return null;
    }

    // Iterate through XML file for name/value pairs
    Configuration conf = new Configuration(false);
    conf.setAllowNullValueProperties(true);
    conf.addResource(filename);

    Map<String, String> retVal = new HashMap<>();
    Iterator<Map.Entry<String, String>> kvItr = conf.iterator();
    while (kvItr.hasNext()) {
      Map.Entry<String, String> entry = kvItr.next();
      String key = entry.getKey();
      // Ignore known xml props
      if (xmlPropsToSkipCompare.contains(key)) {
        LOG_XML.debug("  Skipping Full Key: {}", key);
        continue;
      }
      // Ignore known xml prefixes
      if (xmlPrefixToSkipCompare.stream().anyMatch(key::startsWith)) {
        LOG_XML.debug("  Skipping Prefix Key: " + key);
        continue;
      }
      if (conf.onlyKeyExists(key)) {
        retVal.put(key, null);
        LOG_XML.debug("  XML Key, Null Value: " + key);
      } else {
        if (conf.get(key) != null) {
          retVal.put(key, entry.getValue());
          LOG_XML.debug("  XML Key, Valid Value: " + key);
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
   * @return HashMap containing (DefaultVariableName, DefaultValue) entries
   */
  private Map<String, String>
      extractDefaultVariablesFromConfigurationFields(Field[] fields) {
    // Sanity Check
    if (fields == null) {
      return null;
    }

    Map<String, String> retVal = new HashMap<>();

    // Setup regexp for valid properties

    // Iterate through class member variables
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
          String s = ReflectionUtils.getStringValueOfField(f);
          retVal.put(f.getName(), s);
        } catch (IllegalAccessException iaException) {
          LOG.error("{}", f, iaException);
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
  private static Set<String> compareConfigurationToXmlFields(
      Map<String, String> keyMap1, Map<String, String> keyMap2) {
    Set<String> retVal = new HashSet<>(keyMap1.keySet());
    retVal.removeAll(keyMap2.keySet());

    return retVal;
  }

  /**
   * Initialize the four variables corresponding the Configuration
   * class and the XML properties file.
   */
  @Before
  public void setupTestConfigurationFields() {
    initializeMemberVariables();

    // Error if subclass hasn't set class members
    assertNotNull("XML file name is null", xmlFilename);
    assertNotNull("Configuration classes array is null", configurationClasses);

    // Create class member/value map
    configurationMemberVariables = new HashMap<>();
    LOG_CONFIG.debug("Reading configuration classes\n");
    for (Class c : configurationClasses) {
      Field[] fields = c.getDeclaredFields();
      Map<String, String> memberMap =
          extractMemberVariablesFromConfigurationFields(fields);
      if (memberMap != null) {
        configurationMemberVariables.putAll(memberMap);
      }
    }
    LOG_CONFIG.debug("\n=====\n");

    // Create XML key/value map
    LOG_XML.debug("Reading XML property files\n");
    xmlKeyValueMap = extractPropertiesFromXml(xmlFilename);
    LOG_XML.debug("\n=====\n");

    // Create default configuration variable key/value map
    LOG.debug("Reading Config property files for defaults\n");
    configurationDefaultVariables = new HashMap<>();
    Arrays.stream(configurationClasses)
        .map(Class::getDeclaredFields)
        .map(this::extractDefaultVariablesFromConfigurationFields)
        .filter(Objects::nonNull)
        .forEach(map -> configurationDefaultVariables.putAll(map));
    LOG.debug("\n=====\n");

    // Find class members not in the XML file
    configurationFieldsMissingInXmlFile = compareConfigurationToXmlFields(
        configurationMemberVariables, xmlKeyValueMap);

    // Find XML properties not in the class
    xmlFieldsMissingInConfiguration = compareConfigurationToXmlFields(
        xmlKeyValueMap, configurationMemberVariables);
  }

  /**
   * Compares the properties that are in the Configuration class, but not
   * in the XML properties file.
   */
  @Test
  public void testCompareConfigurationClassAgainstXml() {
    // Error if subclass hasn't set class members
    assertNotNull("XML file name is null", xmlFilename);
    assertNotNull("Configuration classes array is null", configurationClasses);

    final int missingXmlSize = configurationFieldsMissingInXmlFile.size();

    for (Class c : configurationClasses) {
      LOG.info("Configuration class: {}", c.toString());
    }
    LOG.info("({} member variables)\n", configurationMemberVariables.size());

    StringBuilder xmlErrorMsg = new StringBuilder();
    for (Class c : configurationClasses) {
      xmlErrorMsg.append(c);
      xmlErrorMsg.append(" ");
    }
    xmlErrorMsg.append("has ");
    xmlErrorMsg.append(missingXmlSize);
    xmlErrorMsg.append(" variables missing in ");
    xmlErrorMsg.append(xmlFilename);
    LOG.error(xmlErrorMsg.toString());

    if (missingXmlSize == 0) {
      LOG.info("  (None)");
    } else {
      appendMissingEntries(xmlErrorMsg, configurationFieldsMissingInXmlFile);
    }
    LOG.info("\n=====\n");
    if (errorIfMissingXmlProps) {
      assertEquals(xmlErrorMsg.toString(), 0, missingXmlSize);
    }
  }

  /**
   * Take a set of missing entries, sort, append to the string builder
   * and also log at INFO.
   * @param sb string builder
   * @param missing set of missing entries
   */
  private void appendMissingEntries(StringBuilder sb, Set<String> missing) {
    sb.append(" Entries: ");
    new TreeSet<>(missing).forEach(
        (s) -> {
          LOG.info("  {}", s);
          sb.append("  ").append(s);
        });
  }

  /**
   * Compares the properties that are in the XML properties file, but not
   * in the Configuration class.
   */
  @Test
  public void testCompareXmlAgainstConfigurationClass() {
    // Error if subclass hasn't set class members
    assertNotNull("XML file name is null", xmlFilename);
    assertNotNull("Configuration classes array is null", configurationClasses);

    final int missingConfigSize = xmlFieldsMissingInConfiguration.size();

    LOG.info("File {} ({} properties)", xmlFilename, xmlKeyValueMap.size());
    StringBuilder configErrorMsg = new StringBuilder();
    configErrorMsg.append(xmlFilename);
    configErrorMsg.append(" has ");
    configErrorMsg.append(missingConfigSize);
    configErrorMsg.append(" properties missing in");
    Arrays.stream(configurationClasses)
        .forEach(c -> configErrorMsg.append("  ").append(c));
    LOG.info(configErrorMsg.toString());
    if (missingConfigSize == 0) {
      LOG.info("  (None)");
    } else {
      appendMissingEntries(configErrorMsg, xmlFieldsMissingInConfiguration);
    }
    LOG.info("\n=====\n");
    if (errorIfMissingConfigProps) {
      assertEquals(configErrorMsg.toString(), 0, missingConfigSize);
    }
  }

  /**
   * For each property in the XML file, verify that the value matches
   * up to the default if one exists.
   */
  @Test
  public void testXmlAgainstDefaultValuesInConfigurationClass() {
    // Error if subclass hasn't set class members
    assertNotNull("XML file name is null", xmlFilename);
    assertNotNull("Configuration member variables is null", configurationMemberVariables);
    assertNotNull("Configuration default variables is null", configurationMemberVariables);

    Set<String> xmlPropertiesWithEmptyValue = new TreeSet<>();
    Set<String> configPropertiesWithNoDefaultConfig = new TreeSet<>();
    Map<String, String> xmlPropertiesMatchingConfigDefault = new HashMap<>();
    // Ugly solution.  Should have tuple-based solution.
    Map<Map<String, String>, Map<String, String>> mismatchingXmlConfig = new HashMap<>();

    for (Map.Entry<String, String> xEntry : xmlKeyValueMap.entrySet()) {
      String xmlProperty = xEntry.getKey();
      String xmlDefaultValue = xEntry.getValue();
      String configProperty = configurationMemberVariables.get(xmlProperty);
      if (configProperty != null) {
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
              .substring(0, configProperty.length() - 4) + "_DEFAULT";
        }
        String defaultValueCheck2 = configurationDefaultVariables
            .get(defaultNameCheck2);
        // Type Last: Append _DEFAULT suffix
        String defaultNameCheck3 = configProperty + "_DEFAULT";
        String defaultValueCheck3 = configurationDefaultVariables
            .get(defaultNameCheck3);

        // Pick the default value that exists
        if (defaultValueCheck1 != null) {
          defaultConfigName = defaultNameCheck1;
          defaultConfigValue = defaultValueCheck1;
        } else if (defaultValueCheck2 != null) {
          defaultConfigName = defaultNameCheck2;
          defaultConfigValue = defaultValueCheck2;
        } else if (defaultValueCheck3 != null) {
          defaultConfigName = defaultNameCheck3;
          defaultConfigValue = defaultValueCheck3;
        }

        if (defaultConfigValue != null) {
          if (xmlDefaultValue == null) {
            xmlPropertiesWithEmptyValue.add(xmlProperty);
          } else if (!xmlDefaultValue.equals(defaultConfigValue)) {
            Map<String, String> xmlEntry = new HashMap<>();
            xmlEntry.put(xmlProperty, xmlDefaultValue);
            Map<String, String> configEntry = new HashMap<>();
            configEntry.put(defaultConfigName, defaultConfigValue);
            mismatchingXmlConfig.put(xmlEntry, configEntry);
          } else {
            xmlPropertiesMatchingConfigDefault
                .put(xmlProperty, defaultConfigName);
          }
        } else {
          configPropertiesWithNoDefaultConfig.add(configProperty);
        }
      }
    }

    // Print out any unknown mismatching XML value/Config default value
    LOG.info("{} has {} properties that do not match the default Config value",
        xmlFilename, mismatchingXmlConfig.size());
    if (mismatchingXmlConfig.isEmpty()) {
      LOG.info("  (None)");
    } else {
      for (Map.Entry<Map<String, String>, Map<String, String>> xcEntry :
          mismatchingXmlConfig.entrySet()) {
        xcEntry.getKey().forEach((key, value) -> {
          LOG.info("XML Property: {}", key);
          LOG.info("XML Value:    {}", value);
        });
        xcEntry.getValue().forEach((key, value) -> {
          LOG.info("Config Name:  {}", key);
          LOG.info("Config Value: {}", value);
        });
        LOG.info("");
      }
    }
    LOG.info("\n");

    // Print out Config properties that have no corresponding DEFAULT_*
    // variable and cannot do any XML comparison (i.e. probably needs to
    // be checked by hand)
    LOG.info("Configuration(s) have {} " +
        " properties with no corresponding default member variable.  These" +
        " will need to be verified manually.",
        configPropertiesWithNoDefaultConfig.size());
    if (configPropertiesWithNoDefaultConfig.isEmpty()) {
      LOG.info("  (None)");
    } else {
      configPropertiesWithNoDefaultConfig.forEach(c -> LOG.info(" {}", c));
    }
    LOG.info("\n");

    // MAYBE TODO Print out any known mismatching XML value/Config default

    // Print out XML properties that have empty values (i.e. should result
    // in code-based default)
    LOG.info("{} has {} properties with empty values",
        xmlFilename, xmlPropertiesWithEmptyValue.size());
    if (xmlPropertiesWithEmptyValue.isEmpty()) {
      LOG.info("  (None)");
    } else {
      xmlPropertiesWithEmptyValue.forEach(p -> LOG.info("  {}", p));
    }
    LOG.info("\n");

    // Print out any matching XML value/Config default value
    LOG.info("{} has {} properties which match a corresponding Config variable",
        xmlFilename, xmlPropertiesMatchingConfigDefault.size());
    if (xmlPropertiesMatchingConfigDefault.isEmpty()) {
      LOG.info("  (None)");
    } else {
      xmlPropertiesMatchingConfigDefault.forEach(
          (key, value) -> LOG.info("  {} / {}", key, value));
    }
    LOG.info("\n=====\n");
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
      LOG.info("Checking if any of the default values whose name " +
          "contains string \"{}\" collide.", filter);

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
            assertNull("Parameters " + ent.getKey() + " and " + crtValue +
                " are using the same default value!", crtValue);
          }
          valuesChecked++;
        }

      }
      LOG.info("Checked {} default values for collision.", valuesChecked);
    }
  }
}
