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
package org.apache.hadoop.yarn.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Shell;
import org.junit.Test;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestApps {
  @Test
  public void testSetEnvFromInputString() {
    Map<String, String> environment = new HashMap<String, String>();
    environment.put("JAVA_HOME", "/path/jdk");
    String goodEnv = "a1=1,b_2=2,_c=3,d=4,e=,f_win=%JAVA_HOME%"
        + ",g_nix=$JAVA_HOME";
    Apps.setEnvFromInputString(environment, goodEnv, File.pathSeparator);
    assertEquals("1", environment.get("a1"));
    assertEquals("2", environment.get("b_2"));
    assertEquals("3", environment.get("_c"));
    assertEquals("4", environment.get("d"));
    assertEquals("", environment.get("e"));
    if (Shell.WINDOWS) {
      assertEquals("$JAVA_HOME", environment.get("g_nix"));
      assertEquals("/path/jdk", environment.get("f_win"));
    } else {
      assertEquals("/path/jdk", environment.get("g_nix"));
      assertEquals("%JAVA_HOME%", environment.get("f_win"));
    }
    String badEnv = "1,,2=a=b,3=a=,4==,5==a,==,c-3=3,=";
    environment.clear();
    Apps.setEnvFromInputString(environment, badEnv, File.pathSeparator);
    assertEquals(environment.size(), 0);

    // Test "=" in the value part
    environment.clear();
    Apps.setEnvFromInputString(environment, "b1,e1==,e2=a1=a2,b2",
        File.pathSeparator);
    assertEquals("=", environment.get("e1"));
    assertEquals("a1=a2", environment.get("e2"));
  }

  @Test
  public void testSetEnvFromInputProperty() {
    Configuration conf = new Configuration(false);
    Map<String, String> env = new HashMap<>();
    String propName = "mapreduce.map.env";
    String defaultPropName = "mapreduce.child.env";
    // Setup environment input properties
    conf.set(propName, "env1=env1_val,env2=env2_val,env3=env3_val");
    conf.set(propName + ".env4", "env4_val");
    conf.set(propName + ".env2", "new_env2_val");
    // Setup some default values - we shouldn't see these values
    conf.set(defaultPropName, "env1=def1_val,env2=def2_val,env3=def3_val");
    String defaultPropValue = conf.get(defaultPropName);
    // These should never be referenced.
    conf.set(defaultPropName + ".env4", "def4_val");
    conf.set(defaultPropName + ".env2", "new_def2_val");
    Apps.setEnvFromInputProperty(env, propName, defaultPropValue, conf,
        File.pathSeparator);
    // Check values from string
    assertEquals("env1_val", env.get("env1"));
    assertEquals("env3_val", env.get("env3"));
    // Check individual value
    assertEquals("env4_val", env.get("env4"));
    // Check individual value that eclipses one in string
    assertEquals("new_env2_val", env.get("env2"));
  }

  @Test
  public void testSetEnvFromInputPropertyDefault() {
    Configuration conf = new Configuration(false);
    Map<String, String> env = new HashMap<>();
    String propName = "mapreduce.map.env";
    String defaultPropName = "mapreduce.child.env";
    // Setup environment input properties
    conf.set(propName, "env1=env1_val,env2=env2_val,env3=env3_val");
    conf.set(propName + ".env4", "env4_val");
    conf.set(propName + ".env2", "new_env2_val");
    // Setup some default values
    conf.set(defaultPropName, "env1=def1_val,env2=def2_val,env3=def3_val");
    String defaultPropValue = conf.get(defaultPropName);
    // These should never be referenced.
    conf.set(defaultPropName + ".env4", "def4_val");
    conf.set(defaultPropName + ".env2", "new_def2_val");

    // Test using default value for the string.
    // Individually specified env properties do not have defaults,
    // so this should just get things from the defaultPropName string.
    String bogusProp = propName + "bogus";
    Apps.setEnvFromInputProperty(env, bogusProp, defaultPropValue, conf,
        File.pathSeparator);
    // Check values from string
    assertEquals("def1_val", env.get("env1"));
    assertEquals("def2_val", env.get("env2"));
    assertEquals("def3_val", env.get("env3"));
    // Check individual value is not set.
    assertNull(env.get("env4"));
  }

  @Test
  public void testSetEnvFromInputPropertyOverrideDefault() {
    Configuration conf = new Configuration(false);
    Map<String, String> env = new HashMap<>();

    // Try using default value, but specify some individual values using
    // the main propName, but no main value, so it should get values from
    // the default string, and then the individual values.
    String propName = "mapreduce.reduce.env";
    conf.set(propName + ".env2", "new2_val");
    conf.set(propName + ".env4", "new4_val");
    // Setup some default values - we shouldn't see these values
    String defaultPropName = "mapreduce.child.env";
    conf.set(defaultPropName, "env1=def1_val,env2=def2_val,env3=def3_val");
    String defaultPropValue = conf.get(defaultPropName);
    // These should never be referenced.
    conf.set(defaultPropName + ".env4", "def4_val");
    conf.set(defaultPropName + ".env2", "new_def2_val");
    Apps.setEnvFromInputProperty(env, propName, defaultPropValue, conf,
        File.pathSeparator);

    // Check values from string
    assertEquals("def1_val", env.get("env1"));
    assertEquals("def3_val", env.get("env3"));
    // Check individual value
    assertEquals("new4_val", env.get("env4"));
    // Check individual value that eclipses one in string
    assertEquals("new2_val", env.get("env2"));
  }

  @Test
  public void testSetEnvFromInputPropertyCommas() {
    Configuration conf = new Configuration(false);
    Map<String, String> env = new HashMap<>();
    String propName = "mapreduce.reduce.env";
    conf.set(propName, "env1=env1_val,env2=env2_val,env3=env3_val");
    conf.set(propName + ".env2", "new2_val1,new2_val2,new2_val3");
    conf.set(propName + ".env4", "new4_valwith=equals");
    // Setup some default values - we shouldn't see these values
    String defaultPropName = "mapreduce.child.env";
    conf.set(defaultPropName, "env1=def1_val,env2=def2_val,env3=def3_val");
    String defaultPropValue = conf.get(defaultPropName);

    Apps.setEnvFromInputProperty(env, propName, defaultPropValue, conf,
        File.pathSeparator);
    // Check values from string
    assertEquals("env1_val", env.get("env1"));
    assertEquals("env3_val", env.get("env3"));
    // Check individual value
    assertEquals("new4_valwith=equals", env.get("env4"));
    // Check individual value that eclipses one in string
    assertEquals("new2_val1,new2_val2,new2_val3", env.get("env2"));
  }

  @Test
  public void testSetEnvFromInputPropertyNull() {
    Configuration conf = new Configuration(false);
    Map<String, String> env = new HashMap<>();
    String propName = "mapreduce.map.env";
    String defaultPropName = "mapreduce.child.env";
    // Setup environment input properties
    conf.set(propName, "env1=env1_val,env2=env2_val,env3=env3_val");
    conf.set(propName + ".env4", "env4_val");
    conf.set(propName + ".env2", "new_env2_val");
    // Setup some default values - we shouldn't see these values
    conf.set(defaultPropName, "env1=def1_val,env2=def2_val,env3=def3_val");
    String defaultPropValue = conf.get(defaultPropName);
    // These should never be referenced.
    conf.set(defaultPropName + ".env4", "def4_val");
    conf.set(defaultPropName + ".env2", "new_def2_val");
    // Try with null inputs
    Apps.setEnvFromInputProperty(env, "bogus1", null, conf, File.pathSeparator);
    assertTrue(env.isEmpty());
  }
}
