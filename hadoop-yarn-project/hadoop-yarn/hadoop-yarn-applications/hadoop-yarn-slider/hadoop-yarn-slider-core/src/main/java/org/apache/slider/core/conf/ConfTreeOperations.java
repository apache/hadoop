/*
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

package org.apache.slider.core.conf;

import org.apache.slider.common.tools.SliderUtils;
import org.apache.slider.core.exceptions.BadConfigException;
import org.apache.slider.core.persist.ConfTreeSerDeser;
import org.apache.slider.core.persist.PersistKeys;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.map.JsonMappingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ConfTreeOperations {

  public final ConfTree confTree;
  private final MapOperations globalOptions;

  protected static final Logger
    log = LoggerFactory.getLogger(ConfTreeOperations.class);


  public ConfTreeOperations(ConfTree confTree) {
    assert confTree != null : "null tree";
    assert confTree.components != null : "null tree components";
    this.confTree = confTree;
    globalOptions = new MapOperations("global", confTree.global);
  }

  /**
   * Get the underlying conf tree
   * @return the tree
   */
  public ConfTree getConfTree() {
    return confTree;
  }

  /**
   * Validate the configuration
   * @throws BadConfigException
   */
  public void validate() throws BadConfigException {
    validate(null);
  }

  /**
   * Validate the configuration
   * @param validator a provided properties validator
   * @throws BadConfigException
   */
  public void validate(InputPropertiesValidator validator) throws BadConfigException {
    String version = confTree.schema;
    if (version == null) {
      throw new BadConfigException("'version' undefined");
    }
    if (!PersistKeys.SCHEMA.equals(version)) {
      throw new BadConfigException(
          "version %s incompatible with supported version %s",
          version,
          PersistKeys.SCHEMA);
    }
    if (validator != null) {
      validator.validate(this);
    }
  }

  /**
   * Resolve a ConfTree by mapping all global options into each component
   * -if there is none there already
   */
  public void resolve() {
    for (Map.Entry<String, Map<String, String>> comp : confTree.components.entrySet()) {
      mergeInGlobal(comp.getValue());
    }
  }

  /**
   * Merge any options
   * @param component dest values
   */
  public void mergeInGlobal(Map<String, String> component) {
    SliderUtils.mergeMapsIgnoreDuplicateKeys(component, confTree.global);
  }

  /**
   * Get operations on the global set
   * @return a wrapped map
   */
  public MapOperations getGlobalOptions() {
    return globalOptions;
  }


  /**
   * look up a component and return its options
   * @param component component name
   * @return component mapping or null
   */
  public MapOperations getComponent(String component) {
    Map<String, String> instance = confTree.components.get(component);
    if (instance != null) {
      return new MapOperations(component, instance);
    }
    return null;
  }

  /**
   * look up a component and return its options with the specified replacements
   * @param component component name
   * @param replacementOptions replacement options
   * @return component mapping or null
   */
  public MapOperations getComponent(String component, Map<String,String>
      replacementOptions) {
    Map<String, String> instance = confTree.components.get(component);
    if (instance != null) {
      Map<String, String> newInstance = new HashMap<>();
      newInstance.putAll(instance);
      newInstance.putAll(replacementOptions);
      return new MapOperations(component, newInstance);
    }
    return null;
  }

  /**
   * Get at the underlying component map
   * @return a map of components. This is the raw ConfTree data structure
   */
  public Map<String, Map<String, String>> getComponents() {
    return confTree.components;
  }

  /**
   * Get a component -adding it to the components map if
   * none with that name exists
   * @param name role
   * @return role mapping
   */
  public MapOperations getOrAddComponent(String name) {
    MapOperations operations = getComponent(name);
    if (operations != null) {
      return operations;
    }
    //create a new instances
    Map<String, String> map = new HashMap<>();
    confTree.components.put(name, map);
    return new MapOperations(name, map);
  }


  /*
   * return the Set of names names
   */
  @JsonIgnore
  public Set<String> getComponentNames() {
    return new HashSet<String>(confTree.components.keySet());
  }
  
  

  /**
   * Get a component whose presence is mandatory
   * @param name component name
   * @return the mapping
   * @throws BadConfigException if the name is not there
   */
  public MapOperations getMandatoryComponent(String name) throws
                                                          BadConfigException {
    MapOperations ops = getComponent(name);
    if (ops == null) {
      throw new BadConfigException("Missing component " + name);
    }
    return ops;
  }

  /**
   * Set a global option, converting it to a string as needed
   * @param key key
   * @param value non null value
   */
  public void set(String key, Object value) {
    globalOptions.put(key, value.toString());
  }
  /**
   * get a global option
   * @param key key
   * @return value or null
   * 
   */
  public String get(String key) {
    return globalOptions.get(key);
  }
  /**
   * append to a global option
   * @param key key
   * @return value
   *
   */
  public String append(String key, String value) {
    if (SliderUtils.isUnset(value)) {
      return null;
    }
    if (globalOptions.containsKey(key)) {
      globalOptions.put(key, globalOptions.get(key) + "," + value);
    } else {
      globalOptions.put(key, value);
    }
    return globalOptions.get(key);
  }

  /**
   * Propagate all global keys matching a prefix
   * @param src source
   * @param prefix prefix
   */
  public void propagateGlobalKeys(ConfTree src, String prefix) {
    Map<String, String> global = src.global;
    for (Map.Entry<String, String> entry : global.entrySet()) {
      String key = entry.getKey();
      if (key.startsWith(prefix)) {
        set(key, entry.getValue());
      }
    }
  }

  /**
   * Propagate all global keys matching a prefix
   * @param src source
   * @param prefix prefix
   */
  public void propagateGlobalKeys(ConfTreeOperations src, String prefix) {
    propagateGlobalKeys(src.confTree, prefix);
  }

  /**
   * Merge the map of a single component
   * @param component component name
   * @param map map to merge
   */
  public void mergeSingleComponentMap(String component, Map<String, String> map) {
    MapOperations comp = getOrAddComponent(component);
    comp.putAll(map);
  }
  /**
   * Merge the map of a single component
   * @param component component name
   * @param map map to merge
   */
  public void mergeSingleComponentMapPrefix(String component,
                                            Map<String, String> map,
                                            String prefix,
                                            boolean overwrite) {
    boolean needsMerge = false;
    for (Map.Entry<String, String> entry : map.entrySet()) {
      String key = entry.getKey();
      if (key.startsWith(prefix)) {
        needsMerge = true;
        break;
      }
    }
    if (!needsMerge) {
      return;
    }
    MapOperations comp = getOrAddComponent(component);
    comp.mergeMapPrefixedKeys(map,prefix, overwrite);
  }

  /**
   * Merge in components
   * @param commandOptions component options on the CLI
   */
  public void mergeComponents(Map<String, Map<String, String>> commandOptions) {
    for (Map.Entry<String, Map<String, String>> entry : commandOptions.entrySet()) {
      mergeSingleComponentMap(entry.getKey(), entry.getValue());
    }
  }

  /**
   * Merge in components
   * @param commandOptions component options on the CLI
   */
  public void mergeComponentsPrefix(Map<String,
    Map<String, String>> commandOptions,
                                    String prefix,
                                    boolean overwrite) {
    for (Map.Entry<String, Map<String, String>> entry : commandOptions.entrySet()) {
      mergeSingleComponentMapPrefix(entry.getKey(), entry.getValue(), prefix, overwrite);
    }
  }

  /**
   * Merge in another tree -no overwrites of global or conf data
   * (note that metadata does a naive putAll merge/overwrite)
   * @param that the other tree
   */
  public void mergeWithoutOverwrite(ConfTree that) {

    getGlobalOptions().mergeWithoutOverwrite(that.global);
    confTree.metadata.putAll(that.metadata);
    confTree.credentials.putAll(that.credentials);

    for (Map.Entry<String, Map<String, String>> entry : that.components.entrySet()) {
      MapOperations comp = getOrAddComponent(entry.getKey());
      comp.mergeWithoutOverwrite(entry.getValue());
    }
  }
  
  /**
   * Merge in another tree with overwrites
   * @param that the other tree
   */
  public void merge(ConfTree that) {

    getGlobalOptions().putAll(that.global);
    confTree.metadata.putAll(that.metadata);
    confTree.credentials.putAll(that.credentials);

    for (Map.Entry<String, Map<String, String>> entry : that.components.entrySet()) {
      MapOperations comp = getOrAddComponent(entry.getKey());
      comp.putAll(entry.getValue());
    }
  }

  
  /**
   * Load from a resource. The inner conf tree is the loaded data -unresolved
   * @param resource resource
   * @return loaded value
   * @throws IOException load failure
   */
  public static ConfTreeOperations fromResource(String resource) throws
                                                                 IOException {
    ConfTreeSerDeser confTreeSerDeser = new ConfTreeSerDeser();
    ConfTreeOperations ops = new ConfTreeOperations(
       confTreeSerDeser.fromResource(resource) );
    return ops;      
  }
  
  /**
   * Load from a resource. The inner conf tree is the loaded data -unresolved
   * @param resource resource
   * @return loaded value
   * @throws IOException load failure
   */
  public static ConfTreeOperations fromFile(File resource) throws
                                                                 IOException {
    ConfTreeSerDeser confTreeSerDeser = new ConfTreeSerDeser();
    ConfTreeOperations ops = new ConfTreeOperations(
       confTreeSerDeser.fromFile(resource) );
    return ops;
  }

  /**
   * Build from an existing instance -which is cloned via JSON ser/deser
   * @param instance the source instance
   * @return loaded value
   * @throws IOException load failure
   */
  public static ConfTreeOperations fromInstance(ConfTree instance) throws
                                                                 IOException {
    ConfTreeSerDeser confTreeSerDeser = new ConfTreeSerDeser();
    ConfTreeOperations ops = new ConfTreeOperations(
       confTreeSerDeser.fromJson(confTreeSerDeser.toJson(instance)) );
    return ops;
  }

  /**
   * Load from a file and merge it in
   * @param file file
   * @throws IOException any IO problem
   * @throws BadConfigException if the file is invalid
   */
  public void mergeFile(File file) throws IOException, BadConfigException {
    mergeFile(file, null);
  }

  /**
   * Load from a file and merge it in
   * @param file file
   * @param validator properties validator
   * @throws IOException any IO problem
   * @throws BadConfigException if the file is invalid
   */
  public void mergeFile(File file, InputPropertiesValidator validator) throws IOException, BadConfigException {
    ConfTreeSerDeser confTreeSerDeser = new ConfTreeSerDeser();
    ConfTree tree = confTreeSerDeser.fromFile(file);
    ConfTreeOperations ops = new ConfTreeOperations(tree);
    ops.validate(validator);
    merge(ops.confTree);
  }

  @Override
  public String toString() {
    return confTree.toString();
  }

  /**
   * Convert to a JSON string
   * @return a JSON string description
   */
  public String toJson() throws IOException,
                                JsonGenerationException,
                                JsonMappingException {
    return confTree.toJson();
  }

  /**
   * Get a component option
   * @param name component name
   * @param option option name
   * @param defVal default value
   * @return resolved value
   */
  public String getComponentOpt(String name, String option, String defVal) {
    MapOperations roleopts = getComponent(name);
    if (roleopts == null) {
      return defVal;
    }
    return roleopts.getOption(option, defVal);
  }

  /**
   * Get a component opt; use {@link Integer#decode(String)} so as to take hex
   * oct and bin values too.
   *
   * @param name component name
   * @param option option name
   * @param defVal default value
   * @return parsed value
   * @throws NumberFormatException if the role could not be parsed.
   */
  public int getComponentOptInt(String name, String option, int defVal) {
    String val = getComponentOpt(name, option, Integer.toString(defVal));
    return Integer.decode(val);
  }

  /**
   * Get a component opt as a boolean using {@link Boolean#valueOf(String)}.
   *
   * @param name component name
   * @param option option name
   * @param defVal default value
   * @return parsed value
   * @throws NumberFormatException if the role could not be parsed.
   */
  public boolean getComponentOptBool(String name, String option, boolean defVal) {
    String val = getComponentOpt(name, option, Boolean.toString(defVal));
    return Boolean.valueOf(val);
  }

  /**
   * Set a component option, creating the component if necessary
   * @param component component name
   * @param option option name
   * @param val value
   */
  public void setComponentOpt(String component, String option, String val) {
    Map<String, String> roleopts = getOrAddComponent(component);
    roleopts.put(option, val);
  }

  /**
   * Set an integer role option, creating the role if necessary
   * @param role role name
   * @param option option name
   * @param val integer value
   */
  public void setComponentOpt(String role, String option, int val) {
    setComponentOpt(role, option, Integer.toString(val));
  }
  /**
   * Set a long role option, creating the role if necessary
   * @param role role name
   * @param option option name
   * @param val long value
   */
  public void setComponentOpt(String role, String option, long val) {
    setComponentOpt(role, option, Long.toString(val));
  }

  /**
   * append to a component option
   * @param key key
   * @return value
   *
   */
  public String appendComponentOpt(String role, String key, String value) {
    if (SliderUtils.isUnset(value)) {
      return null;
    }
    MapOperations roleopts = getComponent(role);
    if (roleopts == null) {
      return null;
    }

    if (roleopts.containsKey(key)) {
      roleopts.put(key, roleopts.get(key) + "," + value);
    } else {
      roleopts.put(key, value);
    }
    return roleopts.get(key);
  }
}
