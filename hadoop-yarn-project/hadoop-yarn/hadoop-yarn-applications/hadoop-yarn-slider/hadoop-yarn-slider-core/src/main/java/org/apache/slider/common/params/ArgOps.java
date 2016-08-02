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

package org.apache.slider.common.params;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.slider.core.exceptions.BadCommandArgumentsException;
import org.apache.slider.core.exceptions.ErrorStrings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Static argument manipulation operations
 */
public class ArgOps {

  private static final Logger
    log = LoggerFactory.getLogger(ArgOps.class);

  /**
   * create a 3-tuple
   */
  public static List<Object> triple(String msg, int min, int max) {
    List<Object> l = new ArrayList<>(3);
    l.add(msg);
    l.add(min);
    l.add(max);
    return l;
  }

  public static void applyFileSystemBinding(String filesystemBinding,
      Configuration conf) {
    if (filesystemBinding != null) {
      //filesystem argument was set -this overwrites any defaults in the
      //configuration
      FileSystem.setDefaultUri(conf, filesystemBinding);
    }
  }

  public static void splitPairs(Collection<String> pairs,
                                Map<String, String> dest) {
    for (String prop : pairs) {
      String[] keyval = prop.split("=", 2);
      if (keyval.length == 2) {
        dest.put(keyval[0], keyval[1]);
      }
    }
  }


  public static void applyDefinitions(Map<String, String> definitionMap,
                                      Configuration conf) {
    for (Map.Entry<String, String> entry : definitionMap.entrySet()) {
      String key = entry.getKey();
      String val = entry.getValue();
      log.debug("configuration[{}]<=\"{}\"", key, val);
      conf.set(key, val, "command line");
    }
  }

  /**
   * Create a map from a tuple list like ['worker','2','master','1] into a map
   * ['worker':'2',"master":'1'];
   * Duplicate entries also trigger errors
   * @param description description for errors
   * @param list list to conver to tuples
   * @return the map of key value pairs -unordered.
   * @throws BadCommandArgumentsException odd #of arguments received
   */
  public static Map<String, String> convertTupleListToMap(String description,
                                                          List<String> list) throws
                                                                             BadCommandArgumentsException {
    Map<String, String> results = new HashMap<>();
    if (list != null && !list.isEmpty()) {
      int size = list.size();
      if (size % 2 != 0) {
        //odd number of elements, not permitted
        throw new BadCommandArgumentsException(
          ErrorStrings.ERROR_PARSE_FAILURE + description);
      }
      for (int count = 0; count < size; count += 2) {
        String key = list.get(count);
        String val = list.get(count + 1);
        if (results.get(key) != null) {
          throw new BadCommandArgumentsException(
            ErrorStrings.ERROR_DUPLICATE_ENTRY + description
            + ": " + key);
        }
        results.put(key, val);
      }
    }
    return results;
  }

  /**
   * Create a map from a tuple list like
   * ['worker','heapsize','5G','master','heapsize','2M'] into a map
   * ['worker':'2',"master":'1'];
   * Duplicate entries also trigger errors

   * @throws BadCommandArgumentsException odd #of arguments received
   */
  public static Map<String, Map<String, String>> convertTripleListToMaps(String description,
         List<String> list) throws BadCommandArgumentsException {

    Map<String, Map<String, String>> results = new HashMap<>();
    if (list != null && !list.isEmpty()) {
      int size = list.size();
      if (size % 3 != 0) {
        //wrong number of elements, not permitted
        throw new BadCommandArgumentsException(
          ErrorStrings.ERROR_PARSE_FAILURE + description);
      }
      for (int count = 0; count < size; count += 3) {
        String role = list.get(count);
        String key = list.get(count + 1);
        String val = list.get(count + 2);
        Map<String, String> roleMap = results.get(role);
        if (roleMap == null) {
          //demand create new role map
          roleMap = new HashMap<>();
          results.put(role, roleMap);
        }
        if (roleMap.get(key) != null) {
          throw new BadCommandArgumentsException(
            ErrorStrings.ERROR_DUPLICATE_ENTRY + description
            + ": for key " + key + " under " + role);
        }
        roleMap.put(key, val);
      }
    }
    return results;
  }
}
