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
package org.apache.hadoop.classification.tools;

import com.sun.javadoc.DocErrorReporter;

import java.util.ArrayList;
import java.util.List;

class StabilityOptions {
  public static final String STABLE_OPTION = "-stable";
  public static final String EVOLVING_OPTION = "-evolving";
  public static final String UNSTABLE_OPTION = "-unstable";

  public static Integer optionLength(String option) {
    String opt = option.toLowerCase();
    if (opt.equals(UNSTABLE_OPTION)) return 1;
    if (opt.equals(EVOLVING_OPTION)) return 1;
    if (opt.equals(STABLE_OPTION)) return 1;
    return null;
  }

  public static void validOptions(String[][] options,
      DocErrorReporter reporter) {
    for (int i = 0; i < options.length; i++) {
      String opt = options[i][0].toLowerCase();
      if (opt.equals(UNSTABLE_OPTION)) {
	RootDocProcessor.stability = UNSTABLE_OPTION;
      } else if (opt.equals(EVOLVING_OPTION)) {
	RootDocProcessor.stability = EVOLVING_OPTION;
      } else if (opt.equals(STABLE_OPTION)) {
	RootDocProcessor.stability = STABLE_OPTION;	
      }
    }
  }
  
  public static String[][] filterOptions(String[][] options) {
    List<String[]> optionsList = new ArrayList<String[]>();
    for (int i = 0; i < options.length; i++) {
      if (!options[i][0].equalsIgnoreCase(UNSTABLE_OPTION)
	  && !options[i][0].equalsIgnoreCase(EVOLVING_OPTION)
	  && !options[i][0].equalsIgnoreCase(STABLE_OPTION)) {
	optionsList.add(options[i]);
      }
    }
    String[][] filteredOptions = new String[optionsList.size()][];
    int i = 0;
    for (String[] option : optionsList) {
      filteredOptions[i++] = option;
    }
    return filteredOptions;
  }

}
