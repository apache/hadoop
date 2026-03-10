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

import jdk.javadoc.doclet.Reporter;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Doclet option helpers for API stability filtering.
 */
public final class StabilityOptions {

  /** Option flag: {@code -stable}. */
  public static final String STABLE_OPTION = "-stable";

  /** Option flag: {@code -evolving}. */
  public static final String EVOLVING_OPTION = "-evolving";

  /** Option flag: {@code -unstable}. */
  public static final String UNSTABLE_OPTION = "-unstable";

  enum Level { STABLE, EVOLVING, UNSTABLE }
  private static volatile Level level = Level.UNSTABLE;

  static void setLevel(Level l) {
    if (l != null) {
      level = l;
    }
  }

  private StabilityOptions() {
  }

  /**
   * Return option length for a supported stability option.
   *
   * @param option option name
   * @return {@code 1} if supported; otherwise {@code null}
   */
  public static Integer optionLength(String option) {
    String opt = option.toLowerCase(Locale.ENGLISH);
    if (opt.equals(UNSTABLE_OPTION)) {
      return 1;
    }
    if (opt.equals(EVOLVING_OPTION)) {
      return 1;
    }
    if (opt.equals(STABLE_OPTION)) {
      return 1;
    }
    return null;
  }

  static void setFromOptionName(String optName) {
    String opt = optName.toLowerCase(Locale.ENGLISH);
    Level next = null;
    if (opt.equals(UNSTABLE_OPTION)) {
      next = Level.UNSTABLE;
    } else if (opt.equals(EVOLVING_OPTION)) {
      next = Level.EVOLVING;
    } else if (opt.equals(STABLE_OPTION)) {
      next = Level.STABLE;
    }
    if (next != null && next.ordinal() > level.ordinal()) {
      level = next;
    }
  }

  static Level getLevel() {
    return level;
  }

  static void applyToRootProcessor() {
    switch (level) {
    case UNSTABLE:
      RootDocProcessor.setStability(UNSTABLE_OPTION);
      break;
    case EVOLVING:
      RootDocProcessor.setStability(EVOLVING_OPTION);
      break;
    default:
      RootDocProcessor.setStability(STABLE_OPTION);
    }
  }

  /**
   * Validate and apply stability options.
   *
   * @param options  doclet options
   * @param reporter reporter
   */
  public static void validOptions(String[][] options,
      Reporter reporter) {
    for (int i = 0; i < options.length; i++) {
      String opt = options[i][0].toLowerCase(Locale.ENGLISH);
      setFromOptionName(opt);
    }
    applyToRootProcessor();
  }

  /**
   * Filter out stability options from the doclet options array.
   *
   * @param options doclet options
   * @return options without stability flags
   */
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
