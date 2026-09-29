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

import jdk.javadoc.doclet.DocletEnvironment;
import jdk.javadoc.doclet.Reporter;
import javax.lang.model.SourceVersion;

import jdiff.JDiff;

/**
 * <a href=
 * "https://docs.oracle.com/en/java/javase/17/docs/api/jdk.javadoc/jdk/javadoc/doclet/Doclet.html">
 * Doclet</a> for excluding elements that are annotated with
 * {@link org.apache.hadoop.classification.InterfaceAudience.Private} or
 * {@link org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate}.
 * It delegates to the JDiff Doclet, and takes the same options.
 */
public final class ExcludePrivateAnnotationsJDiffDoclet {

  /**
   * Returns the source version used by this doclet.
   *
   * @return the supported source version
   */
  public static SourceVersion languageVersion() {
    return SourceVersion.RELEASE_17;
  }

  /**
   * Legacy doclet entry point used by JDiff/Javadoc.
   *
   * @param root the doclet environment
   * @return true if the doclet completed successfully
   */
  public static boolean start(DocletEnvironment root) {
    System.out.println(
        ExcludePrivateAnnotationsJDiffDoclet.class.getSimpleName());
    return JDiff.start(RootDocProcessor.process(root));
  }

  /**
   * Utility class: provides only static entry points for JDiff.
   */
  private ExcludePrivateAnnotationsJDiffDoclet() {
  }

  /**
   * Returns the length of a supported option.
   *
   * @param option the option name
   * @return the number of arguments including the option itself
   */
  public static int optionLength(String option) {
    Integer length = StabilityOptions.optionLength(option);
    if (length != null) {
      return length;
    }
    return JDiff.optionLength(option);
  }

  /**
   * Validates options before running the doclet.
   *
   * @param options  the options to validate
   * @param reporter the reporter to use for diagnostics
   * @return true if the options are valid
   */
  public static boolean validOptions(String[][] options,
      Reporter reporter) {
    StabilityOptions.validOptions(options, reporter);
    String[][] filteredOptions = StabilityOptions.filterOptions(options);
    return JDiff.validOptions(filteredOptions, reporter);
  }
}
