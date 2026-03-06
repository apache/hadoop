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

/**
 * Process the {@link DocletEnvironment} by filtering elements with
 * Private or LimitedPrivate annotations using HadoopDocEnvImpl.
 * <p>
 * Based on code from http://www.sixlegs.com/blog/java/exclude-javadoc-tag.html.
 */
final class RootDocProcessor {

  private static String stability = StabilityOptions.UNSTABLE_OPTION;
  private static boolean treatUnannotatedClassesAsPrivate = false;

  static void setStability(String value) {
    stability = value;
  }

  private RootDocProcessor() {
    // no instances
  }

  static String getStability() {
    return stability;
  }

  static void setTreatUnannotatedClassesAsPrivate(boolean value) {
    treatUnannotatedClassesAsPrivate = value;
  }

  static boolean isTreatUnannotatedClassesAsPrivate() {
    return treatUnannotatedClassesAsPrivate;
  }

  public static DocletEnvironment process(DocletEnvironment root) {
    return new HadoopDocEnvImpl(root, stability, treatUnannotatedClassesAsPrivate);
  }
}
