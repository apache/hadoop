/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.fs.sdk;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import org.junit.Test;
import org.apache.hadoop.test.AbstractHadoopTestBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests to verify AWS SDK based issues like duplicated shaded classes and others.
 */
public class TestAWSV2SDK extends AbstractHadoopTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(TestAWSV2SDK.class.getName());

  @Test
  public void testShadedClasses() throws IOException {
    String allClassPath = System.getProperty("java.class.path");
    LOG.debug("Current classpath:{}", allClassPath);
    String[] classPaths = allClassPath.split(File.pathSeparator);
    String v2ClassPath = null;
    for (String classPath : classPaths) {
      //Checking for only version 2.x sdk here
      if (classPath.contains("awssdk/bundle/2")) {
        v2ClassPath = classPath;
        break;
      }
    }
    LOG.debug("AWS SDK V2 Classpath:{}", v2ClassPath);
    assertThat(v2ClassPath)
            .as("AWS V2 SDK should be present on the classpath").isNotNull();
    List<String> listOfV2SdkClasses = getClassNamesFromJarFile(v2ClassPath);
    String awsSdkPrefix = "software/amazon/";
    List<String> unshadedClasses = new ArrayList<>();
    for (String awsSdkClass : listOfV2SdkClasses) {
      if (!awsSdkClass.startsWith(awsSdkPrefix)) {
        unshadedClasses.add(awsSdkClass);
      }
    }
    if (!unshadedClasses.isEmpty()) {
      LOG.warn("Unshaded Classes Found :{}", unshadedClasses.size());
      LOG.warn("List of unshaded classes:{}", unshadedClasses);
    } else {
      LOG.info("No Unshaded classes found in the sdk.");
    }
  }

  /**
   * Returns the list of classes in a jar file.
   * @param jarFilePath: the location of the jar file from absolute path
   * @return a list of classes contained by the jar file
   * @throws IOException if the file is not present or the path is not readable
   */
  private List<String> getClassNamesFromJarFile(String jarFilePath) throws IOException {
    List<String> classNames = new ArrayList<>();
    try (JarFile jarFile = new JarFile(new File(jarFilePath))) {
      Enumeration<JarEntry> jarEntryEnumeration = jarFile.entries();
      while (jarEntryEnumeration.hasMoreElements()) {
        JarEntry jarEntry = jarEntryEnumeration.nextElement();
        if (jarEntry.getName().endsWith(".class")) {
          classNames.add(jarEntry.getName());
        }
      }
    }
    return classNames;
  }
}
