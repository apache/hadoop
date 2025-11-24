/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs.compat.common;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hadoop.fs.compat.cases.HdfsCompatBasics;
import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Disabled;

import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;

public class TestHdfsCompatInterfaceCoverage {
  @Test
  @Disabled
  public void testFsCompatibility() {
    Set<String> publicMethods = getPublicInterfaces(FileSystem.class);
    Set<String> targets = getTargets(HdfsCompatBasics.class);
    for (String publicMethod : publicMethods) {
      assertTrue(targets.contains(publicMethod),
          "Method not tested: " + publicMethod);
    }
  }

  private Set<String> getPublicInterfaces(Class<?> cls) {
    return HdfsCompatApiScope.getPublicInterfaces(cls);
  }

  private Set<String> getTargets(Class<? extends AbstractHdfsCompatCase> cls) {
    Method[] methods = cls.getDeclaredMethods();
    Set<String> targets = new HashSet<>();
    for (Method method : methods) {
      if (method.isAnnotationPresent(HdfsCompatCase.class)) {
        targets.add(method.getName());
      }
    }
    return targets;
  }
}