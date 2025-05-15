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

package org.apache.hadoop.util;

import java.io.File;

import org.apache.hadoop.fs.viewfs.ViewFileSystem;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.assertj.core.api.Assertions.assertThat;

public class TestClassUtil {

  @Test
  @Timeout(value = 10)
  public void testFindContainingJar() {
    String containingJar = ClassUtil.findContainingJar(Assertions.class);
    assertThat(containingJar)
        .describedAs("Containing jar for %s", Assertions.class)
        .isNotNull();
    File jarFile = new File(containingJar);
    assertThat(jarFile)
        .describedAs("Containing jar %s", jarFile)
        .exists();
    assertThat(jarFile.getName())
        .describedAs("Containing jar name %s", jarFile.getName())
        .matches("assertj-core.*[.]jar");
  }

  @Test
  @Timeout(value = 10)
  public void testFindContainingClass() {
    String classFileLocation = ClassUtil.findClassLocation(ViewFileSystem.class);
    assertThat(classFileLocation)
        .describedAs("Class path for %s", ViewFileSystem.class)
        .isNotNull();
    File classFile = new File(classFileLocation);
    assertThat(classFile)
        .describedAs("Containing class file %s", classFile)
        .exists();
    assertThat(classFile.getName())
        .describedAs("Containing class file name %s", classFile.getName())
        .matches("ViewFileSystem.class");
  }

}
