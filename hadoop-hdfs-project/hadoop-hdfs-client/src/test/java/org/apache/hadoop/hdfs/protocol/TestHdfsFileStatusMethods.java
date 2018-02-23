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
package org.apache.hadoop.hdfs.protocol;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Stream;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;

import org.apache.hadoop.fs.FileStatus;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit test verifying that {@link HdfsFileStatus} is a superset of
 * {@link FileStatus}.
 */
public class TestHdfsFileStatusMethods {

  @Test
  public void testInterfaceSuperset() {
    Set<MethodSignature>  fsM = signatures(FileStatus.class);
    Set<MethodSignature> hfsM = signatures(HdfsFileStatus.class);
    hfsM.addAll(Stream.of(HdfsFileStatus.class.getInterfaces())
        .flatMap(i -> Stream.of(i.getDeclaredMethods()))
        .map(MethodSignature::new)
        .collect(toSet()));
    // HdfsFileStatus is not a concrete type
    hfsM.addAll(signatures(Object.class));
    assertTrue(fsM.removeAll(hfsM));
    // verify that FileStatus is a subset of HdfsFileStatus
    assertEquals(fsM.stream()
            .map(MethodSignature::toString)
            .collect(joining("\n")),
        Collections.emptySet(), fsM);
  }

  /** Map non-static, declared methods for this class to signatures. */
  private static Set<MethodSignature> signatures(Class<?> c) {
    return Stream.of(c.getDeclaredMethods())
        .filter(m -> !Modifier.isStatic(m.getModifiers()))
        .map(MethodSignature::new)
        .collect(toSet());
  }

  private static class MethodSignature {
    private final String name;
    private final Type rval;
    private final Type[] param;
    MethodSignature(Method m) {
      name = m.getName();
      rval = m.getGenericReturnType();
      param = m.getParameterTypes();
    }
    @Override
    public int hashCode() {
      return name.hashCode();
    }
    /**
     * Methods are equal iff they have the same name, return type, and params
     * (non-generic).
     */
    @Override
    public boolean equals(Object o) {
      if (!(o instanceof MethodSignature)) {
        return false;
      }
      MethodSignature s = (MethodSignature) o;
      return name.equals(s.name) &&
          rval.equals(s.rval) &&
          Arrays.equals(param, s.param);
    }
    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(rval).append(" ").append(name).append("(")
        .append(Stream.of(param)
            .map(Type::toString).collect(joining(",")))
        .append(")");
      return sb.toString();
    }
  }

}
