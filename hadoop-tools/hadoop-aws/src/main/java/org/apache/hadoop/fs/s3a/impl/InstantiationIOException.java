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

package org.apache.hadoop.fs.s3a.impl;

import java.io.IOException;

/**
 * An instantiation exception raised during reflection-based creation
 * of classes.
 * Uses an enum of kind so tests/code can examine it, without
 * creating a full hierarchy of exception classes.
 */
public class InstantiationIOException extends IOException {

  public static final String ABSTRACT_PROVIDER =
      "is abstract and therefore cannot be created";

  public static final String CONSTRUCTOR_EXCEPTION = "constructor exception";

  public static final String INSTANTIATION_EXCEPTION
      = "instantiation exception";

  public static final String DOES_NOT_IMPLEMENT
      = "does not implement";

  /**
   * Exception kind.
   */
  private final Kind kind;

  /**
   * Class being instantiated.
   */
  private final String classname;

  /**
   * key used.
   */
  private final String key;


  public InstantiationIOException(
      final Kind kind,
      final String classname,
      final String key,
      final String message,
      final Throwable cause) {
    super("Class " + classname + " " + message
            + (key != null ? (" (configuration key " + key + ")") : ""),
        cause);
    this.kind = kind;
    this.classname = classname;
    this.key = key;
  }

  public String getClassname() {
    return classname;
  }

  public Kind getKind() {
    return kind;
  }

  public String getKey() {
    return key;
  }

  public static InstantiationIOException isAbstract(String classname, String key) {
    return new InstantiationIOException(Kind.IsAbstract, classname, key, ABSTRACT_PROVIDER, null);
  }

  public static InstantiationIOException isNotInstanceOf(String classname,
      String interfaceName,
      String key) {
    return new InstantiationIOException(Kind.IsNotImplementation, classname,
        key, DOES_NOT_IMPLEMENT + " " + interfaceName, null);
  }


  public static InstantiationIOException unsupportedConstructor(String classname, String key) {
    return new InstantiationIOException(Kind.UnsupportedConstructor,
        classname, key, CONSTRUCTOR_EXCEPTION, null);
  }


  public static InstantiationIOException instantiationException(String classname,
      String key,
      Throwable t) {
    return new InstantiationIOException(Kind.InstantiationFailure,
        classname, key, INSTANTIATION_EXCEPTION + " " + t, t);
  }

  /**
   * An (extensible) enum of kinds.
   */
  public enum Kind {
    IsAbstract,
    UnsupportedConstructor,
    IsNotImplementation,
    InstantiationFailure,
    Other,
  }

}
