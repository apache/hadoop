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

import java.net.URI;

import javax.annotation.Nullable;

import org.apache.hadoop.fs.PathIOException;

/**
 * An instantiation exception raised during reflection-based creation
 * of classes.
 * Uses an enum of kind so tests/code can examine it, without
 * creating a full hierarchy of exception classes.
 */
public class InstantiationIOException extends PathIOException {

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

  /**
   * An (extensible) enum of kinds of instantiation failure.
   */
  public enum Kind {
    Forbidden,
    InstantiationFailure,
    IsAbstract,
    IsNotImplementation,
    Other,
    Unavailable,
    UnsupportedConstructor,
  }

  public InstantiationIOException(
      Kind kind,
      @Nullable URI uri,
      @Nullable String classname,
      @Nullable String key,
      String message,
      @Nullable Throwable cause) {
    super(uri!= null ? uri.toString() : "",
        (classname != null ? ("Class " + classname + " ") : "")
            + message
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

  /**
   * Class is abstract.
   * @param uri URI of filesystem
   * @param classname classname.
   * @param key configuration key
   * @return an exception.
   */
  public static InstantiationIOException isAbstract(URI uri, String classname, String key) {
    return new InstantiationIOException(Kind.IsAbstract,
        uri, classname, key, ABSTRACT_PROVIDER, null);
  }

  /**
   * Class does not implement the desired interface.
   * @param uri URI of filesystem
   * @param classname classname.
   * @param interfaceName required interface
   * @param key configuration key
   * @return an exception.
   */
  public static InstantiationIOException isNotInstanceOf(
      @Nullable URI uri,
      String classname,
      String interfaceName,
      String key) {
    return new InstantiationIOException(Kind.IsNotImplementation, uri, classname,
        key, DOES_NOT_IMPLEMENT + " " + interfaceName, null);
  }

  /**
   * Class is unavailable for some reason, probably a missing dependency.
   * @param uri URI of filesystem
   * @param classname classname.
   * @param key configuration key
   * @param text text to include
   * @return an exception.
   */
  public static InstantiationIOException unavailable(
      @Nullable URI uri,
      @Nullable String classname,
      @Nullable String key,
      String text) {
    return new InstantiationIOException(Kind.Unavailable,
        uri, classname, key, text, null);
  }

  /**
   * Failure to find a valid constructor (signature, visibility) or
   * factory method.
   * @param uri URI of filesystem
   * @param classname classname.
   * @param key configuration key
   * @return an exception.
   */
  public static InstantiationIOException unsupportedConstructor(
      @Nullable URI uri,
      String classname,
      String key) {
    return new InstantiationIOException(Kind.UnsupportedConstructor,
        uri, classname, key, CONSTRUCTOR_EXCEPTION, null);
  }

  /**
   * General instantiation failure.
   * @param uri URI of filesystem
   * @param classname classname.
   * @param key configuration key
   * @param t thrown
   * @return an exception.
   */
  public static InstantiationIOException instantiationException(
      @Nullable URI uri,
      String classname,
      String key,
      Throwable t) {
    return new InstantiationIOException(Kind.InstantiationFailure,
        uri, classname, key, INSTANTIATION_EXCEPTION + " " + t, t);
  }

}
