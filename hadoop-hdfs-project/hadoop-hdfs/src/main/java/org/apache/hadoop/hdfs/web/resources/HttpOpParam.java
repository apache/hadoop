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
package org.apache.hadoop.hdfs.web.resources;

/** Http operation parameter. */
public abstract class HttpOpParam<E extends Enum<E> & HttpOpParam.Op> extends EnumParam<E> {
  /** Default parameter value. */
  public static final String DEFAULT = NULL;

  /** Http operation types */
  public static enum Type {
    GET, PUT, POST, DELETE;
  }

  /** Http operation interface. */
  public static interface Op {
    /** @return the Http operation type. */
    public Type getType();

    /** @return true if the operation has output. */
    public boolean getDoOutput();

    /** @return true if the operation has output. */
    public int getExpectedHttpResponseCode();

    /** @return a URI query string. */
    public String toQueryString();
  }

  /**
   * Constructor.
   * @param str a string representation of the parameter value.
   */
  HttpOpParam(final Domain<E> domain, final E value) {
    super(domain, value);
  }
}