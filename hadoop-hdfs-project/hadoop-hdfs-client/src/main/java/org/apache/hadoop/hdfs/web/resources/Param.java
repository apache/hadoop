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

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Arrays;
import java.util.Comparator;


/** Base class of parameters. */
public abstract class Param<T, D extends Param.Domain<T>> {
  static final String NULL = "null";

  static final Comparator<Param<?,?>> NAME_CMP = new Comparator<Param<?,?>>() {
    @Override
    public int compare(Param<?, ?> left, Param<?, ?> right) {
      return left.getName().compareTo(right.getName());
    }
  };

  /** Convert the parameters to a sorted String.
   *
   * @param separator URI parameter separator character
   * @param parameters parameters to encode into a string
   * @return the encoded URI string
   */
  public static String toSortedString(final String separator,
      final Param<?, ?>... parameters) {
    Arrays.sort(parameters, NAME_CMP);
    final StringBuilder b = new StringBuilder();
    try {
      for(Param<?, ?> p : parameters) {
        if (p.getValue() != null) {
          b.append(separator)
              .append(URLEncoder.encode(p.getName(), "UTF-8"))
              .append("=")
              .append(URLEncoder.encode(p.getValueString(), "UTF-8"));
        }
      }
    } catch (UnsupportedEncodingException e) {
      // Sane systems know about UTF-8, so this should never happen.
      throw new RuntimeException(e);
    }
    return b.toString();
  }

  /** The domain of the parameter. */
  final D domain;
  /** The actual parameter value. */
  final T value;

  Param(final D domain, final T value) {
    this.domain = domain;
    this.value = value;
  }

  /** @return the parameter value. */
  public final T getValue() {
    return value;
  }

  /** @return the parameter value as a string */
  public abstract String getValueString();

  /** @return the parameter name. */
  public abstract String getName();

  @Override
  public String toString() {
    return getName() + "=" + value;
  }

  /** Base class of parameter domains. */
  static abstract class Domain<T> {
    /** Parameter name. */
    final String paramName;

    Domain(final String paramName) {
      this.paramName = paramName;
    }

    /** @return the parameter name. */
    public final String getParamName() {
      return paramName;
    }

    /** @return a string description of the domain of the parameter. */
    public abstract String getDomain();

    /** @return the parameter value represented by the string. */
    abstract T parse(String str);

    /** Parse the given string.
     * @return the parameter value represented by the string.
     */
    public final T parse(final String varName, final String str) {
      try {
        return str != null && str.trim().length() > 0 ? parse(str) : null;
      } catch(Exception e) {
        throw new IllegalArgumentException("Failed to parse \"" + str
            + "\" for the parameter " + varName
            + ".  The value must be in the domain " + getDomain(), e);
      }
    }
  }
}
