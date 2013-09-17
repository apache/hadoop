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

import java.net.InetSocketAddress;

/** InetSocketAddressParam parameter. */
abstract class InetSocketAddressParam
    extends Param<InetSocketAddress, InetSocketAddressParam.Domain> {
  InetSocketAddressParam(final Domain domain, final InetSocketAddress value) {
    super(domain, value);
  }

  @Override
  public String toString() {
    return getName() + "=" + Domain.toString(getValue());
  }

  /** @return the parameter value as a string */
  @Override
  public String getValueString() {
    return Domain.toString(getValue());
  }

  /** The domain of the parameter. */
  static final class Domain extends Param.Domain<InetSocketAddress> {
    Domain(final String paramName) {
      super(paramName);
    }

    @Override
    public String getDomain() {
      return "<HOST:PORT>";
    }

    @Override
    InetSocketAddress parse(final String str) {
      if (str == null) {
        throw new IllegalArgumentException("The input string is null: expect "
            + getDomain());
      }
      final int i = str.indexOf(':');
      if (i < 0) {
        throw new IllegalArgumentException("Failed to parse \"" + str
            + "\" as " + getDomain() + ": the ':' character not found.");
      } else if (i == 0) {
        throw new IllegalArgumentException("Failed to parse \"" + str
            + "\" as " + getDomain() + ": HOST is empty.");
      } else if (i == str.length() - 1) {
        throw new IllegalArgumentException("Failed to parse \"" + str
            + "\" as " + getDomain() + ": PORT is empty.");
      }

      final String host = str.substring(0, i);
      final int port;
      try {
        port = Integer.parseInt(str.substring(i + 1));
      } catch(NumberFormatException e) {
        throw new IllegalArgumentException("Failed to parse \"" + str
            + "\" as " + getDomain() + ": the ':' position is " + i
            + " but failed to parse PORT.", e);
      }

      try {
        return new InetSocketAddress(host, port);
      } catch(Exception e) {
        throw new IllegalArgumentException("Failed to parse \"" + str
            + "\": cannot create InetSocketAddress(host=" + host
            + ", port=" + port + ")", e);
      }
    }

    /** Convert an InetSocketAddress to a HOST:PORT String. */
    static String toString(final InetSocketAddress addr) {
      return addr.getHostName() + ":" + addr.getPort();
    }
  }
}
