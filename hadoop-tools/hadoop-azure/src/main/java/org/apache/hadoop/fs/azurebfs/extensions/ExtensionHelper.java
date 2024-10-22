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

package org.apache.hadoop.fs.azurebfs.extensions;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Optional;
import java.util.function.Function;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;

/**
 * Classes to help with use of extensions, expecially those
 * implementing @{@link BoundDTExtension}.
 */
@InterfaceAudience.LimitedPrivate("authorization-subsystems")
@InterfaceStability.Unstable
public final class ExtensionHelper {

  private ExtensionHelper() {
  }

  /** Format string for a Canonical Service Name. */
  public static final String SERVICE_NAME_FORMAT = "abfs://%s/";

  /**
   * Given a URI, build the Canonical Service Name.
   * @param fsURI filesystem URI.
   * @return Canonical Service Name
   */
  public static String buildCanonicalServiceName(URI fsURI) {
    return String.format(SERVICE_NAME_FORMAT, fsURI.getHost());
  }
  /**
   * Given a URI, build the Canonical Service Name.
   * @param fsURI filesystem URI.
   * @return URI built from Canonical Service name
   * @throws RuntimeException if somehow the URI regeneration failes.
   */
  public static URI buildCanonicalServiceURI(URI fsURI) {
    try {
      return new URI(buildCanonicalServiceName(fsURI));
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * If the passed in extension class implements {@link BoundDTExtension}
   * then it will have its {@link BoundDTExtension#bind(URI, Configuration)}
   * method called.
   * @param extension extension to examine and maybe invoke
   * @param uri URI of the filesystem.
   * @param conf configuration of this extension.
   * @throws IOException failure during binding.
   */
  public static void bind(Object extension, URI uri, Configuration conf)
      throws IOException {
    if (extension instanceof BoundDTExtension) {
      ((BoundDTExtension) extension).bind(uri, conf);
    }
  }

  /**
   * Close an extension if it is closeable.
   * Any error raised is caught and logged.
   * @param extension extension instance.
   */
  public static void close(Object extension) {
    ifBoundDTExtension(extension,
        v -> {
          IOUtils.closeStreams(v);
          return  null;
        });
  }

  /**
   * Invoke {@link BoundDTExtension#getUserAgentSuffix()} or
   * return the default value.
   * @param extension extension to invoke
   * @param def default if the class is of the wrong type.
   * @return a user agent suffix
   */
  public static String getUserAgentSuffix(Object extension, String def) {
    return ifBoundDTExtension(extension, BoundDTExtension::getUserAgentSuffix)
        .orElse(def);
  }

  /**
   * Invoke {@link BoundDTExtension#getCanonicalServiceName()} or
   * return the default value.
   * @param extension extension to invoke
   * @param def default if the class is of the wrong type.
   * @return a canonical service name.
   */
  public static String getCanonicalServiceName(Object extension, String def) {
    return ifBoundDTExtension(extension, BoundDTExtension::getCanonicalServiceName)
        .orElse(def);
  }

  /**
   * Invoke an operation on an object if it implements the BoundDTExtension
   * interface; returns an optional value.
   * @param extension the extension to invoke.
   * @param fn function to apply
   * @param <V> return type of te function.
   * @return an optional value which, if not empty, contains the return value
   * of the invoked function. If empty: the object was not of a compatible
   * type.
   */
  public static <V> Optional<V> ifBoundDTExtension(Object extension,
      Function<? super BoundDTExtension, ? extends V> fn) {
    if (extension instanceof BoundDTExtension) {
      return Optional.of((BoundDTExtension) extension).map(fn);
    } else {
      return Optional.empty();
    }
  }

}
