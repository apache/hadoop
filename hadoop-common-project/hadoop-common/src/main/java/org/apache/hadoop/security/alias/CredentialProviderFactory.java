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

package org.apache.hadoop.security.alias;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ServiceLoader;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.PathIOException;

/**
 * A factory to create a list of CredentialProvider based on the path given in a
 * Configuration. It uses a service loader interface to find the available
 * CredentialProviders and create them based on the list of URIs.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public abstract class CredentialProviderFactory {
  public static final String CREDENTIAL_PROVIDER_PATH =
      CommonConfigurationKeysPublic.HADOOP_SECURITY_CREDENTIAL_PROVIDER_PATH;

  public abstract CredentialProvider createProvider(URI providerName,
                                             Configuration conf
                                             ) throws IOException;

  private static final ServiceLoader<CredentialProviderFactory> serviceLoader =
      ServiceLoader.load(CredentialProviderFactory.class,
          CredentialProviderFactory.class.getClassLoader());

  // Iterate through the serviceLoader to avoid lazy loading.
  // Lazy loading would require synchronization in concurrent use cases.
  static {
    Iterator<CredentialProviderFactory> iterServices = serviceLoader.iterator();
    while (iterServices.hasNext()) {
      iterServices.next();
    }
  }

  /**
   * Fail fast on any recursive load of credential providers, which can
   * happen if the FS itself triggers the load.
   * A simple boolean could be used here, as the synchronized block ensures
   * that only one thread can be active at a time. An atomic is used
   * for rigorousness.
   */
  private static final AtomicBoolean SERVICE_LOADER_LOCKED = new AtomicBoolean(false);

  public static List<CredentialProvider> getProviders(Configuration conf
                                               ) throws IOException {
    List<CredentialProvider> result = new ArrayList<>();
    for(String path: conf.getStringCollection(CREDENTIAL_PROVIDER_PATH)) {
      try {
        URI uri = new URI(path);
        boolean found = false;
        // Iterate serviceLoader in a synchronized block since
        // serviceLoader iterator is not thread-safe.
        synchronized (serviceLoader) {
          try {
            if (SERVICE_LOADER_LOCKED.getAndSet(true)) {
              throw new PathIOException(path,
                  "Recursive load of credential provider; " +
                      "if loading a JCEKS file, this means that the filesystem connector is " +
                      "trying to load the same file");
            }
            for (CredentialProviderFactory factory : serviceLoader) {
              CredentialProvider kp = factory.createProvider(uri, conf);
              if (kp != null) {
                result.add(kp);
                found = true;
                break;
              }
            }
          } finally {
            SERVICE_LOADER_LOCKED.set(false);
          }
        }
        if (!found) {
          throw new IOException("No CredentialProviderFactory for " + uri + " in " +
              CREDENTIAL_PROVIDER_PATH);
        }
      } catch (URISyntaxException error) {
        throw new IOException("Bad configuration of " + CREDENTIAL_PROVIDER_PATH +
            " at " + path, error);
      }
    }
    return result;
  }

  /**
   * Get the CredentialProvider for a given provider URI.
   *
   * @param conf The configuration object
   * @param providerURI The URI of the provider path
   * @return The CredentialProvider
   * @throws IOException If an I/O error occurs
   */
  public static CredentialProvider getProvider(Configuration conf,
      URI providerUri) throws IOException {
    synchronized (serviceLoader) {
      for (CredentialProviderFactory factory : serviceLoader) {
        CredentialProvider kp = factory.createProvider(providerUri, conf);
        if (kp != null) {
          return kp;
        }
      }
    }
    throw new IOException(
        "No CredentialProviderFactory for " + providerUri);
  }
}
