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
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;

/**
 * A provider of credentials or password for Hadoop applications. Provides an
 * abstraction to separate credential storage from users of them. It
 * is intended to support getting or storing passwords in a variety of ways,
 * including third party bindings.
 * 
 * <code>CredentialProvider</code> implementations must be thread safe.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public abstract class CredentialProvider {
  public static final String CLEAR_TEXT_FALLBACK =
      CommonConfigurationKeysPublic.
          HADOOP_SECURITY_CREDENTIAL_CLEAR_TEXT_FALLBACK;

  /**
   * The combination of both the alias and the actual credential value.
   */
  public static class CredentialEntry {
    private final String alias;
    private final char[] credential;

    protected CredentialEntry(String alias,
                         char[] credential) {
      this.alias = alias;
      this.credential = credential;
    }

    public String getAlias() {
      return alias;
    }

    public char[] getCredential() {
      return credential;
    }

    public String toString() {
      StringBuilder buf = new StringBuilder();
      buf.append("alias(");
      buf.append(alias);
      buf.append(")=");
      if (credential == null) {
        buf.append("null");
      } else {
        for(char c: credential) {
          buf.append(c);
        }
      }
      return buf.toString();
    }
  }

  /**
   * Indicates whether this provider represents a store
   * that is intended for transient use - such as the UserProvider
   * is. These providers are generally used to provide job access to
   * passwords rather than for long term storage.
   * @return true if transient, false otherwise
   */
  public boolean isTransient() {
    return false;
  }

  /**
   * Ensures that any changes to the credentials are written to persistent
   * store.
   * @throws IOException
   */
  public abstract void flush() throws IOException;

  /**
   * Get the credential entry for a specific alias.
   * @param alias the name of a specific credential
   * @return the credentialEntry
   * @throws IOException
   */
  public abstract CredentialEntry getCredentialEntry(String alias) 
      throws IOException;

  /**
   * Get the aliases for all credentials.
   * @return the list of alias names
   * @throws IOException
   */
  public abstract List<String> getAliases() throws IOException;

  /**
   * Create a new credential. The given alias must not already exist.
   * @param name the alias of the credential
   * @param credential the credential value for the alias.
   * @throws IOException
   */
  public abstract CredentialEntry createCredentialEntry(String name, 
      char[] credential) throws IOException;

  /**
   * Delete the given credential.
   * @param name the alias of the credential to delete
   * @throws IOException
   */
  public abstract void deleteCredentialEntry(String name) throws IOException;

  /**
   * Does this provider require a password? This means that a password is
   * required for normal operation, and it has not been found through normal
   * means. If true, the password should be provided by the caller using
   * setPassword().
   * @return Whether or not the provider requires a password
   * @throws IOException
   */
  public boolean needsPassword() throws IOException {
    return false;
  }

  /**
   * If a password for the provider is needed, but is not provided, this will
   * return a warning and instructions for supplying said password to the
   * provider.
   * @return A warning and instructions for supplying the password
   */
  public String noPasswordWarning() {
    return null;
  }

  /**
   * If a password for the provider is needed, but is not provided, this will
   * return an error message and instructions for supplying said password to
   * the provider.
   * @return An error message and instructions for supplying the password
   */
  public String noPasswordError() {
    return null;
  }
}
