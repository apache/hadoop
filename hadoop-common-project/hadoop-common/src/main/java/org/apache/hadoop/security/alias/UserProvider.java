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
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.Charsets;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * A CredentialProvider for UGIs. It uses the credentials object associated
 * with the current user to find credentials. This provider is created using a
 * URI of "user:///".
 */
@InterfaceAudience.Private
public class UserProvider extends CredentialProvider {
  public static final String SCHEME_NAME = "user";
  private final UserGroupInformation user;
  private final Credentials credentials;

  private UserProvider() throws IOException {
    user = UserGroupInformation.getCurrentUser();
    credentials = user.getCredentials();
  }

  @Override
  public boolean isTransient() {
    return true;
  }

  @Override
  public CredentialEntry getCredentialEntry(String alias) {
    byte[] bytes = credentials.getSecretKey(new Text(alias));
    if (bytes == null) {
      return null;
    }
    return new CredentialEntry(
        alias, new String(bytes, Charsets.UTF_8).toCharArray());
  }

  @Override
  public CredentialEntry createCredentialEntry(String name, char[] credential) 
      throws IOException {
    Text nameT = new Text(name);
    if (credentials.getSecretKey(nameT) != null) {
      throw new IOException("Credential " + name + 
          " already exists in " + this);
    }
    credentials.addSecretKey(new Text(name), 
        new String(credential).getBytes("UTF-8"));
    return new CredentialEntry(name, credential);
  }

  @Override
  public void deleteCredentialEntry(String name) throws IOException {
    byte[] cred = credentials.getSecretKey(new Text(name));
    if (cred != null) {
      credentials.removeSecretKey(new Text(name));
    }
    else {
      throw new IOException("Credential " + name + 
          " does not exist in " + this);
    }
  }

  @Override
  public String toString() {
    return SCHEME_NAME + ":///";
  }

  @Override
  public void flush() {
    user.addCredentials(credentials);
  }

  public static class Factory extends CredentialProviderFactory {

    @Override
    public CredentialProvider createProvider(URI providerName,
                                      Configuration conf) throws IOException {
      if (SCHEME_NAME.equals(providerName.getScheme())) {
        return new UserProvider();
      }
      return null;
    }
  }

  @Override
  public List<String> getAliases() throws IOException {
    List<String> list = new ArrayList<String>();
    List<Text> aliases = credentials.getAllSecretKeys();
    for (Text key : aliases) {
      list.add(key.toString());
    }
    return list;
  }
}
