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
package org.apache.hadoop.crypto.key;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class FailureInjectingJavaKeyStoreProvider extends JavaKeyStoreProvider {

  public static final String SCHEME_NAME = "failjceks";

  private boolean backupFail = false;
  private boolean writeFail = false;
  FailureInjectingJavaKeyStoreProvider(JavaKeyStoreProvider prov) {
    super(prov);
  }

  public void setBackupFail(boolean b) {
    backupFail = b;
  }

  public void setWriteFail(boolean b) {
    backupFail = b;
  }

  // Failure injection methods..
  @Override
  public void writeToNew(Path newPath) throws IOException {
    if (writeFail) {
      throw new IOException("Injecting failure on write");
    }
    super.writeToNew(newPath);
  }

  @Override
  public boolean backupToOld(Path oldPath) throws IOException {
    if (backupFail) {
      throw new IOException("Inejection Failure on backup");
    }
    return super.backupToOld(oldPath);
  }

  public static class Factory extends KeyProviderFactory {
    @Override
    public KeyProvider createProvider(URI providerName,
        Configuration conf) throws IOException {
      if (SCHEME_NAME.equals(providerName.getScheme())) {
        try {
          return new FailureInjectingJavaKeyStoreProvider(
              (JavaKeyStoreProvider) new JavaKeyStoreProvider.Factory()
                  .createProvider(
                      new URI(providerName.toString().replace(SCHEME_NAME,
                          JavaKeyStoreProvider.SCHEME_NAME)), conf));
        } catch (URISyntaxException e) {
          throw new RuntimeException(e);
        }
      }
      return null;
    }
  }
}
