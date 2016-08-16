/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.slider.server.services.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.alias.CredentialProvider;
import org.apache.hadoop.security.alias.CredentialProviderFactory;
import org.apache.hadoop.security.alias.JavaKeyStoreProvider;
import org.apache.slider.common.SliderKeys;
import org.apache.slider.common.SliderXmlConfKeys;
import org.apache.slider.core.conf.AggregateConf;
import org.apache.slider.core.conf.MapOperations;
import org.apache.slider.core.exceptions.SliderException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertTrue;

/**
 *
 */
public class TestMultiThreadedStoreGeneration {

  public static final int NUM_THREADS = 30;
  @Rule
  public TemporaryFolder workDir = new TemporaryFolder();;

  private void setupCredentials(AggregateConf instanceDefinition,
                                String keyAlias, String trustAlias)
      throws Exception {
    Configuration conf = new Configuration();
    final Path jksPath = new Path(SecurityUtils.getSecurityDir(), "test.jks");
    final String ourUrl =
        JavaKeyStoreProvider.SCHEME_NAME + "://file" + jksPath.toUri();

    File file = new File(SecurityUtils.getSecurityDir(), "test.jks");
    file.delete();
    conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, ourUrl);

    instanceDefinition.getAppConf().credentials.put(ourUrl, new ArrayList<String>());

    CredentialProvider provider =
        CredentialProviderFactory.getProviders(conf).get(0);

    // create new aliases
    try {

      if (keyAlias != null) {
        char[] storepass = {'k', 'e', 'y', 'p', 'a', 's', 's'};
        provider.createCredentialEntry(
            keyAlias, storepass);
      }

      if (trustAlias != null) {
        char[] trustpass = {'t', 'r', 'u', 's', 't', 'p', 'a', 's', 's'};
        provider.createCredentialEntry(
            trustAlias, trustpass);
      }

      // write out so that it can be found in checks
      provider.flush();
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
  }


  @Test
  public void testMultiThreadedStoreGeneration() throws Exception {

    CertificateManager certMan = new CertificateManager();
    MapOperations compOperations = new MapOperations();
    File secDir = new File(workDir.getRoot(), SliderKeys.SECURITY_DIR);
    File keystoreFile = new File(secDir, SliderKeys.KEYSTORE_FILE_NAME);
    compOperations.put(SliderXmlConfKeys.KEY_KEYSTORE_LOCATION,
                       keystoreFile.getAbsolutePath());
    certMan.initialize(compOperations, "cahost", null, null);

    final CountDownLatch latch = new CountDownLatch(1);
    final List<SecurityStore> stores = new ArrayList<>();
    List<Thread> threads = new ArrayList<>();
    final AggregateConf instanceDefinition = new AggregateConf();

    setupCredentials(instanceDefinition,
                     SliderKeys.COMP_KEYSTORE_PASSWORD_ALIAS_DEFAULT, null);
    final MapOperations compOps = new MapOperations();
    compOps.put(SliderKeys.COMP_STORES_REQUIRED_KEY, "true");

    for (int i=0; i<NUM_THREADS; ++i) {
      final int finalI = i;
      Runnable runner = new Runnable() {
        public void run() {
          System.out.println ("----> In run");
          try {
            latch.await();
            SecurityStore[] stores1 = StoresGenerator.generateSecurityStores(
                "testhost",
                "container" + finalI,
                "component" + finalI,
                instanceDefinition,
                compOps);
            System.out.println ("----> stores1" + stores1);
            List<SecurityStore>
                securityStores =
                Arrays.asList(stores1);
            stores.addAll(securityStores);
          } catch (InterruptedException e) {
            e.printStackTrace();
          } catch (SliderException e) {
            e.printStackTrace();
          } catch (IOException e) {
            e.printStackTrace();
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
      };
      Thread thread = new Thread(runner, "TestThread" + i);
      threads.add(thread);
      thread.start();
    }
    latch.countDown();
    for (Thread t : threads) {
      t.join();
    }

    for (int i=0; i < NUM_THREADS; i++) {
      assertTrue("keystore " + i + " not generated", stores.get(i).getFile().exists());
    }
  }

}
