/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.recon.spi.impl;

import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_DB_DIR;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.utils.db.DBStore;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.ProvisionException;
import com.google.inject.Singleton;

/**
 * Tests the class that provides the instance of the DB Store used by Recon to
 * store its container - key data.
 */
public class TestReconContainerDBProvider {

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  private Injector injector;

  @Before
  public void setUp() throws IOException {
    tempFolder.create();
    injector = Guice.createInjector(new AbstractModule() {
      @Override
      protected void configure() {
        File dbDir = tempFolder.getRoot();
        OzoneConfiguration configuration = new OzoneConfiguration();
        configuration.set(OZONE_RECON_DB_DIR, dbDir.getAbsolutePath());
        bind(OzoneConfiguration.class).toInstance(configuration);
        bind(DBStore.class).toProvider(ReconContainerDBProvider.class).in(
            Singleton.class);
      }
    });
  }

  @Test
  public void testGet() throws Exception {

    ReconContainerDBProvider reconContainerDBProvider = injector.getInstance(
        ReconContainerDBProvider.class);
    DBStore dbStore = reconContainerDBProvider.get();
    assertNotNull(dbStore);

    ReconContainerDBProvider reconContainerDBProviderNew = new
        ReconContainerDBProvider();
    try {
      reconContainerDBProviderNew.get();
      fail();
    } catch (Exception e) {
      assertTrue(e instanceof ProvisionException);
    }
  }

}