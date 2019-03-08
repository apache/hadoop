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
package org.apache.hadoop.ozone.recon;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.recon.spi.ReconContainerDBProvider;
import org.apache.hadoop.ozone.recon.spi.ContainerDBServiceProvider;
import org.apache.hadoop.ozone.recon.spi.impl.ContainerDBServiceProviderImpl;
import org.apache.hadoop.utils.MetadataStore;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;

/**
 * Guice controller that defines concrete bindings.
 */
public class ReconControllerModule extends AbstractModule {
  @Override
  protected void configure() {
    bind(OzoneConfiguration.class).toProvider(OzoneConfigurationProvider.class);
    bind(ReconHttpServer.class).in(Singleton.class);
    bind(MetadataStore.class).toProvider(ReconContainerDBProvider.class);
    bind(ContainerDBServiceProvider.class)
        .to(ContainerDBServiceProviderImpl.class);
  }


}
