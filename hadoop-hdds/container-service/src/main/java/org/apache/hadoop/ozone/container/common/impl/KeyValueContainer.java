/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.container.common.impl;


import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;


import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.NoSuchAlgorithmException;


/**
 * Class to perform KeyValue Container operations.
 */
public class KeyValueContainer implements Container {

  static final Logger LOG =
      LoggerFactory.getLogger(Container.class);

  private KeyValueContainerData containerData;

  public KeyValueContainer(KeyValueContainerData containerData) {
    Preconditions.checkNotNull(containerData, "KeyValueContainerData cannot " +
        "be null");
    this.containerData = containerData;
  }

  @Override
  public void create(ContainerData cData) throws StorageContainerException {


  }

  @Override
  public void delete(boolean forceDelete)
      throws StorageContainerException {

  }

  @Override
  public void update(boolean forceUpdate)
      throws StorageContainerException {

  }

  @Override
  public ContainerData getContainerData()  {
    return containerData;
  }

  @Override
  public void close() throws StorageContainerException,
      NoSuchAlgorithmException {

  }

}
