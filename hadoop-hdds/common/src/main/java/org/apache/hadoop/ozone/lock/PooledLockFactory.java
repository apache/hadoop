/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.lock;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

/**
 * Pool factory to create {@code ActiveLock} instances.
 */
public class PooledLockFactory extends BasePooledObjectFactory<ActiveLock> {

  @Override
  public ActiveLock create() throws Exception {
    return ActiveLock.newInstance();
  }

  @Override
  public PooledObject<ActiveLock> wrap(ActiveLock activeLock) {
    return new DefaultPooledObject<>(activeLock);
  }

  @Override
  public void activateObject(PooledObject<ActiveLock> pooledObject) {
    pooledObject.getObject().resetCounter();
  }
}
