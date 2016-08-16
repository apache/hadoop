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

package org.apache.slider.server.services.workflow;

import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.service.ServiceStateException;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MockService extends AbstractService {
  private final boolean fail;
  private final int lifespan;
  private final ExecutorService executorService =
      Executors.newSingleThreadExecutor();

  MockService() {
    this("mock", false, -1);
  }

  MockService(String name, boolean fail, int lifespan) {
    super(name);
    this.fail = fail;
    this.lifespan = lifespan;
  }

  @Override
  protected void serviceStart() throws Exception {
    //act on the lifespan here
    if (lifespan > 0) {
      executorService.submit(new Runnable() {
        @Override
        public void run() {
          try {
            Thread.sleep(lifespan);
          } catch (InterruptedException ignored) {

          }
          finish();
        }
      });
    } else {
      if (lifespan == 0) {
        finish();
      } else {
        //continue until told not to
      }
    }
  }

  void finish() {
    if (fail) {
      ServiceStateException e =
          new ServiceStateException(getName() + " failed");

      noteFailure(e);
      stop();
      throw e;
    } else {
      stop();
    }
  }

}
