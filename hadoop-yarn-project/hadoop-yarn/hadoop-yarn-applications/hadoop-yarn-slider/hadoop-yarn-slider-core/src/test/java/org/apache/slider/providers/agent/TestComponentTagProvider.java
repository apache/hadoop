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

package org.apache.slider.providers.agent;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TestComponentTagProvider {
  protected static final Logger log =
      LoggerFactory.getLogger(TestComponentTagProvider.class);

  @Test
  public void testTagProvider() throws Exception {
    ComponentTagProvider ctp = new ComponentTagProvider();
    Assert.assertEquals("", ctp.getTag(null, null));
    Assert.assertEquals("", ctp.getTag(null, "cid"));
    Assert.assertEquals("", ctp.getTag("comp1", null));

    Assert.assertEquals("1", ctp.getTag("comp1", "cid1"));
    Assert.assertEquals("2", ctp.getTag("comp1", "cid2"));
    Assert.assertEquals("3", ctp.getTag("comp1", "cid3"));
    ctp.releaseTag("comp1", "cid2");
    Assert.assertEquals("2", ctp.getTag("comp1", "cid22"));

    ctp.releaseTag("comp1", "cid4");
    ctp.recordAssignedTag("comp1", "cid5", "5");
    Assert.assertEquals("4", ctp.getTag("comp1", "cid4"));
    Assert.assertEquals("4", ctp.getTag("comp1", "cid4"));
    Assert.assertEquals("6", ctp.getTag("comp1", "cid6"));

    ctp.recordAssignedTag("comp1", "cid55", "5");
    Assert.assertEquals("5", ctp.getTag("comp1", "cid55"));

    ctp.recordAssignedTag("comp2", "cidb3", "3");
    Assert.assertEquals("1", ctp.getTag("comp2", "cidb1"));
    Assert.assertEquals("2", ctp.getTag("comp2", "cidb2"));
    Assert.assertEquals("4", ctp.getTag("comp2", "cidb4"));

    ctp.recordAssignedTag("comp2", "cidb5", "six");
    ctp.recordAssignedTag("comp2", "cidb5", "-55");
    ctp.recordAssignedTag("comp2", "cidb5", "tags");
    ctp.recordAssignedTag("comp2", "cidb5", null);
    ctp.recordAssignedTag("comp2", "cidb5", "");
    ctp.recordAssignedTag("comp2", "cidb5", "5");
    Assert.assertEquals("6", ctp.getTag("comp2", "cidb6"));

    ctp.recordAssignedTag("comp2", null, "5");
    ctp.recordAssignedTag(null, null, "5");
    ctp.releaseTag("comp1", null);
    ctp.releaseTag(null, "cid4");
    ctp.releaseTag(null, null);
  }

  @Test
  public void testTagProviderWithThread() throws Exception {
    ComponentTagProvider ctp = new ComponentTagProvider();
    Thread thread = new Thread(new Taggged(ctp));
    Thread thread2 = new Thread(new Taggged(ctp));
    Thread thread3 = new Thread(new Taggged(ctp));
    thread.start();
    thread2.start();
    thread3.start();
    ctp.getTag("comp1", "cid50");
    thread.join();
    thread2.join();
    thread3.join();
    Assert.assertEquals("101", ctp.getTag("comp1", "cid101"));
  }

  public class Taggged implements Runnable {
    private final ComponentTagProvider ctp;

    public Taggged(ComponentTagProvider ctp) {
      this.ctp = ctp;
    }

    public void run() {
      for (int i = 0; i < 100; i++) {
        String containerId = "cid" + (i + 1);
        this.ctp.getTag("comp1", containerId);
      }
      for (int i = 0; i < 100; i++) {
        String containerId = "cid" + (i + 1);
        this.ctp.getTag("comp1", containerId);
      }
      for (int i = 0; i < 100; i += 2) {
        String containerId = "cid" + (i + 1);
        this.ctp.releaseTag("comp1", containerId);
      }
      for (int i = 0; i < 100; i += 2) {
        String containerId = "cid" + (i + 1);
        this.ctp.getTag("comp1", containerId);
      }
    }
  }
}
