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

package org.apache.hadoop.conf;

import org.junit.Test;
import org.junit.Before;
import static org.junit.Assert.*;

import java.util.Collection;
import java.util.Arrays;

public class TestReconfiguration {
  private Configuration conf1;
  private Configuration conf2;

  private static final String PROP1 = "test.prop.one";
  private static final String PROP2 = "test.prop.two";
  private static final String PROP3 = "test.prop.three";
  private static final String PROP4 = "test.prop.four";
  private static final String PROP5 = "test.prop.five";

  private static final String VAL1 = "val1";
  private static final String VAL2 = "val2";

  @Before
  public void setUp () {
    conf1 = new Configuration();
    conf2 = new Configuration();
    
    // set some test properties
    conf1.set(PROP1, VAL1);
    conf1.set(PROP2, VAL1);
    conf1.set(PROP3, VAL1);

    conf2.set(PROP1, VAL1); // same as conf1
    conf2.set(PROP2, VAL2); // different value as conf1
    // PROP3 not set in conf2
    conf2.set(PROP4, VAL1); // not set in conf1

  }
  
  /**
   * Test ReconfigurationUtil.getChangedProperties.
   */
  @Test
  public void testGetChangedProperties() {
    Collection<ReconfigurationUtil.PropertyChange> changes = 
      ReconfigurationUtil.getChangedProperties(conf2, conf1);

    assertTrue("expected 3 changed properties but got " + changes.size(),
               changes.size() == 3);

    boolean changeFound = false;
    boolean unsetFound = false;
    boolean setFound = false;

    for (ReconfigurationUtil.PropertyChange c: changes) {
      if (c.prop.equals(PROP2) && c.oldVal != null && c.oldVal.equals(VAL1) &&
          c.newVal != null && c.newVal.equals(VAL2)) {
        changeFound = true;
      } else if (c.prop.equals(PROP3) && c.oldVal != null && c.oldVal.equals(VAL1) &&
          c.newVal == null) {
        unsetFound = true;
      } else if (c.prop.equals(PROP4) && c.oldVal == null &&
          c.newVal != null && c.newVal.equals(VAL1)) {
        setFound = true;
      } 
    }
    
    assertTrue("not all changes have been applied",
               changeFound && unsetFound && setFound);
  }

  /**
   * a simple reconfigurable class
   */
  public static class ReconfigurableDummy extends ReconfigurableBase 
  implements Runnable {
    public volatile boolean running = true;

    public ReconfigurableDummy(Configuration conf) {
      super(conf);
    }

    /**
     * {@inheritDoc}
     */
    @Override 
    public Collection<String> getReconfigurableProperties() {
      return Arrays.asList(PROP1, PROP2, PROP4);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void reconfigurePropertyImpl(String property, 
                                                     String newVal) {
      // do nothing
    }
    
    /**
     * Run until PROP1 is no longer VAL1.
     */
    @Override
    public void run() {
      while (running && getConf().get(PROP1).equals(VAL1)) {
        try {
          Thread.sleep(1);
        } catch (InterruptedException ignore) {
          // do nothing
        }
      }
    }

  }

  /**
   * Test reconfiguring a Reconfigurable.
   */
  @Test
  public void testReconfigure() {
    ReconfigurableDummy dummy = new ReconfigurableDummy(conf1);

    assertTrue(PROP1 + " set to wrong value ",
               dummy.getConf().get(PROP1).equals(VAL1));
    assertTrue(PROP2 + " set to wrong value ",
               dummy.getConf().get(PROP2).equals(VAL1));
    assertTrue(PROP3 + " set to wrong value ",
               dummy.getConf().get(PROP3).equals(VAL1));
    assertTrue(PROP4 + " set to wrong value ",
               dummy.getConf().get(PROP4) == null);
    assertTrue(PROP5 + " set to wrong value ",
               dummy.getConf().get(PROP5) == null);

    assertTrue(PROP1 + " should be reconfigurable ",
               dummy.isPropertyReconfigurable(PROP1));
    assertTrue(PROP2 + " should be reconfigurable ",
               dummy.isPropertyReconfigurable(PROP2));
    assertFalse(PROP3 + " should not be reconfigurable ",
                dummy.isPropertyReconfigurable(PROP3));
    assertTrue(PROP4 + " should be reconfigurable ",
               dummy.isPropertyReconfigurable(PROP4));
    assertFalse(PROP5 + " should not be reconfigurable ",
                dummy.isPropertyReconfigurable(PROP5));

    // change something to the same value as before
    {
      boolean exceptionCaught = false;
      try {
        dummy.reconfigureProperty(PROP1, VAL1);
        assertTrue(PROP1 + " set to wrong value ",
                   dummy.getConf().get(PROP1).equals(VAL1));
      } catch (ReconfigurationException e) {
        exceptionCaught = true;
      }
      assertFalse("received unexpected exception",
                  exceptionCaught);
    }

    // change something to null
    {
      boolean exceptionCaught = false;
      try {
        dummy.reconfigureProperty(PROP1, null);
        assertTrue(PROP1 + "set to wrong value ",
                   dummy.getConf().get(PROP1) == null);
      } catch (ReconfigurationException e) {
        exceptionCaught = true;
      }
      assertFalse("received unexpected exception",
                  exceptionCaught);
    }

    // change something to a different value than before
    {
      boolean exceptionCaught = false;
      try {
        dummy.reconfigureProperty(PROP1, VAL2);
        assertTrue(PROP1 + "set to wrong value ",
                   dummy.getConf().get(PROP1).equals(VAL2));
      } catch (ReconfigurationException e) {
        exceptionCaught = true;
      }
      assertFalse("received unexpected exception",
                  exceptionCaught);
    }

    // set unset property to null
    {
      boolean exceptionCaught = false;
      try {
        dummy.reconfigureProperty(PROP4, null);
        assertTrue(PROP4 + "set to wrong value ",
                   dummy.getConf().get(PROP4) == null);
      } catch (ReconfigurationException e) {
        exceptionCaught = true;
      }
      assertFalse("received unexpected exception",
                  exceptionCaught);
    }

    // set unset property
    {
      boolean exceptionCaught = false;
      try {
        dummy.reconfigureProperty(PROP4, VAL1);
        assertTrue(PROP4 + "set to wrong value ",
                   dummy.getConf().get(PROP4).equals(VAL1));
      } catch (ReconfigurationException e) {
        exceptionCaught = true;
      }
      assertFalse("received unexpected exception",
                  exceptionCaught);
    }

    // try to set unset property to null (not reconfigurable)
    {
      boolean exceptionCaught = false;
      try {
        dummy.reconfigureProperty(PROP5, null);
      } catch (ReconfigurationException e) {
        exceptionCaught = true;
      }
      assertTrue("did not receive expected exception",
                 exceptionCaught);
    }

    // try to set unset property to value (not reconfigurable)
    {
      boolean exceptionCaught = false;
      try {
        dummy.reconfigureProperty(PROP5, VAL1);
      } catch (ReconfigurationException e) {
        exceptionCaught = true;
      }
      assertTrue("did not receive expected exception",
                 exceptionCaught);
    }

    // try to change property to value (not reconfigurable)
    {
      boolean exceptionCaught = false;
      try {
        dummy.reconfigureProperty(PROP3, VAL2);
      } catch (ReconfigurationException e) {
        exceptionCaught = true;
      }
      assertTrue("did not receive expected exception",
                 exceptionCaught);
    }

    // try to change property to null (not reconfigurable)
    {
      boolean exceptionCaught = false;
      try {
        dummy.reconfigureProperty(PROP3, null);
      } catch (ReconfigurationException e) {
        exceptionCaught = true;
      }
      assertTrue("did not receive expected exception",
                 exceptionCaught);
    }
  }

  /**
   * Test whether configuration changes are visible in another thread.
   */
  @Test
  public void testThread() throws ReconfigurationException { 
    ReconfigurableDummy dummy = new ReconfigurableDummy(conf1);
    assertTrue(dummy.getConf().get(PROP1).equals(VAL1));
    Thread dummyThread = new Thread(dummy);
    dummyThread.start();
    try {
      Thread.sleep(500);
    } catch (InterruptedException ignore) {
      // do nothing
    }
    dummy.reconfigureProperty(PROP1, VAL2);

    long endWait = System.currentTimeMillis() + 2000;
    while (dummyThread.isAlive() && System.currentTimeMillis() < endWait) {
      try {
        Thread.sleep(50);
      } catch (InterruptedException ignore) {
        // do nothing
      }
    }

    assertFalse("dummy thread should not be alive",
                dummyThread.isAlive());
    dummy.running = false;
    try {
      dummyThread.join();
    } catch (InterruptedException ignore) {
      // do nothing
    }
    assertTrue(PROP1 + " is set to wrong value",
               dummy.getConf().get(PROP1).equals(VAL2));
    
  }

}