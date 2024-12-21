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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.policy;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

public class TestPreferredIterator {

  @Before
  public void setup() {
    Logger rootLogger = LogManager.getRootLogger();
    rootLogger.setLevel(Level.DEBUG);
  }

  @Test
  public void testPreferredIterator() {
    List<String> examples =
        Arrays.asList("a", "b", "c", "d", "e", "f", "g", "h", "i", "j");
    PreferredIterator<String> iterator;
    // preferRatio=0, dropRatio=0
    iterator = new PreferredIterator<>(0, 0, examples);
    assertEquals(iterator, (items)->{
      Assert.assertEquals(10, items.size());
      Assert.assertEquals(examples, items);
      return null;
    });
    // preferRatio=0, dropRatio=0.2
    iterator = new PreferredIterator<>(0, 0.2f, examples);
    assertEquals(iterator, (items)->{
      Assert.assertEquals(8, items.size());
      Assert.assertEquals(examples.subList(0, 8), items);
      return null;
    });
    // preferRatio=0.2, dropRatio=0
    iterator = new PreferredIterator<>(0.2f, 0, examples);
    assertEquals(iterator, (items)->{
      Assert.assertEquals(10, items.size());
      Assert.assertEquals(examples.subList(2, 10), items.subList(2, 10));
      return null;
    });
    // preferRatio=0.2, dropRatio=0.3
    iterator = new PreferredIterator<>(0.2f, 0.3f, examples);
    assertEquals(iterator, (items)->{
      Assert.assertEquals(7, items.size());
      Assert.assertEquals(examples.subList(2, 7), items.subList(2, 7));
      return null;
    });
    // preferRatio=0.5, dropRatio=0
    iterator = new PreferredIterator<>(0.5f, 0, examples);
    assertEquals(iterator, (items)->{
      Assert.assertEquals(10, items.size());
      Assert.assertEquals(examples.subList(5, examples.size()),
          items.subList(5, examples.size()));
      return null;
    });
    // preferRatio=0.5, dropRatio=0
    iterator = new PreferredIterator<>(0.5f, 0.4f, examples);
    assertEquals(iterator, (items)->{
      Assert.assertEquals(6, items.size());
      Assert.assertEquals(examples.subList(5, 6), items.subList(5, 6));
      return null;
    });
    // preferRatio=1, dropRatio=0
    iterator = new PreferredIterator<>(1f, 0, examples);
    assertEquals(iterator, (items)->{
      Assert.assertEquals(10, items.size());
      return null;
    });
  }

  private void assertEquals(PreferredIterator<String> iterator,
      Function<List<String>, Void> checkFn) {
    for (int i = 0; i < 10; i++) {
      List<String> items = getItems(iterator);
      checkFn.apply(items);
    }
  }

  private List<String> getItems(PreferredIterator<String> iterator) {
    List<String> rst = new ArrayList<>();
    iterator.forEachRemaining(rst::add);
    iterator.reinitialize();
    return rst;
  }

}
