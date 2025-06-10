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
package org.apache.hadoop.io;

import java.io.IOException;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestBooleanWritable {

  @Test
  public void testCompareUnequalWritables() throws Exception {
    DataOutputBuffer bTrue = writeWritable(new BooleanWritable(true));
    DataOutputBuffer bFalse = writeWritable(new BooleanWritable(false));
    WritableComparator writableComparator =
      WritableComparator.get(BooleanWritable.class);

    assertEquals(0, compare(writableComparator, bTrue, bTrue));
    assertEquals(0, compare(writableComparator, bFalse, bFalse));
    assertEquals(1, compare(writableComparator, bTrue, bFalse));
    assertEquals(-1, compare(writableComparator, bFalse, bTrue));
  }

  private int compare(WritableComparator writableComparator,
      DataOutputBuffer buf1, DataOutputBuffer buf2) {
    return writableComparator.compare(buf1.getData(), 0, buf1.size(),
        buf2.getData(), 0, buf2.size());
  }

  protected DataOutputBuffer writeWritable(Writable writable)
      throws IOException {
    DataOutputBuffer out = new DataOutputBuffer(1024);
    writable.write(out);
    out.flush();
    return out;
  }
  
  /**
   * test {@link BooleanWritable} methods hashCode(), equals(), compareTo() 
   */
  @Test
  public void testCommonMethods() {    
    assertTrue(newInstance(true).equals(newInstance(true)),
        "testCommonMethods1 error !!!");
    assertTrue(newInstance(false).equals(newInstance(false)),
        "testCommonMethods2 error  !!!");
    assertFalse(newInstance(false).equals(newInstance(true)),
        "testCommonMethods3 error !!!");
    assertTrue(checkHashCode(newInstance(true), newInstance(true)),
        "testCommonMethods4 error !!!");
    assertFalse(checkHashCode(newInstance(true), newInstance(false)),
        "testCommonMethods5 error !!! ");
    assertTrue(newInstance(true).compareTo(newInstance(false)) > 0,
        "testCommonMethods6 error !!!" );
    assertTrue(newInstance(false).compareTo(newInstance(true)) < 0,
        "testCommonMethods7 error !!!" );
    assertTrue(newInstance(false).compareTo(newInstance(false)) == 0,
        "testCommonMethods8 error !!!" );
    assertEquals("true", newInstance(true).toString(),
        "testCommonMethods9 error !!!");
  }
  
  private boolean checkHashCode(BooleanWritable f, BooleanWritable s) {
    return f.hashCode() == s.hashCode();
  }    
  
  private static BooleanWritable newInstance(boolean flag) {
    return new BooleanWritable(flag);
  }
}
