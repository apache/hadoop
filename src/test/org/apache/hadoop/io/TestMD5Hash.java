/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import org.apache.hadoop.io.TestWritable;
import junit.framework.TestCase;
import java.security.MessageDigest;
import java.util.Random;

/** Unit tests for MD5Hash. */
public class TestMD5Hash extends TestCase {
  public TestMD5Hash(String name) { super(name); }

  private static final Random RANDOM = new Random();

  public static MD5Hash getTestHash() throws Exception {
    MessageDigest digest = MessageDigest.getInstance("MD5");
    byte[] buffer = new byte[1024];
    RANDOM.nextBytes(buffer);
    digest.update(buffer);
    return new MD5Hash(digest.digest());
  }

  public void testMD5Hash() throws Exception {
    MD5Hash md5Hash = getTestHash();

    MD5Hash md5Hash00
      = new MD5Hash(new byte[] {0,0,0,0,0,0,0,0, 0,0,0,0,0,0,0,0});

    MD5Hash md5HashFF
      = new MD5Hash(new byte[] {-1,-1,-1,-1,-1,-1,-1,-1,
                                -1,-1,-1,-1,-1,-1,-1,-1});

    // test i/o
    TestWritable.testWritable(md5Hash);
    TestWritable.testWritable(md5Hash00);
    TestWritable.testWritable(md5HashFF);

    // test equals()
    assertEquals(md5Hash, md5Hash);
    assertEquals(md5Hash00, md5Hash00);
    assertEquals(md5HashFF, md5HashFF);

    // test compareTo()
    assertTrue(md5Hash.compareTo(md5Hash) == 0);
    assertTrue(md5Hash00.compareTo(md5Hash) < 0);
    assertTrue(md5HashFF.compareTo(md5Hash) > 0);

    // test toString and string ctor
    assertEquals(md5Hash, new MD5Hash(md5Hash.toString()));
    assertEquals(md5Hash00, new MD5Hash(md5Hash00.toString()));
    assertEquals(md5HashFF, new MD5Hash(md5HashFF.toString()));

  }
	
}
