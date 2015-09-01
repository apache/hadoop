/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hdfs.web.oauth2;

import org.apache.hadoop.util.Timer;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestAccessTokenTimer {
  @Test
  public void expireConversionWorks() {
    Timer mockTimer = mock(Timer.class);
    when(mockTimer.now())
        .thenReturn(5l);
    
    AccessTokenTimer timer = new AccessTokenTimer(mockTimer);
    
    timer.setExpiresIn("3");
    assertEquals(3005, timer.getNextRefreshMSSinceEpoch());
    
    assertTrue(timer.shouldRefresh());
  }
  
  @Test
  public void shouldRefreshIsCorrect() {
    Timer mockTimer = mock(Timer.class);
    when(mockTimer.now())
        .thenReturn(500l)
        .thenReturn(1000000l + 500l);
    
    AccessTokenTimer timer = new AccessTokenTimer(mockTimer);
    
    timer.setExpiresInMSSinceEpoch("1000000");
    
    assertFalse(timer.shouldRefresh());
    assertTrue(timer.shouldRefresh());
    
    verify(mockTimer, times(2)).now();
  } 
}
