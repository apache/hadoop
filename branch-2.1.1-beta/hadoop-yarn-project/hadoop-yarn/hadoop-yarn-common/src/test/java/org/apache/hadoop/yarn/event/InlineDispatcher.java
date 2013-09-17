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

package org.apache.hadoop.yarn.event;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;

@SuppressWarnings({"unchecked", "rawtypes"})
public class InlineDispatcher extends AsyncDispatcher {
  private static final Log LOG = LogFactory.getLog(InlineDispatcher.class);

  private class TestEventHandler implements EventHandler {
    @Override
    public void handle(Event event) {
      dispatch(event);
    }
  }
  @Override
  protected void dispatch(Event event) {
      LOG.info("Dispatching the event " + event.getClass().getName() + "."
        + event.toString());

    Class<? extends Enum> type = event.getType().getDeclaringClass();
    if (eventDispatchers.get(type) != null) {
      eventDispatchers.get(type).handle(event);
    }
  }
  @Override
  public EventHandler getEventHandler() {
    return new TestEventHandler();
  }
  
  public static class EmptyEventHandler implements EventHandler<Event> {
    @Override
    public void handle(Event event) {
      //do nothing      
    }    
  }
}