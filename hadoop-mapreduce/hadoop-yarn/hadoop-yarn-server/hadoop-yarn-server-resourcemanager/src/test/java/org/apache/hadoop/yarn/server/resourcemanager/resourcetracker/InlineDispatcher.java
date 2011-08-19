package org.apache.hadoop.yarn.server.resourcemanager.resourcetracker;

import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;

class InlineDispatcher extends AsyncDispatcher {
  private class InlineEventHandler implements EventHandler {
    private final InlineDispatcher dispatcher;
    public InlineEventHandler(InlineDispatcher dispatcher) {
      this.dispatcher = dispatcher;
    }
    @Override
    public void handle(Event event) {
      this.dispatcher.dispatch(event);
    }
  }
  public void dispatch(Event event) {
    super.dispatch(event);
  }
  @Override
  public EventHandler getEventHandler() {
    return new InlineEventHandler(this);
  }

  static class EmptyEventHandler implements EventHandler<Event> {
    @Override
    public void handle(Event event) {
      ; // ignore
    }
  }
}