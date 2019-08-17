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
package org.apache.hadoop.hdds.server.events;

/**
 * Executors defined the  way how an EventHandler should be called.
 * <p>
 * Executors are used only by the EventQueue and they do the thread separation
 * between the caller and the EventHandler.
 * <p>
 * Executors should guarantee that only one thread is executing one
 * EventHandler at the same time.
 *
 * @param <PAYLOAD> the payload type of the event.
 */
public interface EventExecutor<PAYLOAD> extends AutoCloseable {

  /**
   * Process an event payload.
   *
   * @param handler      the handler to process the payload
   * @param eventPayload to be processed.
   * @param publisher    to send response/other message forward to the chain.
   */
  void onMessage(EventHandler<PAYLOAD> handler,
      PAYLOAD eventPayload,
      EventPublisher
          publisher);

  /**
   * Return the number of the failed events.
   */
  long failedEvents();


  /**
   * Return the number of the processed events.
   */
  long successfulEvents();

  /**
   * Return the number of the not-yet processed events.
   */
  long queuedEvents();

  /**
   * The human readable name for the event executor.
   * <p>
   * Used in monitoring and logging.
   *
   */
  String getName();
}
