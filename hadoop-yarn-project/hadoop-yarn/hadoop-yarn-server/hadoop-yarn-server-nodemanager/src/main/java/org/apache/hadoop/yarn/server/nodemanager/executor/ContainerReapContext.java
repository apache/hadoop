/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.nodemanager.executor;

import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;

/**
 * Encapsulate the details needed to reap a container.
 */
public final class ContainerReapContext {

  private final Container container;
  private final String user;

  /**
   * Builder for the ContainerReapContext.
   */
  public static final class Builder {
    private Container builderContainer;
    private String builderUser;

    public Builder() {
    }

    /**
     * Set the container within the context.
     *
     * @param container the {@link Container}.
     * @return the Builder with the container set.
     */
    public Builder setContainer(Container container) {
      this.builderContainer = container;
      return this;
    }

    /**
     * Set the set within the context.
     *
     * @param user the user.
     * @return the Builder with the user set.
     */
    public Builder setUser(String user) {
      this.builderUser = user;
      return this;
    }

    /**
     * Builds the context with the attributes set.
     *
     * @return the context.
     */
    public ContainerReapContext build() {
      return new ContainerReapContext(this);
    }
  }

  private ContainerReapContext(Builder builder) {
    this.container = builder.builderContainer;
    this.user = builder.builderUser;
  }

  /**
   * Get the container set for the context.
   *
   * @return the {@link Container} set in the context.
   */
  public Container getContainer() {
    return container;
  }

  /**
   * Get the user set for the context.
   *
   * @return the user set in the context.
   */
  public String getUser() {
    return user;
  }
}
