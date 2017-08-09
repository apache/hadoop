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

package org.apache.hadoop.yarn.webapp.view;

import org.apache.hadoop.classification.InterfaceAudience;

@InterfaceAudience.LimitedPrivate({"YARN", "MapReduce"})
public class LipsumBlock extends HtmlBlock {

  @Override
  public void render(Block html) {
    html.
      p().
        __("Lorem ipsum dolor sit amet, consectetur adipiscing elit.",
          "Vivamus eu dui in ipsum tincidunt egestas ac sed nibh.",
          "Praesent quis nisl lorem, nec interdum urna.",
          "Duis sagittis dignissim purus sed sollicitudin.",
          "Morbi quis diam eu enim semper suscipit.",
          "Nullam pretium faucibus sapien placerat tincidunt.",
          "Donec eget lorem at quam fermentum vulputate a ac purus.",
          "Cras ac dui felis, in pulvinar est.",
          "Praesent tempor est sed neque pulvinar dictum.",
          "Nullam magna augue, egestas luctus sollicitudin sed,",
          "venenatis nec turpis.",
          "Ut ante enim, congue sed laoreet et, accumsan id metus.",
          "Mauris tincidunt imperdiet est, sed porta arcu vehicula et.",
          "Etiam in nisi nunc.",
          "Phasellus vehicula scelerisque quam, ac dignissim felis euismod a.",
          "Proin eu ante nisl, vel porttitor eros.",
          "Aliquam gravida luctus augue, at scelerisque enim consectetur vel.",
          "Donec interdum tempor nisl, quis laoreet enim venenatis eu.",
          "Quisque elit elit, vulputate eget porta vel, laoreet ac lacus.").__();
  }
}
