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

import java.io.PrintWriter;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.yarn.webapp.MimeType;
import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.WebAppException;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

@InterfaceAudience.LimitedPrivate({"YARN", "MapReduce"})
public abstract class HtmlBlock extends TextView implements SubView {

  public class Block extends Hamlet {
    Block(PrintWriter out, int level, boolean wasInline) {
      super(out, level, wasInline);
    }

    @Override
    protected void subView(Class<? extends SubView> cls) {
      context().set(nestLevel(), wasInline());
      render(cls);
      setWasInline(context().wasInline());
    }
  }

  private Block block;

  private Block block() {
    if (block == null) {
      block = new Block(writer(), context().nestLevel(), context().wasInline());
    }
    return block;
  }

  protected HtmlBlock() {
    this(null);
  }

  protected HtmlBlock(ViewContext ctx) {
    super(ctx, MimeType.HTML);
  }

  @Override
  public void render() {
    int nestLevel = context().nestLevel();
    LOG.debug("Rendering {} @{}", getClass(), nestLevel);
    render(block());
    if (block.nestLevel() != nestLevel) {
      throw new WebAppException("Error rendering block: nestLevel="+
                                block.nestLevel() +" expected "+ nestLevel);
    }
    context().set(nestLevel, block.wasInline());
  }

  @Override
  public void renderPartial() {
    render();
  }

  /**
   * Render a block of html. To be overridden by implementation.
   * @param html the block to render
   */
  protected abstract void render(Block html);
}
