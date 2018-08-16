/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

package org.apache.hadoop.yarn.submarine.client.cli;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.yarn.submarine.common.ClientContext;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.submarine.common.exception.SubmarineException;

import java.io.IOException;

public abstract class AbstractCli implements Tool {
  protected ClientContext clientContext;

  public AbstractCli(ClientContext cliContext) {
    this.clientContext = cliContext;
  }

  @Override
  public abstract int run(String[] args)
      throws ParseException, IOException, YarnException, InterruptedException,
      SubmarineException;

  @Override
  public void setConf(Configuration conf) {
    clientContext.setSubmarineConfig(conf);
  }

  @Override
  public Configuration getConf() {
    return clientContext.getSubmarineConfig();
  }
}
