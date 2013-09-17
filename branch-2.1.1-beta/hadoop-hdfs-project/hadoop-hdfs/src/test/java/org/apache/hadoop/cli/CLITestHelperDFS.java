/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.cli;

import org.apache.hadoop.cli.util.CLICommandDFSAdmin;
import org.xml.sax.SAXException;

public class CLITestHelperDFS extends CLITestHelper {

  @Override
  protected TestConfigFileParser getConfigParser() {
    return new TestConfigFileParserDFS();
  }

  class TestConfigFileParserDFS extends CLITestHelper.TestConfigFileParser {
    @Override
    public void endElement(String uri, String localName, String qName)
        throws SAXException {
      if (qName.equals("dfs-admin-command")) {
        if (testCommands != null) {
          testCommands.add(new CLITestCmdDFS(charString,
              new CLICommandDFSAdmin()));
        } else if (cleanupCommands != null) {
          cleanupCommands.add(new CLITestCmdDFS(charString,
              new CLICommandDFSAdmin()));
        }
      } else {
        super.endElement(uri, localName, qName);
      }
    }
  }
}
