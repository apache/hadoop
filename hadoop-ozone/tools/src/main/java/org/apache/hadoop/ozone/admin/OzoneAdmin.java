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
package org.apache.hadoop.ozone.admin;

import org.apache.hadoop.hdds.cli.GenericCli;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.admin.om.OMAdmin;
import org.apache.hadoop.util.NativeCodeLoader;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import picocli.CommandLine;

/**
 * Ozone Admin Command line tool.
 */
@CommandLine.Command(name = "ozone admin",
    hidden = true,
    description = "Developer tools for Ozone Admin operations",
    versionProvider = HddsVersionProvider.class,
    subcommands = {
        OMAdmin.class
    },
    mixinStandardHelpOptions = true)
public class OzoneAdmin extends GenericCli {

    private OzoneConfiguration ozoneConf;

    public OzoneConfiguration getOzoneConf() {
        if (ozoneConf == null) {
            ozoneConf = createOzoneConfiguration();
        }
        return ozoneConf;
    }

    /**
     * Main for the Ozone Admin shell Command handling.
     *
     * @param argv - System Args Strings[]
     * @throws Exception
     */
    public static void main(String[] argv) throws Exception {

        LogManager.resetConfiguration();
        Logger.getRootLogger().setLevel(Level.INFO);
        Logger.getRootLogger()
            .addAppender(new ConsoleAppender(new PatternLayout("%m%n")));
        Logger.getLogger(NativeCodeLoader.class).setLevel(Level.ERROR);

        new OzoneAdmin().run(argv);
    }
}
