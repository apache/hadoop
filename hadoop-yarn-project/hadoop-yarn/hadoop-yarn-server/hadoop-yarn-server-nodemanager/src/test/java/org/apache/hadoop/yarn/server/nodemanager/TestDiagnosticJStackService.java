/** * Licensed to the Apache Software Foundation (ASF) under one
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

package org.apache.hadoop.yarn.server.nodemanager;

import org.apache.hadoop.yarn.server.nodemanager.webapp.DiagnosticJStackService;
import org.junit.jupiter.api.Test;


import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;


public class TestDiagnosticJStackService {



    @Test
    public void testExtractPidsFromEmptyProcessOutput(){
        String psOutput = "";

        List<Long> pids = DiagnosticJStackService.extractPids(psOutput);

        assertTrue(pids.isEmpty());

    }

    @Test
    public void testExtractPidsFromOneProcessOutput(){
        String psOutput = "root       414  1.3  1.7 8124480 434520 ?      Sl   11:36";

        List<Long> pids = DiagnosticJStackService.extractPids(psOutput);

        assertEquals(414, pids.get(0));

    }

    @Test
    public void testExtractPidsFromMultipleProcessOutputs(){
        String psOutput = """
                root       414  1.3  1.7 8124480 434520 ?      Sl   11:36
                root       420  1.3  1.7 8124480 434520 ?      Sl   11:36
                """;

        List<Long> pids = DiagnosticJStackService.extractPids(psOutput);

        assertEquals(414, pids.get(0));
        assertEquals(420, pids.get(1));
    }

}
