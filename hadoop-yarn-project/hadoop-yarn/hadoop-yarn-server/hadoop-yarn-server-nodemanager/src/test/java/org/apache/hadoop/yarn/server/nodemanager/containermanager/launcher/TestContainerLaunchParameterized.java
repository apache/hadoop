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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher;

import org.apache.hadoop.util.Sets;
import org.apache.hadoop.util.Shell;
import org.junit.Assert;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Set;
import java.util.stream.Stream;

public class TestContainerLaunchParameterized {
  private static Stream<Arguments> inputForGetEnvDependenciesLinux() {
    return Stream.of(
        Arguments.of(null, asSet()),
        Arguments.of("", asSet()),
        Arguments.of("A", asSet()),
        Arguments.of("\\$A", asSet()),
        Arguments.of("$$", asSet()),
        Arguments.of("$1", asSet()),
        Arguments.of("handle \"'$A'\" simple quotes", asSet()),
        Arguments.of("handle \" escaped \\\" '${A}'\" simple quotes", asSet()),
        Arguments
            .of("$ crash test for StringArrayOutOfBoundException", asSet()),
        Arguments.of("${ crash test for StringArrayOutOfBoundException",
            asSet()),
        Arguments.of("${# crash test for StringArrayOutOfBoundException",
            asSet()),
        Arguments
            .of("crash test for StringArrayOutOfBoundException $", asSet()),
        Arguments.of("crash test for StringArrayOutOfBoundException ${",
            asSet()),
        Arguments.of("crash test for StringArrayOutOfBoundException ${#",
            asSet()),
        Arguments.of("$A", asSet("A")),
        Arguments.of("${A}", asSet("A")),
        Arguments.of("${#A[*]}", asSet("A")),
        Arguments.of("in the $A midlle", asSet("A")),
        Arguments.of("${A:-$B} var in var", asSet("A", "B")),
        Arguments.of("${A}$B var outside var", asSet("A", "B")),
        Arguments.of("$A:$B:$C:pathlist var", asSet("A", "B", "C")),
        Arguments
            .of("${A}/foo/bar:$B:${C}:pathlist var", asSet("A", "B", "C")),
        // https://www.gnu.org/software/bash/manual/html_node/Shell-Parameter-Expansion.html
        Arguments.of("${parameter:-word}", asSet("parameter")),
        Arguments.of("${parameter:=word}", asSet("parameter")),
        Arguments.of("${parameter:?word}", asSet("parameter")),
        Arguments.of("${parameter:+word}", asSet("parameter")),
        Arguments.of("${parameter:71}", asSet("parameter")),
        Arguments.of("${parameter:71:30}", asSet("parameter")),
        Arguments.of("!{prefix*}", asSet()),
        Arguments.of("${!prefix@}", asSet()),
        Arguments.of("${!name[@]}", asSet()),
        Arguments.of("${!name[*]}", asSet()),
        Arguments.of("${#parameter}", asSet("parameter")),
        Arguments.of("${parameter#word}", asSet("parameter")),
        Arguments.of("${parameter##word}", asSet("parameter")),
        Arguments.of("${parameter%word}", asSet("parameter")),
        Arguments.of("${parameter/pattern/string}", asSet("parameter")),
        Arguments.of("${parameter^pattern}", asSet("parameter")),
        Arguments.of("${parameter^^pattern}", asSet("parameter")),
        Arguments.of("${parameter,pattern}", asSet("parameter")),
        Arguments.of("${parameter,,pattern}", asSet("parameter")),
        Arguments.of("${parameter@o}", asSet("parameter")),
        Arguments.of("${parameter:-${another}}", asSet("parameter", "another")),
        Arguments
            .of("${FILES:-$(git diff --name-only \"${GIT_REVISION}..HEAD\"" +
                    " | grep \"java$\" | grep -iv \"test\")}",
                asSet("FILES", "GIT_REVISION")),
        Arguments.of("handle '${A}' simple quotes", asSet("A")),
        Arguments.of("handle '${A} $B ${C:-$D}' simple quotes",
            asSet("A", "B", "C", "D")),
        Arguments.of("handle \"'${A}'\" double and single quotes", asSet()),
        Arguments.of("handle \"'\\${A}'\" double and single quotes", asSet()),
        Arguments.of("handle '\\${A} \\$B \\${C:-D}' single quotes", asSet()),
        Arguments.of("handle \"${A}\" double quotes", asSet("A")),
        Arguments.of("handle \"${A} $B ${C:-$D}\" double quotes",
            asSet("A", "B", "C", "D"))
    );
  }

  @ParameterizedTest
  @MethodSource("inputForGetEnvDependenciesLinux")
  void testGetEnvDependenciesLinux(String input,
                                   Set<String> expected) {
    ContainerLaunch.ShellScriptBuilder bash =
        ContainerLaunch.ShellScriptBuilder.create(Shell.OSType.OS_TYPE_LINUX);
    Assert.assertEquals("Failed to parse " + input, expected,
        bash.getEnvDependencies(input));
  }

  private static Stream<Arguments> inputForGetEnvDependenciesWin() {
    return Stream.of(
        Arguments.of(null, asSet()),
        Arguments.of("", asSet()),
        Arguments.of("A", asSet()),
        Arguments.of("%%%%%%", asSet()),
        Arguments.of("%%A%", asSet()),
        Arguments.of("%A", asSet()),
        Arguments.of("%A:", asSet()),
        Arguments.of("%A%", asSet("A")),
        Arguments.of("%:%", asSet(":")),
        Arguments.of("%:A%", asSet()),
        Arguments.of("%%%A%", asSet("A")),
        Arguments.of("%%C%A%", asSet("A")),
        Arguments.of("%A:~-1%", asSet("A")),
        Arguments.of("%A:%", asSet("A")),
        Arguments.of("%A:whatever:a:b:%", asSet("A")),
        Arguments.of("%A%B%", asSet("A")),
        Arguments.of("%A%%%%%B%", asSet("A")),
        Arguments.of("%A%%B%", asSet("A", "B")),
        Arguments.of("%A%%%%B%", asSet("A", "B")),
        Arguments.of("%A%:%B%:%C%:pathlist var", asSet("A", "B", "C")),
        Arguments.of("%A%\\\\foo\\\\bar:%B%:%C%:pathlist var",
            asSet("A", "B", "C"))
    );
  }

  @ParameterizedTest
  @MethodSource("inputForGetEnvDependenciesWin")
  void testGetEnvDependenciesWin(String input,
                                 Set<String> expected) {
    ContainerLaunch.ShellScriptBuilder win =
        ContainerLaunch.ShellScriptBuilder.create(Shell.OSType.OS_TYPE_WIN);
    Assert.assertEquals("Failed to parse " + input, expected,
        win.getEnvDependencies(input));
  }

  private static Set<String> asSet(String... str) {
    return Sets.newHashSet(str);
  }
}
