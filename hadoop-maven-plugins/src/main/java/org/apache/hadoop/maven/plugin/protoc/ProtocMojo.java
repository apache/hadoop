/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.maven.plugin.protoc;

import org.apache.hadoop.maven.plugin.util.Exec;
import org.apache.hadoop.maven.plugin.util.FileSetUtils;
import org.apache.maven.model.FileSet;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;

import java.io.File;
import java.util.ArrayList;
import java.util.List;


@Mojo(name="protoc", defaultPhase = LifecyclePhase.GENERATE_SOURCES)
public class ProtocMojo extends AbstractMojo {

  @Parameter(defaultValue="${project}", readonly=true)
  private MavenProject project;

  @Parameter
  private File[] imports;

  @Parameter(defaultValue="${project.build.directory}/generated-sources/java")
  private File output;

  @Parameter(required=true)
  private FileSet source;

  @Parameter(defaultValue="protoc")
  private String protocCommand;

  @Parameter(required=true)
  private String protocVersion;

  public void execute() throws MojoExecutionException {
    try {
      List<String> command = new ArrayList<String>();
      command.add(protocCommand);
      command.add("--version");
      Exec exec = new Exec(this);
      List<String> out = new ArrayList<String>();
      if (exec.run(command, out) == 127) {
        getLog().error("protoc, not found at: " + protocCommand);
        throw new MojoExecutionException("protoc failure");        
      } else {
        if (out.isEmpty()) {
          getLog().error("stdout: " + out);
          throw new MojoExecutionException(
              "'protoc --version' did not return a version");
        } else {
          if (!out.get(0).endsWith(protocVersion)) {
            throw new MojoExecutionException(
                "protoc version is '" + out.get(0) + "', expected version is '" 
                    + protocVersion + "'");            
          }
        }
      }
      if (!output.mkdirs()) {
        if (!output.exists()) {
          throw new MojoExecutionException("Could not create directory: " + 
            output);
        }
      }
      command = new ArrayList<String>();
      command.add(protocCommand);
      command.add("--java_out=" + output.getCanonicalPath());
      if (imports != null) {
        for (File i : imports) {
          command.add("-I" + i.getCanonicalPath());
        }
      }
      for (File f : FileSetUtils.convertFileSetToFiles(source)) {
        command.add(f.getCanonicalPath());
      }
      exec = new Exec(this);
      out = new ArrayList<String>();
      if (exec.run(command, out) != 0) {
        getLog().error("protoc compiler error");
        for (String s : out) {
          getLog().error(s);
        }
        throw new MojoExecutionException("protoc failure");
      }
    } catch (Throwable ex) {
      throw new MojoExecutionException(ex.toString(), ex);
    }
    project.addCompileSourceRoot(output.getAbsolutePath());
  }

}
