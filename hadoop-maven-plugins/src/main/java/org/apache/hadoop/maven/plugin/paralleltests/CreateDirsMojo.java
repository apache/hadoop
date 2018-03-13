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
package org.apache.hadoop.maven.plugin.paralleltests;

import java.io.File;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;


/**
 * Goal which creates the parallel-test directories.
 */
@Mojo(name="parallel-tests-createdir",
      defaultPhase = LifecyclePhase.GENERATE_TEST_RESOURCES)
public class CreateDirsMojo extends AbstractMojo {

  /**
   * Location of the test.build.dir.
   */
  @Parameter(defaultValue="${project.build.directory}/test-dir")
  private File testBuildDir;

  /**
   * Location of the test.build.data.
   */
  @Parameter(defaultValue="${project.build.directory}/test-dir")
  private File testBuildData;

  /**
   * Location of the test.build.data.
   */
  @Parameter(defaultValue="${project.build.directory}/tmp")
  private File hadoopTmpDir;

  /**
   * Thread count.
   */
  @Parameter(defaultValue="${testsThreadCount}")
  private String testsThreadCount;

  public void execute() throws MojoExecutionException {
    int numDirs=getTestsThreadCount();

    mkParallelDirs(testBuildDir, numDirs);
    mkParallelDirs(testBuildData, numDirs);
    mkParallelDirs(hadoopTmpDir, numDirs);
  }

  /**
   * Get the real number of parallel threads.
   * @return int number of threads
   */

  public int getTestsThreadCount() {
    int threadCount = 1;
    if (testsThreadCount != null) {
      String trimProp = testsThreadCount.trim();
      if (trimProp.endsWith("C")) {
        double multiplier = Double.parseDouble(
            trimProp.substring(0, trimProp.length()-1));
        double calculated = multiplier * ((double) Runtime
            .getRuntime()
            .availableProcessors());
        threadCount = calculated > 0d ? Math.max((int) calculated, 1) : 0;
      } else {
        threadCount = Integer.parseInt(testsThreadCount);
      }
    }
    return threadCount;
  }

  private void mkParallelDirs(File testDir, int numDirs)
      throws MojoExecutionException {
    for (int i=1; i<=numDirs; i++) {
      File newDir = new File(testDir, String.valueOf(i));
      if (!newDir.exists()) {
        getLog().info("Creating " + newDir.toString());
        if (!newDir.mkdirs()) {
          throw new MojoExecutionException("Unable to create "
              + newDir.toString());
        }
      }
    }
  }
}