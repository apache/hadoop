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
package org.apache.hadoop.maven.plugin.resourcegz;

import com.google.common.collect.Lists;
import org.apache.commons.io.IOUtils;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.zip.GZIPOutputStream;

/**
 * ResourceGzMojo will gzip files.
 * It is meant to be used for gzipping website resource files (e.g. .js, .css,
 * etc).  It takes an input directory, output directory, and extensions to
 * process and will generate the .gz files. Any additional directory structure
 * beyond the input directory is preserved in the output directory.
 */
@Mojo(name="resource-gz")
public class ResourceGzMojo extends AbstractMojo {

  /**
   * The input directory.  Will be searched recursively and its directory
   * structure will be maintaned in the outputDirectory.
   */
  @Parameter(property = "inputDirectory", required = true)
  private String inputDirectory;

  /**
   * The output directory.
   */
  @Parameter(property = "outputDirectory", required = true)
  private String outputDirectory;

  /**
   * A comma separated list of extensions to include.
   */
  @Parameter(property = "extensions", required = true)
  private String extensions;

  public void execute() throws MojoExecutionException, MojoFailureException {
    try {
      Path inputDir = new File(inputDirectory).toPath();
      File outputDir = new File(outputDirectory);
      List<String> exts = Lists.newArrayList(extensions.split(","));
      exts.replaceAll(String::trim);
      GZConsumer cons = new GZConsumer(inputDir.toFile(), outputDir);
      Files.walk(inputDir).filter(path -> {
        for (String ext : exts) {
          if (path.getFileName().toString().endsWith("." + ext)) {
            return true;
          }
        }
        return false;
      }).forEach(cons);
      if (cons.getThrowable() != null) {
        throw new MojoExecutionException(cons.getThrowable().toString(),
            cons.getThrowable());
      }
    } catch (Throwable t) {
      throw new MojoExecutionException(t.toString(), t);
    }
  }

  private class GZConsumer implements Consumer<Path> {
    private final File inputDir;
    private final File outputDir;
    private Throwable throwable;

    public GZConsumer(File inputDir, File outputDir) {
      this.inputDir = inputDir;
      this.outputDir = outputDir;
      this.throwable = null;
    }

    @Override
    public void accept(Path path) {
      if (throwable != null) {
        return;
      }
      try {
        File outFile = new File(outputDir, path.toFile().getCanonicalPath()
            .replaceFirst(Matcher.quoteReplacement(
                inputDir.getCanonicalPath()), "") + ".gz");
        if (outFile.getParentFile().isDirectory() ||
            outFile.getParentFile().mkdirs()) {
          try (
              GZIPOutputStream os = new GZIPOutputStream(
                  new FileOutputStream(outFile));
              BufferedReader is = Files.newBufferedReader(path)
          ) {
            getLog().info("Compressing " + path + " to " + outFile);
            IOUtils.copy(is, os);
          }
        } else {
          throw new IOException("Directory " + outFile.getParent()
              + " does not exist or was unable to be created");
        }
      } catch (Throwable t) {
        this.throwable = t;
      }
    }

    public Throwable getThrowable() {
      return throwable;
    }
  }
}
