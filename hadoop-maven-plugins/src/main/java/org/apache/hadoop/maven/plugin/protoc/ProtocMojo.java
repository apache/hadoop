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
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;

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

  @Parameter(defaultValue =
      "${project.build.directory}/hadoop-maven-plugins-protoc-checksums.json")
  private String checksumPath;

  /**
   * Compares include and source file checksums against previously computed
   * checksums stored in a json file in the build directory.
   */
  public class ChecksumComparator {

    private final Map<String, Long> storedChecksums;
    private final Map<String, Long> computedChecksums;

    private final File checksumFile;

    ChecksumComparator(String checksumPath) throws IOException {
      checksumFile = new File(checksumPath);
      // Read in the checksums
      if (checksumFile.exists()) {
        ObjectMapper mapper = new ObjectMapper();
        storedChecksums = mapper
            .readValue(checksumFile, new TypeReference<Map<String, Long>>() {
            });
      } else {
        storedChecksums = new HashMap<>(0);
      }
      computedChecksums = new HashMap<>();
    }

    public boolean hasChanged(File file) throws IOException {
      if (!file.exists()) {
        throw new FileNotFoundException(
            "Specified protoc include or source does not exist: " + file);
      }
      if (file.isDirectory()) {
        return hasDirectoryChanged(file);
      } else if (file.isFile()) {
        return hasFileChanged(file);
      } else {
        throw new IOException("Not a file or directory: " + file);
      }
    }

    private boolean hasDirectoryChanged(File directory) throws IOException {
      File[] listing = directory.listFiles();
      boolean changed = false;
      if (listing == null) {
        // not changed.
        return false;
      }
      // Do not exit early, since we need to compute and save checksums
      // for each file within the directory.
      for (File f : listing) {
        if (f.isDirectory()) {
          if (hasDirectoryChanged(f)) {
            changed = true;
          }
        } else if (f.isFile()) {
          if (hasFileChanged(f)) {
            changed = true;
          }
        } else {
          getLog().debug("Skipping entry that is not a file or directory: "
              + f);
        }
      }
      return changed;
    }

    private boolean hasFileChanged(File file) throws IOException {
      long computedCsum = computeChecksum(file);

      // Return if the generated csum matches the stored csum
      Long storedCsum = storedChecksums.get(file.getCanonicalPath());
      if (storedCsum == null || storedCsum.longValue() != computedCsum) {
        // It has changed.
        return true;
      }
      return false;
    }

    private long computeChecksum(File file) throws IOException {
      // If we've already computed the csum, reuse the computed value
      final String canonicalPath = file.getCanonicalPath();
      if (computedChecksums.containsKey(canonicalPath)) {
        return computedChecksums.get(canonicalPath);
      }
      // Compute the csum for the file
      CRC32 crc = new CRC32();
      byte[] buffer = new byte[1024*64];
      try (BufferedInputStream in =
          new BufferedInputStream(new FileInputStream(file))) {
        while (true) {
          int read = in.read(buffer);
          if (read <= 0) {
            break;
          }
          crc.update(buffer, 0, read);
        }
      }
      // Save it in the generated map and return
      final long computedCsum = crc.getValue();
      computedChecksums.put(canonicalPath, computedCsum);
      return crc.getValue();
    }

    public void writeChecksums() throws IOException {
      ObjectMapper mapper = new ObjectMapper();
      try (BufferedOutputStream out = new BufferedOutputStream(
          new FileOutputStream(checksumFile))) {
        mapper.writeValue(out, computedChecksums);
        getLog().info("Wrote protoc checksums to file " + checksumFile);
      }
    }
  }

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
          throw new MojoExecutionException(
              "Could not create directory: " + output);
        }
      }

      // Whether the import or source protoc files have changed.
      ChecksumComparator comparator = new ChecksumComparator(checksumPath);
      boolean importsChanged = false;

      command = new ArrayList<String>();
      command.add(protocCommand);
      command.add("--java_out=" + output.getCanonicalPath());
      if (imports != null) {
        for (File i : imports) {
          if (comparator.hasChanged(i)) {
            importsChanged = true;
          }
          command.add("-I" + i.getCanonicalPath());
        }
      }
      // Filter to generate classes for just the changed source files.
      List<File> changedSources = new ArrayList<>();
      boolean sourcesChanged = false;
      for (File f : FileSetUtils.convertFileSetToFiles(source)) {
        // Need to recompile if the source has changed, or if any import has
        // changed.
        if (comparator.hasChanged(f) || importsChanged) {
          sourcesChanged = true;
          changedSources.add(f);
          command.add(f.getCanonicalPath());
        }
      }

      if (!sourcesChanged && !importsChanged) {
        getLog().info("No changes detected in protoc files, skipping "
            + "generation.");
      } else {
        if (getLog().isDebugEnabled()) {
          StringBuilder b = new StringBuilder();
          b.append("Generating classes for the following protoc files: [");
          String prefix = "";
          for (File f : changedSources) {
            b.append(prefix);
            b.append(f.toString());
            prefix = ", ";
          }
          b.append("]");
          getLog().debug(b.toString());
        }

        exec = new Exec(this);
        out = new ArrayList<String>();
        List<String> err = new ArrayList<>();
        if (exec.run(command, out, err) != 0) {
          getLog().error("protoc compiler error");
          for (String s : out) {
            getLog().error(s);
          }
          for (String s : err) {
            getLog().error(s);
          }
          throw new MojoExecutionException("protoc failure");
        }
        // Write the new checksum file on success.
        comparator.writeChecksums();
      }
    } catch (Throwable ex) {
      throw new MojoExecutionException(ex.toString(), ex);
    }
    project.addCompileSourceRoot(output.getAbsolutePath());
  }

}
