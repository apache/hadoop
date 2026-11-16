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
package org.apache.hadoop.maven.plugin.versioninfo;

import java.io.Serializable;
import java.util.Locale;
import org.apache.hadoop.maven.plugin.util.Exec;
import org.apache.hadoop.maven.plugin.util.FileSetUtils;
import org.apache.maven.model.FileSet;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * VersionInfoMojo calculates information about the current version of the
 * codebase and exports the information as properties for further use in a Maven
 * build.  The version information includes build time, SCM URI, SCM branch, SCM
 * commit, and an MD5 checksum of the contents of the files in the codebase.
 * <p>
 * The build time and the SCM fields can each be pinned to a fixed value so that
 * two builds of the same source produce the same output; see
 * {@code version-info.build.time.value} and {@code version-info.scm.static}.
 */
@Mojo(name="version-info")
public class VersionInfoMojo extends AbstractMojo {

  static final String UNKNOWN = "Unknown";

  /**
   * Rendered into {@code common-version-info.properties} and read back by
   * {@code VersionInfo}; the minute precision is long-standing and retained.
   */
  private static final DateTimeFormatter BUILD_TIME_FORMAT =
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm'Z'")
          .withZone(ZoneOffset.UTC);

  @Parameter(defaultValue="${project}", readonly=true)
  private MavenProject project;

  @Parameter(required=true)
  private FileSet source;

  @Parameter(defaultValue="version-info.build.time")
  private String buildTimeProperty;

  @Parameter(defaultValue="version-info.source.md5")
  private String md5Property;

  @Parameter(defaultValue="version-info.scm.uri")
  private String scmUriProperty;

  @Parameter(defaultValue="version-info.scm.branch")
  private String scmBranchProperty;

  @Parameter(defaultValue="version-info.scm.commit")
  private String scmCommitProperty;

  @Parameter(defaultValue="git")
  private String gitCommand;

  /**
   * Build time to record, as an ISO-8601 instant or as seconds since the epoch.
   * Defaults to the Maven-wide reproducible-build timestamp; a value of one
   * character or less means unset, following the maven-archiver convention, and
   * the current time is used instead.
   */
  @Parameter(property="version-info.build.time.value",
      defaultValue="${project.build.outputTimestamp}")
  private String buildTimeValue;

  /**
   * When true, report the SCM values below instead of running git.
   */
  @Parameter(property="version-info.scm.static", defaultValue="false")
  private boolean staticScmInfo;

  @Parameter(property="version-info.scm.uri.value", defaultValue=VersionInfoMojo.UNKNOWN)
  private String scmUriValue;

  @Parameter(property="version-info.scm.branch.value", defaultValue=VersionInfoMojo.UNKNOWN)
  private String scmBranchValue;

  @Parameter(property="version-info.scm.commit.value", defaultValue=VersionInfoMojo.UNKNOWN)
  private String scmCommitValue;

  private enum SCM {NONE, GIT}

  @Override
  public void execute() throws MojoExecutionException {
    try {
      project.getProperties().setProperty(buildTimeProperty, getBuildTime());
      if (staticScmInfo) {
        getLog().info("SCM: static");
        project.getProperties().setProperty(scmUriProperty, scmUriValue);
        project.getProperties().setProperty(scmBranchProperty, scmBranchValue);
        project.getProperties().setProperty(scmCommitProperty, scmCommitValue);
      } else {
        SCM scm = determineSCM();
        project.getProperties().setProperty(scmUriProperty, getSCMUri(scm));
        project.getProperties().setProperty(scmBranchProperty, getSCMBranch(scm));
        project.getProperties().setProperty(scmCommitProperty, getSCMCommit(scm));
      }
      project.getProperties().setProperty(md5Property, computeMD5());
    } catch (Throwable ex) {
      throw new MojoExecutionException(ex.toString(), ex);
    }
  }

  /**
   * Returns the build time to record, formatted in UTC.
   *
   * @return String representing the build time
   * @throws MojoExecutionException if the configured value cannot be parsed
   */
  private String getBuildTime() throws MojoExecutionException {
    Instant instant = parseBuildTime(buildTimeValue);
    return BUILD_TIME_FORMAT.format(instant != null ? instant : Instant.now());
  }

  /**
   * Parses a build timestamp given either as seconds since the epoch or as an
   * ISO-8601 instant.
   *
   * @param value configured value; null, empty or a single character all mean
   *              "not set"
   * @return the instant, or null if the value was not set
   * @throws MojoExecutionException if the value is set but unparseable
   */
  static Instant parseBuildTime(String value) throws MojoExecutionException {
    if (value == null || value.length() <= 1) {
      return null;
    }
    try {
      return Instant.ofEpochSecond(Long.parseLong(value));
    } catch (NumberFormatException ignored) {
      // not epoch seconds, fall through to ISO-8601
    }
    try {
      return DateTimeFormatter.ISO_DATE_TIME.parse(value, Instant::from);
    } catch (DateTimeParseException e) {
      throw new MojoExecutionException("Cannot parse build time '" + value
          + "'; expected epoch seconds or an ISO-8601 instant", e);
    }
  }
  private List<String> scmOut;

  /**
   * Determines which SCM is in use (git or none) and captures
   * output of the SCM command for later parsing.
   * 
   * @return SCM in use for this build
   * @throws Exception if any error occurs attempting to determine SCM
   */
  private SCM determineSCM() throws Exception {
    Exec exec = new Exec(this);
    SCM scm = SCM.NONE;
    scmOut = new ArrayList<String>();
    int ret;
    ret = exec.run(Arrays.asList(gitCommand, "branch"), scmOut);
    if (ret == 0) {
      ret = exec.run(Arrays.asList(gitCommand, "remote", "-v"), scmOut);
      if (ret != 0) {
        scm = SCM.NONE;
        scmOut = null;
      } else {
        ret = exec.run(Arrays.asList(gitCommand, "log", "-n", "1"), scmOut);
        if (ret != 0) {
          scm = SCM.NONE;
          scmOut = null;
        } else {
          scm = SCM.GIT;
        }
      }
    }

    if (scmOut != null) {
      getLog().debug(scmOut.toString());
    }
    getLog().info("SCM: " + scm);
    return scm;
  }

  /**
   * Parses SCM output and returns URI of SCM.
   * 
   * @param scm SCM in use for this build
   * @return String URI of SCM
   */
  private String getSCMUri(SCM scm) {
    String uri = UNKNOWN;
    switch (scm) {
      case GIT:
        for (String s : scmOut) {
          if (s.startsWith("origin") && s.endsWith("(fetch)")) {
            uri = s.substring("origin".length());
            uri = uri.substring(0, uri.length() - "(fetch)".length());
            break;
          }
        }
        break;
    }
    return uri.trim();
  }

  /**
   * Parses SCM output and returns commit of SCM.
   * 
   * @param scm SCM in use for this build
   * @return String commit of SCM
   */
  private String getSCMCommit(SCM scm) {
    String commit = UNKNOWN;
    switch (scm) {
      case GIT:
        for (String s : scmOut) {
          if (s.startsWith("commit")) {
            commit = s.substring("commit".length());
            break;
          }
        }
        break;
    }
    return commit.trim();
  }

  /**
   * Parses SCM output and returns branch of SCM.
   * 
   * @param scm SCM in use for this build
   * @return String branch of SCM
   */
  private String getSCMBranch(SCM scm) {
    String branch = UNKNOWN;
    switch (scm) {
      case GIT:
        for (String s : scmOut) {
          if (s.startsWith("*")) {
            branch = s.substring("*".length());
            break;
          }
        }
        break;
    }
    return branch.trim();
  }

  /**
   * Reads and returns the full contents of the specified file.
   * 
   * @param file File to read
   * @return byte[] containing full contents of file
   * @throws IOException if there is an I/O error while reading the file
   */
  private byte[] readFile(File file) throws IOException {
    RandomAccessFile raf = new RandomAccessFile(file, "r");
    byte[] buffer = new byte[(int) raf.length()];
    raf.readFully(buffer);
    raf.close();
    return buffer;
  }

  /**
   * Given a list of files, computes and returns an MD5 checksum of the full
   * contents of all files.
   * 
   * @param files List<File> containing every file to input into the MD5 checksum
   * @return byte[] calculated MD5 checksum
   * @throws IOException if there is an I/O error while reading a file
   * @throws NoSuchAlgorithmException if the MD5 algorithm is not supported
   */
  private byte[] computeMD5(List<File> files) throws IOException, NoSuchAlgorithmException {
    MessageDigest md5 = MessageDigest.getInstance("MD5");
    for (File file : files) {
      getLog().debug("Computing MD5 for: " + file);
      md5.update(readFile(file));
    }
    return md5.digest();
  }

  /**
   * Converts bytes to a hexadecimal string representation and returns it.
   * 
   * @param array byte[] to convert
   * @return String containing hexadecimal representation of bytes
   */
  private String byteArrayToString(byte[] array) {
    StringBuilder sb = new StringBuilder();
    for (byte b : array) {
      sb.append(Integer.toHexString(0xff & b));
    }
    return sb.toString();
  }

  static class MD5Comparator implements Comparator<File>, Serializable {
    private static final long serialVersionUID = 1L;

    @Override
    public int compare(File lhs, File rhs) {
      return normalizePath(lhs).compareTo(normalizePath(rhs));
    }

    private String normalizePath(File file) {
      return file.getPath().toUpperCase(Locale.ENGLISH)
          .replaceAll("\\\\", "/");
    }
  }

  /**
   * Computes and returns an MD5 checksum of the contents of all files in the
   * input Maven FileSet.
   * 
   * @return String containing hexadecimal representation of MD5 checksum
   * @throws Exception if there is any error while computing the MD5 checksum
   */
  private String computeMD5() throws Exception {
    List<File> files = FileSetUtils.convertFileSetToFiles(source);
    // File order of MD5 calculation is significant.  Sorting is done on
    // unix-format names, case-folded, in order to get a platform-independent
    // sort and calculate the same MD5 on all platforms.
    Collections.sort(files, new MD5Comparator());
    byte[] md5 = computeMD5(files);
    String md5str = byteArrayToString(md5);
    getLog().info("Computed MD5: " + md5str);
    return md5str;
  }
}
