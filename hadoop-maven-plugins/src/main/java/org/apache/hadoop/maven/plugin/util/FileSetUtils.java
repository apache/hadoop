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
package org.apache.hadoop.maven.plugin.util;

import org.apache.maven.model.FileSet;
import org.codehaus.plexus.util.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * FileSetUtils contains helper methods for mojo implementations that need to
 * work with a Maven FileSet.
 */
public class FileSetUtils {

  /**
   * Returns a string containing every element of the given list, with each
   * element separated by a comma.
   * 
   * @param list List of all elements
   * @return String containing every element, comma-separated
   */
  private static String getCommaSeparatedList(List list) {
    StringBuilder buffer = new StringBuilder();
    String separator = "";
    for (Object e : list) {
      buffer.append(separator).append(e);
      separator = ",";
    }
    return buffer.toString();
  }

  /**
   * Converts a Maven FileSet to a list of File objects.
   * 
   * @param source FileSet to convert
   * @return List<File> containing every element of the FileSet as a File
   * @throws IOException if an I/O error occurs while trying to find the files
   */
  @SuppressWarnings("unchecked")
  public static List<File> convertFileSetToFiles(FileSet source) throws IOException {
    String includes = getCommaSeparatedList(source.getIncludes());
    String excludes = getCommaSeparatedList(source.getExcludes());
    return FileUtils.getFiles(new File(source.getDirectory()), includes, excludes);
  }
}
