/*
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
 *
 */

package org.apache.hadoop.fs.adl.live;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.adl.common.Parallelized;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.util.*;

/**
 * Test supported ASCII, UTF-8 character set supported by Adl storage file
 * system on file/folder operation.
 */
@RunWith(Parallelized.class)
public class TestAdlSupportedCharsetInPath {

  private static final String TEST_ROOT = "/test/";
  private static final Logger LOG = LoggerFactory
      .getLogger(TestAdlSupportedCharsetInPath.class);
  private String path;

  public TestAdlSupportedCharsetInPath(String filePath) {
    path = filePath;
  }

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> adlCharTestData()
      throws UnsupportedEncodingException {

    ArrayList<String> filePathList = new ArrayList<>();
    for (int i = 32; i < 127; ++i) {
      String specialChar = (char) i + "";
      if (i >= 48 && i <= 57) {
        continue;
      }

      if (i >= 65 && i <= 90) {
        continue;
      }

      if (i >= 97 && i <= 122) {
        continue;
      }

      // Special char at start of the path
      if (i != 92 && i != 58 && i != 46 && i != 47) {
        filePathList.add(specialChar + "");
      }

      // Special char at end of string
      if (i != 92 && i != 47 && i != 58) {
        filePathList.add("file " + i + " " + specialChar);
      }

      // Special char in between string
      if (i != 47 && i != 58 && i != 92) {
        filePathList.add("file " + i + " " + specialChar + "_name");
      }
    }

    filePathList.add("a  ");
    filePathList.add("a..b");
    fillUnicodes(filePathList);
    Collection<Object[]> result = new ArrayList<>();
    for (String item : filePathList) {
      result.add(new Object[] {item});
    }
    return result;
  }

  private static void fillUnicodes(ArrayList<String> filePathList) {
    // Unicode characters
    filePathList.add("البيانات الكبيرة"); // Arabic
    filePathList.add("Të dhënat i madh"); // Albanian
    filePathList.add("մեծ տվյալները"); // Armenian
    filePathList.add("böyük data"); // Azerbaijani
    filePathList.add("вялікія дадзеныя"); // Belarusian,
    filePathList.add("বিগ ডেটা"); // Bengali
    filePathList.add("veliki podataka"); // Bosnian
    filePathList.add("голяма данни"); // Bulgarian
    filePathList.add("大数据"); // Chinese - Simplified
    filePathList.add("大數據"); // Chinese - Traditional
    filePathList.add("დიდი მონაცემთა"); // Georgian,
    filePathList.add("große Daten"); // German
    filePathList.add("μεγάλο δεδομένα"); // Greek
    filePathList.add("મોટા માહિતી"); // Gujarati
    filePathList.add("נתונים גדולים"); // Hebrew
    filePathList.add("बड़ा डेटा"); // Hindi
    filePathList.add("stór gögn"); // Icelandic
    filePathList.add("sonraí mór"); // Irish
    filePathList.add("ビッグデータ"); // Japanese
    filePathList.add("үлкен деректер"); // Kazakh
    filePathList.add("ទិន្នន័យធំ"); // Khmer
    filePathList.add("빅 데이터"); // Korean
    filePathList.add("ຂໍ້ມູນ ຂະຫນາດໃຫຍ່"); // Lao
    filePathList.add("големи податоци"); // Macedonian
    filePathList.add("ठूलो डाटा"); // Nepali
    filePathList.add("വലിയ ഡാറ്റ"); // Malayalam
    filePathList.add("मोठे डेटा"); // Marathi
    filePathList.add("том мэдээлэл"); // Mangolian
    filePathList.add("اطلاعات بزرگ"); // Persian
    filePathList.add("ਵੱਡੇ ਡਾਟੇ ਨੂੰ"); // Punjabi
    filePathList.add("большие данные"); // Russian
    filePathList.add("Велики података"); // Serbian
    filePathList.add("විශාල දත්ත"); // Sinhala
    filePathList.add("big dát"); // Slovak
    filePathList.add("маълумоти калон"); // Tajik
    filePathList.add("பெரிய தரவு"); // Tamil
    filePathList.add("పెద్ద డేటా"); // Telugu
    filePathList.add("ข้อมูลใหญ่"); // Thai
    filePathList.add("büyük veri"); // Turkish
    filePathList.add("великі дані"); // Ukranian
    filePathList.add("بڑے اعداد و شمار"); // Urdu
    filePathList.add("katta ma'lumotlar"); // Uzbek
    filePathList.add("dữ liệu lớn"); // Vietanamese
    filePathList.add("גרויס דאַטן"); // Yiddish
    filePathList.add("big idatha"); // Zulu
    filePathList.add("rachelχ");
    filePathList.add("jessicaο");
    filePathList.add("sarahδ");
    filePathList.add("katieν");
    filePathList.add("wendyξ");
    filePathList.add("davidμ");
    filePathList.add("priscillaυ");
    filePathList.add("oscarθ");
    filePathList.add("xavierχ");
    filePathList.add("gabriellaθ");
    filePathList.add("davidυ");
    filePathList.add("ireneμ");
    filePathList.add("fredρ");
    filePathList.add("davidτ");
    filePathList.add("ulyssesν");
    filePathList.add("gabriellaμ");
    filePathList.add("zachζ");
    filePathList.add("gabriellaλ");
    filePathList.add("ulyssesφ");
    filePathList.add("davidχ");
    filePathList.add("sarahσ");
    filePathList.add("hollyψ");
    filePathList.add("nickα");
    filePathList.add("ulyssesι");
    filePathList.add("mikeβ");
    filePathList.add("priscillaκ");
    filePathList.add("wendyθ");
    filePathList.add("jessicaς");
    filePathList.add("fredχ");
    filePathList.add("fredζ");
    filePathList.add("sarahκ");
    filePathList.add("calvinη");
    filePathList.add("xavierχ");
    filePathList.add("yuriχ");
    filePathList.add("ethanλ");
    filePathList.add("hollyε");
    filePathList.add("xavierσ");
    filePathList.add("victorτ");
    filePathList.add("wendyβ");
    filePathList.add("jessicaς");
    filePathList.add("quinnφ");
    filePathList.add("xavierυ");
    filePathList.add("nickι");
    filePathList.add("rachelφ");
    filePathList.add("oscarξ");
    filePathList.add("zachδ");
    filePathList.add("zachλ");
    filePathList.add("rachelα");
    filePathList.add("jessicaφ");
    filePathList.add("lukeφ");
    filePathList.add("tomζ");
    filePathList.add("nickξ");
    filePathList.add("nickκ");
    filePathList.add("ethanδ");
    filePathList.add("fredχ");
    filePathList.add("priscillaθ");
    filePathList.add("zachξ");
    filePathList.add("xavierξ");
    filePathList.add("zachψ");
    filePathList.add("ethanα");
    filePathList.add("oscarι");
    filePathList.add("ireneδ");
    filePathList.add("ireneζ");
    filePathList.add("victorο");
    filePathList.add("wendyβ");
    filePathList.add("mikeσ");
    filePathList.add("fredο");
    filePathList.add("mikeη");
    filePathList.add("sarahρ");
    filePathList.add("quinnβ");
    filePathList.add("mikeυ");
    filePathList.add("nickζ");
    filePathList.add("nickο");
    filePathList.add("tomκ");
    filePathList.add("bobλ");
    filePathList.add("yuriπ");
    filePathList.add("davidτ");
    filePathList.add("quinnπ");
    filePathList.add("mikeλ");
    filePathList.add("davidη");
    filePathList.add("ethanτ");
    filePathList.add("nickφ");
    filePathList.add("yuriο");
    filePathList.add("ethanυ");
    filePathList.add("bobθ");
    filePathList.add("davidλ");
    filePathList.add("priscillaξ");
    filePathList.add("nickγ");
    filePathList.add("lukeυ");
    filePathList.add("ireneλ");
    filePathList.add("xavierο");
    filePathList.add("fredυ");
    filePathList.add("ulyssesμ");
    filePathList.add("wendyγ");
    filePathList.add("zachλ");
    filePathList.add("rachelς");
    filePathList.add("sarahπ");
    filePathList.add("aliceψ");
    filePathList.add("bobτ");
  }

  @AfterClass
  public static void testReport() throws IOException, URISyntaxException {
    if (!AdlStorageConfiguration.isContractTestEnabled()) {
      return;
    }

    FileSystem fs = AdlStorageConfiguration.createStorageConnector();
    fs.delete(new Path(TEST_ROOT), true);
  }

  @Test
  public void testAllowedSpecialCharactersMkdir()
      throws IOException, URISyntaxException {
    Path parentPath = new Path(TEST_ROOT, UUID.randomUUID().toString() + "/");
    Path specialFile = new Path(parentPath, path);
    FileSystem fs = AdlStorageConfiguration.createStorageConnector();

    Assert.assertTrue("Mkdir failed : " + specialFile, fs.mkdirs(specialFile));
    Assert.assertTrue("File not Found after Mkdir success" + specialFile,
        fs.exists(specialFile));
    Assert.assertTrue("Not listed under parent " + parentPath,
        contains(fs.listStatus(parentPath),
            fs.makeQualified(specialFile).toString()));
    Assert.assertTrue("Delete failed : " + specialFile,
            fs.delete(specialFile, true));
    Assert.assertFalse("File still exist after delete " + specialFile,
        fs.exists(specialFile));
  }

  private boolean contains(FileStatus[] statuses, String remotePath) {
    for (FileStatus status : statuses) {
      if (status.getPath().toString().equals(remotePath)) {
        return true;
      }
    }

    Arrays.stream(statuses).forEach(s -> LOG.info(s.getPath().toString()));
    return false;
  }

  @Before
  public void setup() throws Exception {
    org.junit.Assume
        .assumeTrue(AdlStorageConfiguration.isContractTestEnabled());
  }

  @Test
  public void testAllowedSpecialCharactersRename()
      throws IOException, URISyntaxException {

    String parentPath = TEST_ROOT + UUID.randomUUID().toString() + "/";
    Path specialFile = new Path(parentPath + path);
    Path anotherLocation = new Path(parentPath + UUID.randomUUID().toString());
    FileSystem fs = AdlStorageConfiguration.createStorageConnector();

    Assert.assertTrue("Could not create " + specialFile.toString(),
        fs.createNewFile(specialFile));
    Assert.assertTrue(
        "Failed to rename " + specialFile.toString() + " --> " + anotherLocation
            .toString(), fs.rename(specialFile, anotherLocation));
    Assert.assertFalse("File should not be present after successful rename : "
        + specialFile.toString(), fs.exists(specialFile));
    Assert.assertTrue("File should be present after successful rename : "
        + anotherLocation.toString(), fs.exists(anotherLocation));
    Assert.assertFalse(
        "Listed under parent whereas expected not listed : " + parentPath,
        contains(fs.listStatus(new Path(parentPath)),
            fs.makeQualified(specialFile).toString()));

    Assert.assertTrue(
        "Failed to rename " + anotherLocation.toString() + " --> " + specialFile
            .toString(), fs.rename(anotherLocation, specialFile));
    Assert.assertTrue(
        "File should be present after successful rename : " + "" + specialFile
            .toString(), fs.exists(specialFile));
    Assert.assertFalse("File should not be present after successful rename : "
        + anotherLocation.toString(), fs.exists(anotherLocation));

    Assert.assertTrue("Not listed under parent " + parentPath,
        contains(fs.listStatus(new Path(parentPath)),
            fs.makeQualified(specialFile).toString()));

    Assert.assertTrue("Failed to delete " + parentPath,
        fs.delete(new Path(parentPath), true));
  }
}