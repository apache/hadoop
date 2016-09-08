<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

Offline Image Viewer Guide
==========================

* [Offline Image Viewer Guide](#Offline_Image_Viewer_Guide)
    * [Overview](#Overview)
    * [Usage](#Usage)
        * [Web Processor](#Web_Processor)
        * [XML Processor](#XML_Processor)
    * [Options](#Options)
    * [Analyzing Results](#Analyzing_Results)
    * [oiv\_legacy Command](#oiv_legacy_Command)
        * [Usage](#Usage)
        * [Options](#Options)

Overview
--------

The Offline Image Viewer is a tool to dump the contents of hdfs fsimage files to a human-readable format and provide read-only WebHDFS API in order to allow offline analysis and examination of an Hadoop cluster's namespace. The tool is able to process very large image files relatively quickly. The tool handles the layout formats that were included with Hadoop versions 2.4 and up. If you want to handle older layout formats, you can use the Offline Image Viewer of Hadoop 2.3 or [oiv\_legacy Command](#oiv_legacy_Command). If the tool is not able to process an image file, it will exit cleanly. The Offline Image Viewer does not require a Hadoop cluster to be running; it is entirely offline in its operation.

The Offline Image Viewer provides several output processors:

1.  Web is the default output processor. It launches a HTTP server
    that exposes read-only WebHDFS API. Users can investigate the namespace
    interactively by using HTTP REST API.

2.  XML creates an XML document of the fsimage and includes all of the
    information within the fsimage. The
    output of this processor is amenable to automated processing and
    analysis with XML tools. Due to the verbosity of the XML syntax,
    this processor will also generate the largest amount of output.

3.  FileDistribution is the tool for analyzing file sizes in the
    namespace image. In order to run the tool one should define a range
    of integers [0, maxSize] by specifying maxSize and a step. The
    range of integers is divided into segments of size step: [0, s[1],
    ..., s[n-1], maxSize], and the processor calculates how many files
    in the system fall into each segment [s[i-1], s[i]). Note that
    files larger than maxSize always fall into the very last segment.
    By default, the output file is formatted as a tab separated two column
    table: Size and NumFiles. Where Size represents the start of the segment,
    and numFiles is the number of files form the image which size falls
    in this segment. By specifying the option -format, the output file will be
    formatted in a human-readable fashion rather than a number of bytes that
    showed in Size column. In addition, the Size column will be changed to the
    Size Range column.

4. Delimited (experimental): Generate a text file with all of the elements
   common to both inodes and inodes-under-construction, separated by a
   delimiter. The default delimiter is \t, though this may be changed via
   the -delimiter argument.

5. ReverseXML (experimental): This is the opposite of the XML processor;
   it reconstructs an fsimage from an XML file. This processor makes it easy to
   create fsimages for testing, and manually edit fsimages when there is
   corruption.

Usage
-----

### Web Processor

Web processor launches a HTTP server which exposes read-only WebHDFS API. Users can specify the address to listen by -addr option (default by localhost:5978).

       bash$ bin/hdfs oiv -i fsimage
       14/04/07 13:25:14 INFO offlineImageViewer.WebImageViewer: WebImageViewer
       started. Listening on /127.0.0.1:5978. Press Ctrl+C to stop the viewer.

Users can access the viewer and get the information of the fsimage by the following shell command:

       bash$ bin/hdfs dfs -ls webhdfs://127.0.0.1:5978/
       Found 2 items
       drwxrwx--* - root supergroup          0 2014-03-26 20:16 webhdfs://127.0.0.1:5978/tmp
       drwxr-xr-x   - root supergroup          0 2014-03-31 14:08 webhdfs://127.0.0.1:5978/user

To get the information of all the files and directories, you can simply use the following command:

       bash$ bin/hdfs dfs -ls -R webhdfs://127.0.0.1:5978/

Users can also get JSON formatted FileStatuses via HTTP REST API.

       bash$ curl -i http://127.0.0.1:5978/webhdfs/v1/?op=liststatus
       HTTP/1.1 200 OK
       Content-Type: application/json
       Content-Length: 252

       {"FileStatuses":{"FileStatus":[
       {"fileId":16386,"accessTime":0,"replication":0,"owner":"theuser","length":0,"permission":"755","blockSize":0,"modificationTime":1392772497282,"type":"DIRECTORY","group":"supergroup","childrenNum":1,"pathSuffix":"user"}
       ]}}

The Web processor now supports the following operations:

* [LISTSTATUS](./WebHDFS.html#List_a_Directory)
* [GETFILESTATUS](./WebHDFS.html#Status_of_a_FileDirectory)
* [GETACLSTATUS](./WebHDFS.html#Get_ACL_Status)
* [GETXATTRS](./WebHDFS.html#Get_an_XAttr)
* [LISTXATTRS](./WebHDFS.html#List_all_XAttrs)
* [CONTENTSUMMARY] (./WebHDFS.html#Get_Content_Summary_of_a_Directory)

### XML Processor

XML Processor is used to dump all the contents in the fsimage. Users can specify input and output file via -i and -o command-line.

       bash$ bin/hdfs oiv -p XML -i fsimage -o fsimage.xml

This will create a file named fsimage.xml contains all the information in the fsimage. For very large image files, this process may take several minutes.

Applying the Offline Image Viewer with XML processor would result in the following output:

       <?xml version="1.0"?>
       <fsimage>
       <NameSection>
         <genstampV1>1000</genstampV1>
         <genstampV2>1002</genstampV2>
         <genstampV1Limit>0</genstampV1Limit>
         <lastAllocatedBlockId>1073741826</lastAllocatedBlockId>
         <txid>37</txid>
       </NameSection>
       <INodeSection>
         <lastInodeId>16400</lastInodeId>
         <inode>
           <id>16385</id>
           <type>DIRECTORY</type>
           <name></name>
           <mtime>1392772497282</mtime>
           <permission>theuser:supergroup:rwxr-xr-x</permission>
           <nsquota>9223372036854775807</nsquota>
           <dsquota>-1</dsquota>
         </inode>
       ...remaining output omitted...

Options
-------

| **Flag** | **Description** |
|:---- |:---- |
| `-i`\|`--inputFile` *input file* | Specify the input fsimage file (or XML file, if ReverseXML processor is used) to process. Required. |
| `-o`\|`--outputFile` *output file* | Specify the output filename, if the specified output processor generates one. If the specified file already exists, it is silently overwritten. (output to stdout by default) If the input file is an XML file, it also creates an &lt;outputFile&gt;.md5. |
| `-p`\|`--processor` *processor* | Specify the image processor to apply against the image file. Currently valid options are `Web` (default), `XML`, `Delimited`, `FileDistribution` and `ReverseXML`. |
| `-addr` *address* | Specify the address(host:port) to listen. (localhost:5978 by default). This option is used with Web processor. |
| `-maxSize` *size* | Specify the range [0, maxSize] of file sizes to be analyzed in bytes (128GB by default). This option is used with FileDistribution processor. |
| `-step` *size* | Specify the granularity of the distribution in bytes (2MB by default). This option is used with FileDistribution processor. |
| `-format` | Format the output result in a human-readable fashion rather than a number of bytes. (false by default). This option is used with FileDistribution processor. |
| `-delimiter` *arg* | Delimiting string to use with Delimited processor. |
| `-t`\|`--temp` *temporary dir* | Use temporary dir to cache intermediate result to generate Delimited outputs. If not set, Delimited processor constructs the namespace in memory before outputting text. |
| `-h`\|`--help` | Display the tool usage and help information and exit. |

Analyzing Results
-----------------

The Offline Image Viewer makes it easy to gather large amounts of data about the hdfs namespace. This information can then be used to explore file system usage patterns or find specific files that match arbitrary criteria, along with other types of namespace analysis.

oiv\_legacy Command
-------------------

Due to the internal layout changes introduced by the ProtocolBuffer-based fsimage ([HDFS-5698](https://issues.apache.org/jira/browse/HDFS-5698)), OfflineImageViewer consumes excessive amount of memory and loses some functions such as Indented processor. If you want to process without large amount of memory or use these processors, you can use `oiv_legacy` command (same as `oiv` in Hadoop 2.3).

### Usage

1. Set `dfs.namenode.legacy-oiv-image.dir` to an appropriate directory
   to make standby NameNode or SecondaryNameNode save its namespace in the
   old fsimage format during checkpointing.

2. Use `oiv_legacy` command to the old format fsimage.

        bash$ bin/hdfs oiv_legacy -i fsimage_old -o output

### Options

| **Flag** | **Description** |
|:---- |:---- |
| `-i`\|`--inputFile` *input file* | Specify the input fsimage file to process. Required. |
| `-o`\|`--outputFile` *output file* | Specify the output filename, if the specified output processor generates one. If the specified file already exists, it is silently overwritten. Required. |
| `-p`\|`--processor` *processor* | Specify the image processor to apply against the image file. Valid options are Ls (default), XML, Delimited, Indented, and FileDistribution. |
| `-maxSize` *size* | Specify the range [0, maxSize] of file sizes to be analyzed in bytes (128GB by default). This option is used with FileDistribution processor. |
| `-step` *size* | Specify the granularity of the distribution in bytes (2MB by default). This option is used with FileDistribution processor. |
| `-format` | Format the output result in a human-readable fashion rather than a number of bytes. (false by default). This option is used with FileDistribution processor. |
| `-skipBlocks` | Do not enumerate individual blocks within files. This may save processing time and outfile file space on namespaces with very large files. The Ls processor reads the blocks to correctly determine file sizes and ignores this option. |
| `-printToScreen` | Pipe output of processor to console as well as specified file. On extremely large namespaces, this may increase processing time by an order of magnitude. |
| `-delimiter` *arg* | When used in conjunction with the Delimited processor, replaces the default tab delimiter with the string specified by *arg*. |
| `-h`\|`--help` | Display the tool usage and help information and exit. |
