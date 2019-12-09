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

Offline Edits Viewer Guide
==========================

<!-- MACRO{toc|fromDepth=0|toDepth=3} -->

Overview
--------

Offline Edits Viewer is a tool to parse the Edits log file. The current processors are mostly useful for conversion between different formats, including XML which is human readable and easier to edit than native binary format.

The tool can parse the edits formats -18 (roughly Hadoop 0.19) and later. The tool operates on files only, it does not need Hadoop cluster to be running.

Input formats supported:

1.  **binary**: native binary format that Hadoop uses internally
2.  **xml**: XML format, as produced by xml processor, used if filename
    has `.xml` (case insensitive) extension

Note: XML/Binary format input file is not allowed to be processed by the same type processor.

The Offline Edits Viewer provides several output processors (unless stated otherwise the output of the processor can be converted back to original edits file):

1.  **binary**: native binary format that Hadoop uses internally
2.  **xml**: XML format
3.  **stats**: prints out statistics, this cannot be converted back to
    Edits file

Usage
-----

### XML Processor

XML processor can create an XML file that contains the edits log information. Users can specify input and output file via -i and -o command-line.

       bash$ bin/hdfs oev -p xml -i edits -o edits.xml

XML processor is the default processor in Offline Edits Viewer, users can also use the following command:

       bash$ bin/hdfs oev -i edits -o edits.xml

This would result in the following output:

       <?xml version="1.0" encoding="UTF-8"?>
       <EDITS>
         <EDITS_VERSION>-64</EDITS_VERSION>
         <RECORD>
           <OPCODE>OP_START_LOG_SEGMENT</OPCODE>
           <DATA>
             <TXID>1</TXID>
           </DATA>
         </RECORD>
         <RECORD>
           <OPCODE>OP_UPDATE_MASTER_KEY</OPCODE>
           <DATA>
             <TXID>2</TXID>
             <DELEGATION_KEY>
               <KEY_ID>1</KEY_ID>
               <EXPIRY_DATE>1487921580728</EXPIRY_DATE>
               <KEY>2e127ca41c7de215</KEY>
             </DELEGATION_KEY>
           </DATA>
         </RECORD>
         <RECORD>
       ...remaining output omitted...

### Binary Processor

Binary processor is the opposite of the XML processor. Users can specify input XML file and output file via -i and -o command-line.

       bash$ bin/hdfs oev -p binary -i edits.xml -o edits

This will reconstruct an edits log file from an XML file.

### Stats Processor

Stats processor is used to aggregate counts of op codes contained in the edits log file. Users can specify this processor by -p option.

       bash$ bin/hdfs oev -p stats -i edits -o edits.stats

The output result of this processor should be like the following output:

       VERSION                             : -64
       OP_ADD                         (  0): 8
       OP_RENAME_OLD                  (  1): 1
       OP_DELETE                      (  2): 1
       OP_MKDIR                       (  3): 1
       OP_SET_REPLICATION             (  4): 1
       OP_DATANODE_ADD                (  5): 0
       OP_DATANODE_REMOVE             (  6): 0
       OP_SET_PERMISSIONS             (  7): 1
       OP_SET_OWNER                   (  8): 1
       OP_CLOSE                       (  9): 9
       OP_SET_GENSTAMP_V1             ( 10): 0
       ...some output omitted...
       OP_APPEND                      ( 47): 1
       OP_SET_QUOTA_BY_STORAGETYPE    ( 48): 1
       OP_ADD_ERASURE_CODING_POLICY   ( 49): 0
       OP_ENABLE_ERASURE_CODING_POLICY  ( 50): 1
       OP_DISABLE_ERASURE_CODING_POLICY ( 51): 0
       OP_REMOVE_ERASURE_CODING_POLICY  ( 52): 0
       OP_INVALID                     ( -1): 0

The output is formatted as a colon separated two column table: OpCode and OpCodeCount. Each OpCode corresponding to the specific operation(s) in NameNode.

Options
-------

| Flag | Description |
|:---- |:---- |
| [`-i` ; `--inputFile`] *input file* | Specify the input edits log file to process. Xml (case insensitive) extension means XML format otherwise binary format is assumed. Required. |
| [`-o` ; `--outputFile`] *output file* | Specify the output filename, if the specified output processor generates one. If the specified file already exists, it is silently overwritten. Required. |
| [`-p` ; `--processor`] *processor* | Specify the image processor to apply against the image file. Currently valid options are `binary`, `xml` (default) and `stats`. |
| [`-v` ; `--verbose`] | Print the input and output filenames and pipe output of processor to console as well as specified file. On extremely large files, this may increase processing time by an order of magnitude. |
| [`-f` ; `--fix-txids`] | Renumber the transaction IDs in the input, so that there are no gaps or invalid transaction IDs. |
| [`-r` ; `--recover`] | When reading binary edit logs, use recovery mode. This will give you the chance to skip corrupt parts of the edit log. |
| [`-h` ; `--help`] | Display the tool usage and help information and exit. |

Case study: Hadoop cluster recovery
-----------------------------------

In case there is some problem with hadoop cluster and the edits file is corrupted it is possible to save at least part of the edits file that is correct. This can be done by converting the binary edits to XML, edit it manually and then convert it back to binary. The most common problem is that the edits file is missing the closing record (record that has opCode -1). This should be recognized by the tool and the XML format should be properly closed.

If there is no closing record in the XML file you can add one after last correct record. Anything after the record with opCode -1 is ignored.

Example of a closing record (with opCode -1):

      <RECORD>
        <OPCODE>-1</OPCODE>
        <DATA>
        </DATA>
      </RECORD>
