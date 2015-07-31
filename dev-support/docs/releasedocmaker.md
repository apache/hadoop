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

releasedocmaker
===============

* [Purpose](#Purpose)
* [Basic Usage](#Basic_Usage)
* [Changing the Header](#Changing_the_Header)
* [Multiple Versions](#Multiple_Versions)
* [Unreleased Dates](#Unreleased_Dates)
* [Lint Mode](#Lint_Mode)

# Purpose

Building changelog information in a form that is human digestible but still containing as much useful information is difficult.  Many attempts over the years have resulted in a variety of methods that projects use to solve this problem:

* JIRA-generated release notes from the "Release Notes" button
* Manually modified CHANGES file
* Processing git log information

All of these methods have their pros and cons.  Some have issues with accuracy.  Some have issues with lack of details. None of these methods seem to cover all of the needs of many projects and are full of potential pitfalls.

In order to solve these problems, releasedocmaker was written to automatically generate a changelog and release notes by querying Apache's JIRA instance.

# Basic Usage

Minimally, the name of the JIRA project and a version registered in JIRA must be provided:

```bash
$ releasedocmaker.py --project (project) --version (version)
```

This will query Apache JIRA, generating two files in a directory named after the given version in an extended markdown format which can be processed by both mvn site and GitHub.

* CHANGES.(version).md

This is similar to the JIRA "Release Notes" button but is in tabular format and includes the priority, component, reporter, and contributor fields.  It also highlights Incompatible Changes so that readers know what to look out for when upgrading. The top of the file also includes the date that the version was marked as released in JIRA.


* RELEASENOTES.(version).md

If your JIRA project supports the release note field, this will contain any JIRA mentioned in the CHANGES log that is either an incompatible change or has a release note associated with it.  If your JIRA project does not support the release notes field, this will be the description field.

For example, to build the release documentation for HBase v1.2.0...

```bash
$ releasedocmaker.py --project HBASE --version 1.2.0
```

... will create a 1.2.0 directory and inside that directory will be CHANGES.1.2.0.md and RELEASENOTES.1.2.0.md .


# Changing the Header

By default, it will use a header that matches the project name.  But that is kind of ugly and the case may be wrong.  Luckily, the title can be changed:

```bash
$ releasedocmaker.py --project HBASE --version 1.2.0 --projecttitle "Apache HBase"
```

Now instead of "HBASE", it will use "Apache HBASE" for some titles and headers.

# Multiple Versions

The script can also generate multiple versions at once, by

```bash
$ releasedocmaker.py --project HBASE --version 1.0.0 --version 1.2.0
```

This will create the files for versions 1.0.0 and versions 1.2.0 in their own directories.

But what if the version numbers are not known?  releasedocmaker can also generate version data based upon ranges:

```bash
$ releasedocmaker.py --project HBASE --version 1.0.0 --version 1.2.0 --range
```

In this form, releasedocmaker will query JIRA, discover all versions that alphabetically appear to be between 1.0.0 and 1.2.0, inclusive, and generate all of the relative release documents.  This is especially useful when bootstrapping an existing project.

# Unreleased Dates

For released versions, releasedocmaker will pull the date of the release from JIRA.  However, for unreleased versions it marks the release as "Unreleased". This can be inconvenient when actually building a release and wanting to include it inside the source package.

The --usetoday option can be used to signify that instead of using Unreleased, releasedocmaker should use today's date.

```bash
$ releasedocmaker.py --project HBASE --version 1.0.0 --usetoday
```

After using this option and release, don't forget to change JIRA's release date to match!

# Lint Mode

In order to ensure proper formatting while using mvn site, releasedocmaker puts in periods (.) for fields that are empty or unassigned.  This can be unsightly and not proper for any given project.  There are also other things, such as missing release notes for incompatible changes, that are less than desirable.

In order to help release managers from having to scan through potentially large documents, releasedocmaker features a lint mode, triggered via --lint:

```bash
$ releasedocmaker.py --project HBASE --version 1.0.0 --lint
```

This will do the normal JIRA querying, looking for items it considers problematic.  It will print the information to the screen and then exit with either success or failure, depending upon if any issues were discovered.
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

releasedocmaker
===============

* [Purpose](#Purpose)
* [Basic Usage](#Basic_Usage)
* [Changing the Header](#Changing_the_Header)
* [Multiple Versions](#Multiple_Versions)
* [Unreleased Dates](#Unreleased_Dates)
* [Lint Mode](#Lint_Mode)

# Purpose

Building changelog information in a form that is human digestible but still containing as much useful information is difficult.  Many attempts over the years have resulted in a variety of methods that projects use to solve this problem:

* JIRA-generated release notes from the "Release Notes" button
* Manually modified CHANGES file
* Processing git log information

All of these methods have their pros and cons.  Some have issues with accuracy.  Some have issues with lack of details. None of these methods seem to cover all of the needs of many projects and are full of potential pitfalls.

In order to solve these problems, releasedocmaker was written to automatically generate a changelog and release notes by querying Apache's JIRA instance.

# Basic Usage

Minimally, the name of the JIRA project and a version registered in JIRA must be provided:

```bash
$ releasedocmaker.py --project (project) --version (version)
```

This will query Apache JIRA, generating two files in a directory named after the given version in an extended markdown format which can be processed by both mvn site and GitHub.

* CHANGES.(version).md

This is similar to the JIRA "Release Notes" button but is in tabular format and includes the priority, component, reporter, and contributor fields.  It also highlights Incompatible Changes so that readers know what to look out for when upgrading. The top of the file also includes the date that the version was marked as released in JIRA.


* RELEASENOTES.(version).md

If your JIRA project supports the release note field, this will contain any JIRA mentioned in the CHANGES log that is either an incompatible change or has a release note associated with it.  If your JIRA project does not support the release notes field, this will be the description field.

For example, to build the release documentation for HBase v1.2.0...

```bash
$ releasedocmaker.py --project HBASE --version 1.2.0
```

... will create a 1.2.0 directory and inside that directory will be CHANGES.1.2.0.md and RELEASENOTES.1.2.0.md .


# Changing the Header

By default, it will use a header that matches the project name.  But that is kind of ugly and the case may be wrong.  Luckily, the title can be changed:

```bash
$ releasedocmaker.py --project HBASE --version 1.2.0 --projecttitle "Apache HBase"
```

Now instead of "HBASE", it will use "Apache HBASE" for some titles and headers.

# Multiple Versions

The script can also generate multiple versions at once, by

```bash
$ releasedocmaker.py --project HBASE --version 1.0.0 --version 1.2.0
```

This will create the files for versions 1.0.0 and versions 1.2.0 in their own directories.

But what if the version numbers are not known?  releasedocmaker can also generate version data based upon ranges:

```bash
$ releasedocmaker.py --project HBASE --version 1.0.0 --version 1.2.0 --range
```

In this form, releasedocmaker will query JIRA, discover all versions that alphabetically appear to be between 1.0.0 and 1.2.0, inclusive, and generate all of the relative release documents.  This is especially useful when bootstrapping an existing project.

# Unreleased Dates

For released versions, releasedocmaker will pull the date of the release from JIRA.  However, for unreleased versions it marks the release as "Unreleased". This can be inconvenient when actually building a release and wanting to include it inside the source package.

The --usetoday option can be used to signify that instead of using Unreleased, releasedocmaker should use today's date.

```bash
$ releasedocmaker.py --project HBASE --version 1.0.0 --usetoday
```

After using this option and release, don't forget to change JIRA's release date to match!

# Lint Mode

In order to ensure proper formatting while using mvn site, releasedocmaker puts in periods (.) for fields that are empty or unassigned.  This can be unsightly and not proper for any given project.  There are also other things, such as missing release notes for incompatible changes, that are less than desirable.

In order to help release managers from having to scan through potentially large documents, releasedocmaker features a lint mode, triggered via --lint:

```bash
$ releasedocmaker.py --project HBASE --version 1.0.0 --lint
```

This will do the normal JIRA querying, looking for items it considers problematic.  It will print the information to the screen and then exit with either success or failure, depending upon if any issues were discovered.
