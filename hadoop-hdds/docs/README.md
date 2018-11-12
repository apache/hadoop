<!---
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->
# Hadoop Ozone/HDDS docs

This subproject contains the inline documentation for Ozone/HDDS components.

You can create a new page with:

```
hugo new content/title.md
```

You can check the rendering with:

```
hugo serve
```

This maven project will create the rendered HTML page during the build (ONLY if hugo is available).
And the dist project will include the documentation.

You can adjust the menu hierarchy with adjusting the header of the markdown file:

To show it in the main header add the menu entry:

```
---
menu: main
---
```

To show it as a subpage, you can set the parent. (The value could be the title of the parent page,
our you can defined an `id: ...` in the parent markdown and use that in the parent reference.

```
---
menu:
   main:
	   parent: "Getting started"
---
```
