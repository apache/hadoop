# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Shared mapping from canonical (merged) license names to the project's
# LICENSE-<slug>.txt convention. Sourced by resolve-licenses.sh and
# normalize-licenses.sh. License names are lowercased before matching.

license_slug() {
  local name
  name="$(echo "$1" | tr '[:upper:]' '[:lower:]' | sed 's/[[:space:]]*$//')"
  case "$name" in
    "the apache software license, version 2.0") echo "apache-2.0" ;;
    "mit license")                              echo "mit" ;;
    # azure-keyvault-core's POM declares 'The MIT License (MIT)': this
    # extractor balances parens, so the resolver sees the FULL name
    # (the Python generator's parser sees the truncated
    # 'The MIT License (MIT' — mapped in generate-archive-manifest.py)
    "the mit license (mit)")                     echo "mit" ;;
    "bsd 2-clause license")                     echo "bsd-2-clause" ;;
    # generic "BSD License" carriers in this repo (stax2-api, dom4j,
    # jdom2, jline) are all 3-clause BSD
    "revised bsd"|"bsd-3-clause"|"bsd license"|"the bsd license") echo "bsd-3-clause" ;;
    "eclipse public license - version 1.0")     echo "epl-1.0" ;;
    "epl 2.0")                                  echo "epl-2.0" ;;
    # pre-merge spellings that licenseMerges also collapses; mapped here
    # so resolution also works on a raw report
    "eclipse public license 2.0"|"eclipse public license v2.0"|"eclipse public license - v 2.0"|"eclipse public license v. 2.0") echo "epl-2.0" ;;
    "gnu general public license (gpl), version 2, with the classpath exception") echo "gplv2-ce" ;;
    "gnu general public license, version 2 with the gnu classpath exception")     echo "gplv2-ce" ;;
    "the gnu general public license (gpl), version 2, with classpath exception") echo "gplv2-ce" ;;
    "edl 1.0")                                  echo "edl-1.0" ;;
    "cddl + gplv2 with classpath exception")    echo "cddl-gplv2-ce" ;;
    "cddl license")                             echo "cddl" ;;
    "mpl 1.1")                                  echo "mpl-1.1" ;;
    "lgpl 2.1")                                 echo "lgpl-2.1" ;;
    "the go license")                           echo "go" ;;
    "bouncy castle licence")                    echo "bouncycastle" ;;
    "w3c license")                              echo "w3c" ;;
    "public domain")                            echo "public-domain" ;;
    "public domain, per creative commons cc0") echo "public-domain" ;;
    "jquery license")                           echo "jquery" ;;
    "json")                                     echo "json" ;;
    # fallback: keep alphanumerics
    *) echo "$name" | tr -cd 'a-z0-9' | cut -c1-40 ;;
  esac
}
