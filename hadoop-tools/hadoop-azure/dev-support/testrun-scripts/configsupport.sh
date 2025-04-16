#!/usr/bin/env bash

set -eo pipefail

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FILE=$1
OUTPUT_FILE=""

# ------------------------------------------------------------------------------
# Initialization and Dependency Checks
# ------------------------------------------------------------------------------

checkDependency() {
    if ! command -v xmlstarlet &> /dev/null; then
        echo "ERROR: 'xmlstarlet' is not installed. Please install it to run this script."
        echo "Exiting..."
        exit 1
    fi
}

validateInputFile() {
    if [ ! -f "$FILE" ]; then
        echo "Error: File '$FILE' not found. Exiting...."
        exit 1
    fi

    if [[ "$FILE" != *.xml ]]; then
        echo "The file provided is not an XML file. Exiting...."
        exit 1
    fi
}

init() {
    checkDependency
    validateInputFile
    OUTPUT_FILE="abfs-converted-config.xml"
    cp "$FILE" "$OUTPUT_FILE"
}

# ------------------------------------------------------------------------------
# Global Variables & Config Mappings
# ------------------------------------------------------------------------------

contactTeamMsg="For any queries or support, kindly reach out to us at 'askabfs@microsoft.com'."
endpoint=".dfs."

# Mapping for renaming configurations
declare -A renameConfigsMap=(
    ["autothrottling.enable"]="enable.autothrottling" #fs.azure.autothrottling.enable  to fs.azure.enable.autothrottling
    ["rename.dir"]="rename.key" # fs.azure.atomic.rename.dir to fs.azure.atomic.rename.key
    ["block.blob.buffered.pread.disable"]="buffered.pread.disable" #fs.azure.block.blob.buffered.pread.disable to fs.azure.buffered.pread.disable
    ["fs.azure.sas"]="fs.azure.sas.fixed.token." #fs.azure.sas.CONTAINER_NAME.ACCOUNT_NAME to fs.azure.sas.fixed.token.CONTAINER_NAME.ACCOUNT_NAME
    ["check.block.md5"]="enable.checksum.validation" #fs.azure.check.block.md5 to fs.azure.enable.checksum.validation
)

# Configs not supported in ABFS
unsupportedConfigsList=(
    "fs.azure.page.blob.dir"
    "fs.azure.block.blob.with.compaction.dir"
    "fs.azure.store.blob.md5"
)

# Configurations not required in ABFS Driver and can be removed
obsoleteConfigsList=(
    "azure.authorization" #fs.azure.authorization, fs.azure.authorization.caching.enable , fs.azure.authorization.caching.maxentries, fs.azure.authorization.cacheentry.expiry.period, fs.azure.authorization.remote.service.urls
    "azure.selfthrottling" #fs.azure.selfthrottling.enable, fs.azure.selfthrottling.read.factor, fs.azure.selfthrottling.write.factor
    "azure.saskey" #fs.azure.saskey.cacheentry.expiry.period , fs.azure.saskey.usecontainersaskeyforallaccess
    "copyblob.retry" #fs.azure.io.copyblob.retry.min.backoff.interval, fs.azure.io.copyblob.retry.max.backoff.interval, fs.azure.io.copyblob.retry.backoff.interval, fs.azure.io.copyblob.retry.max.retries
    "service.urls" #fs.azure.cred.service.urls , fs.azure.delegation.token.service.urls, fs.azure.authorization.remote.service.urls
    "blob.metadata.key.case.sensitive" #fs.azure.blob.metadata.key.case.sensitive
    "cacheentry.expiry.period" #fs.azure.cacheentry.expiry.period
    "chmod.allowed.userlist" #fs.azure.chmod.allowed.userlist
    "chown.allowed.userlist" #fs.azure.chown.allowed.userlist
    "daemon.userlist" #fs.azure.daemon.userlist
    "delete.threads" #fs.azure.delete.threads
    "enable.kerberos.support" #fs.azure.enable.kerberos.support
    "flatlist.enable" #fs.azure.flatlist.enable
    "fsck.temp.expiry.seconds" #fs.azure.fsck.temp.expiry.seconds
    "local.sas.key.mode" #fs.azure.local.sas.key.mode
    "override.canonical.service.name" #fs.azure.override.canonical.service.name
    "permissions.supergroup" #fs.azure.permissions.supergroup
    "rename.threads" #fs.azure.rename.threads
    "secure.mode" #fs.azure.secure.mode
    "skip.metrics" #fs.azure.skip.metrics
    "storage.client.logging" #fs.azure.storage.client.logging
    "storage.emulator.account.name" #fs.azure.storage.emulator.account.name
    "storage.timeout" #fs.azure.storage.timeout
    "enable.append.support" #fs.azure.enable.append.support
)

# ------------------------------------------------------------------------------
# User Interaction
# ------------------------------------------------------------------------------

promptNamespaceType() {
    printf "Select 'HNS' if you're migrating to ABFS driver for Hierarchical Namespace enabled account,
            or 'Non-HNS' if you're migrating to ABFS driver for Non-Hierarchical Namespace (FNS) account. \n"
    printf "WARNING: Please ensure the correct option is chosen as it will affect the configuration changes made to the file. \n"
    printf "If you are unsure, follow the instructions below to check from Azure Portal: \n"
    printf "* Go to the Azure Portal and navigate to your storage account. \n"
    printf "* In the left-hand menu, select 'Overview' section and look for 'Properties'. \n"
    printf "* Under 'Blob service', check if 'Hierarchical namespace' is enabled or disabled. \n"
    echo "$contactTeamMsg"
    select namespaceType in "HNS" "NonHNS"
    do
        case $namespaceType in
            HNS)
                xmlstarlet ed -L -i '//configuration/property[1]' -t elem -n property -v '' \
                    -s '//configuration/property[1]' -t elem -n name -v 'fs.azure.account.hns.enabled' \
                    -s '//configuration/property[1]' -t elem -n value -v 'true' "$OUTPUT_FILE"
                break
                ;;
            NonHNS)
                endpoint=".blob."
                break
                ;;
            *)
                echo "Invalid selection. Please try again. Exiting..."
                exit 1
                ;;
        esac
    done
}

# ------------------------------------------------------------------------------
# Config File Transformations
# ------------------------------------------------------------------------------

# Stop the script if any unsupported config is found
unsupportedConfigCheck() {
    for key in "${unsupportedConfigsList[@]}"; do
        if grep -q "$key" "$OUTPUT_FILE"; then
            echo "Remove the following configuration from file and rerun: '$key'"
            failure=true
        fi
    done

    if [ "$failure" = true ]; then
        echo "FAILURE: Unsupported Config Found"
        echo "$contactTeamMsg"
        echo "Exiting..."
        exit 1
    fi
}

# Renaming the configs
renameConfigs() {
    for old in "${!renameConfigsMap[@]}"; do
        new="${renameConfigsMap[$old]}"
        xmlstarlet ed -L -u "//property/name[contains(., '$old')]" -x "concat(substring-before(., '$old'),
         '$new', substring-after(., '$old'))" "$OUTPUT_FILE"
    done
}

# Remove the obsolete configs
removeObsoleteConfigs() {
    for key in "${obsoleteConfigsList[@]}"; do
        xmlstarlet ed -L -d "//property[name[contains(text(), '$key')]]" "$OUTPUT_FILE"
    done
}

# Change the endpoints to DFS if migrating to HNS
changeEndpointForHNS() {
    if [ "$endpoint" = ".dfs." ]; then
        xmlstarlet ed -L -u "//property/name[contains(., '.blob.')]" -x "concat(substring-before(., '.blob.'),
         '$endpoint', substring-after(., '.blob.'))" "$OUTPUT_FILE"
    fi
}

# Change the value of fs.defaultFS
handleDefaultFSValue() {
    if xmlstarlet sel -t -v "//property[name='fs.defaultFS']/value" "$OUTPUT_FILE" | grep -q "."; then
        if xmlstarlet sel -t -v "//property[name='fs.defaultFS']/value" "$OUTPUT_FILE" | grep -q ".blob."; then
            xmlstarlet ed -L -u "//property[name='fs.defaultFS']/value" -x "concat('abfs', substring-before(substring-after(., 'wasb'), '@'),
             '@', substring-before(substring-after(., '@'), '.blob.'), '$endpoint', 'core.windows.net')" "$OUTPUT_FILE"
        else
            echo "ERROR: 'fs.defaultFS' does not have 'Blob' as endpoint. Exiting..."
            echo "$contactTeamMsg"
            exit 1
        fi
    fi
}

# ------------------------------------------------------------------------------
# Script Execution
# ------------------------------------------------------------------------------

init
promptNamespaceType
unsupportedConfigCheck
removeObsoleteConfigs
renameConfigs
changeEndpointForHNS
handleDefaultFSValue

# Clean up any <property> blocks with empty <name> tags
xmlstarlet ed -L -d "//property[not(name) or name='']" "$OUTPUT_FILE"

echo "Updated file: $OUTPUT_FILE"
