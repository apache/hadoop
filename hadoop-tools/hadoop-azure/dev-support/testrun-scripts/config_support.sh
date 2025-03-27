#!/usr/bin/env bash

set -eo pipefail

FILE=$1

if [ ! -f "$FILE" ]; then
    echo "Error: File '$FILE' not found. Exiting...."
    exit 1
fi

if [[ "$1" != *.xml ]]; then
    echo "The file provided is not an XML file. Exiting...."
    exit 1
fi

OUTPUT_FILE="abfs-converted-config.xml"
cp "$FILE" "$OUTPUT_FILE"

endpoint="dfs"
printf "Select \'HNS\' if you're migrating to Hierarchical Namespace enabled account, or \'Non-HNS\' if you're migrating to Non-Hierarchical Namespace. \n"
select namespaceType in "HNS" "NonHNS"
do
    case $namespaceType in
        HNS)
            break;
            ;;
        NonHNS)
            endpoint="blob"
            break;
            ;;
          *)
            echo "Invalid selection. Please try again. Exiting..."
            exit 1;
            ;;
    esac
done

#change value of default FS from WASB to ABFS
sed -i '/<name>fs.defaultFS<\/name>/!b;n;s|<value>wasb\(s\?\)://\([^@]*\)@\([^<]*\).blob.core.windows.net</value>|<value>abfs\1://\2@\3.'"$endpoint"'.core.windows.net</value>|' "$OUTPUT_FILE"

# Mapping for renaming configurations
declare -A rename_configs_map=(
    ["autothrottling.enable"]="enable.autothrottling" #fs.azure.autothrottling.enable  to fs.azure.enable.autothrottling
    ["rename.dir"]="rename.key" # fs.azure.atomic.rename.dir to fs.azure.atomic.rename.key
    ["block.blob.buffered.pread.disable"]="buffered.pread.disable" #fs.azure.block.blob.buffered.pread.disable to fs.azure.buffered.pread.disable
    ["fs.azure.sas"]="fs.azure.sas.fixed.token." #fs.azure.sas.CONTAINER_NAME.ACCOUNT_NAME to fs.azure.sas.fixed.token.CONTAINER_NAME.ACCOUNT_NAME
    ["check.block.md5"]="enable.checksum.validation" #fs.azure.check.block.md5 to fs.azure.enable.checksum.validation
)

# Configs not supported in WASB that would throw error
# User needs to remove these configs from XML file for the script to run
unsupported_configs_list=(
    "fs.azure.page.blob.dir"
    "fs.azure.block.blob.with.compaction.dir"
    "fs.azure.store.blob.md5"
)

# Configurations that are not required in ABFS Driver and can be removed
obsolete_configs_list=(
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
)

# Stop the script if any unsupported config is found
for key in "${unsupported_configs_list[@]}"; do
    if grep -q "$key" "$OUTPUT_FILE"; then
        echo "FAILURE: Remove the following configuration from file and rerun: '$key' "
            echo "Exiting..."
            exit 1
    fi
done

# Renaming the configs
for old in "${!rename_configs_map[@]}"; do
    new="${rename_configs_map[$old]}"
    sed -i "s/\(<name>.*\)$old\(.*<\/name>\)/\1$new\2/g" "$OUTPUT_FILE"
done

# Remove the obsolete configs
for key in "${obsolete_configs_list[@]}"; do
    sed -i "/<name>.*$key.*<\/name>/d;g" "$OUTPUT_FILE"
done

#change the endpoints for properties if migration is to HNS
if [ "$endpoint" = "dfs" ];
then
    sed -i 's/<name>\(.*\).blob.core.windows.net\(.*\)<\/name>/<name>\1.dfs.core.windows.net\2<\/name>/g' "$OUTPUT_FILE"
fi

#remove the property block if any name tag is empty
sed -i '/<property>/ { :a; N; /<\/property>/!ba; /<name>/!d; }' "$OUTPUT_FILE"

echo "Updated file: $OUTPUT_FILE"
