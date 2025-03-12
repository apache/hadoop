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

# Mapping for renaming configurations
declare -A rename_configs_map=(
    ["autothrottling.enable"]="enable.autothrottling" #fs.azure.autothrottling.enable  to fs.azure.enable.autothrottling
    ["rename.dir"]="rename.key" # fs.azure.atomic.rename.dir to fs.azure.atomic.rename.key
    ["block.blob.buffered.pread.disable"]="buffered.pread.disable" #fs.azure.block.blob.buffered.pread.disable to fs.azure.buffered.pread.disable
    ["fs.azure.sas"]="fs.azure.sas.fixed.token." #fs.azure.sas.CONTAINER_NAME.ACCOUNT_NAME to fs.azure.sas.fixed.token.CONTAINER_NAME.ACCOUNT_NAME
    ["check.block.md5"]="enable.checksum.validation" #Fs.azure.check.block.md5 to fs.azure.enable.checksum.validation
)

# Configs not supported in WASB that would throw error
# User needs to remove these configs from XML file for the script to run
unsupported_configs_list=(
    "fs.azure.page.blob.dir "
    "fs.azure.block.blob.with.compaction.dir"
    "fs.azure.store.blob.md5"
)

# Configurations that are not required in ABFS Driver and can be removed
obsolete_configs_list=(
    "copyblob.retry" #fs.azure.io.copyblob.retry.min.backoff.interval, fs.azure.io.copyblob.retry.max.backoff.interval, fs.azure.io.copyblob.retry.backoff.interval, fs.azure.io.copyblob.retry.max.retries
    "fsck.temp.expiry.seconds" #fs.azure.fsck.temp.expiry.seconds
    "selfthrottling" #fs.azure.selfthrottling.enable, fs.azure.selfthrottling.read.factor, fs.azure.selfthrottling.write.factor
    "rename.threads" #fs.azure.rename.threads
    "delete.threads" #fs.azure.delete.threads
    "secure.mode" #fs.azure.secure.mode
    "local.sas.key" #fs.azure.local.sas.key.mode
    "authorization" #Fs.azure.authorization, Fs.azure.authorization.caching.enable , Fs.azure.authorization.caching.maxentries, Fs.azure.authorization.cacheentry.expiry.period, fs.azure.authorization.remote.service.urls
    "saskey" #Fs.azure.saskey.cacheentry.expiry.period , fs.azure.saskey.usecontainersaskeyforallaccess
    "chown" #Fs.azure.chown.allowed.userlist
    "chmod" #Fs.azure.chmod.allowed.userlist
    "daemon" #Fs.azure.daemon.userlist
    "kerberos" #Fs.azure.enable.kerberos.support
    "emulator" #fs.azure.storage.emulator.account.name
    "case.sensitive" #Fs.azure.blob.metadata.key.case.sensitive
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
    sed -i "/<name>.*$key.*<\/name>/d" "$OUTPUT_FILE"
done
#remove the property block if any name tag is empty
sed -i '/<property>/ { :a; N; /<\/property>/!ba; /<name>/!d; }' "$OUTPUT_FILE"

echo "Updated file: $OUTPUT_FILE"
