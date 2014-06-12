/**
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
 */

#include "common/hadoop_err.h"
#include "common/hconf.h"
#include "common/htable.h"

#include <errno.h>
#include <expat.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/** Size of the buffer to use when reading files. */
#define XML_PARSE_BUF_LEN 16384

/** The maximum number of times we'll attempt to expand a config value. */
#define MAX_EXPANSIONS 20

struct hconf_builder_entry {
    /**
     * A dynamically allocated string with the text of the entry.
     */
    char *text;

    /**
     * Nonzero if this entry is final.
     * Final entries cannot be overridden during loading, although they can be
     * overriden manually by calling hconf_builder_set.
     */
    int final;
};

struct hconf_builder {
    /**
     * Non-zero if we encountered an out-of-memory error during
     * hconf_builder_set, and will report it later during hconf_build.
     */
    int oom;

    /**
     * A hash table mapping malloced C strings to malloced hconf_builder_entry
     * objects.
     */
    struct htable *table;

    /**
     * During hconf_build, the hconf object we're in the process of building.
     */
    struct hconf *conf;
};

/**
 * A Hadoop configuration.  This is immutable once it's fully constructed.
 */
struct hconf {
    /**
     * A hash table mapping malloced C strings to malloced C strings.
     */
    struct htable *table;
};

/**
 * A hash table mapping static C strings to static C strings.
 * Protected by g_deprecation_table_once.
 */
static struct htable *DEPRECATION_TABLE;

static uv_once_t g_deprecation_table_once = UV_ONCE_INIT;

/**
 * The error we encountered when loading the deprecation table, or NULL if the
 * loading succeeded.  Protected by g_deprecation_table_once.
 */
static struct hadoop_err *g_deprecation_table_err;

/**
 * Deprecations.
 * 
 * The pattern here is:
 * [modern-key-name-a] [deprecated-alias-a-1] [deprecated-alias-a-2] ... NULL
 * [modern-key-name-b] [deprecated-alias-b-1] [deprecated-alias-b-2] ... NULL
 * ...
 * NULL NULL
 */
static const char* const DEPRECATIONS[] = {
    "fs.defaultFS", "fs.default.name", NULL,
    "dfs.client.socket-timeout", "dfs.socket.timeout", NULL,
    "dfs.client-write-packet-size", "dfs.write.packet.size", NULL,
    "dfs.client.file-block-storage-locations.timeout.millis",
                "dfs.client.file-block-storage-locations.timeout", NULL,
    "dfs.client-write-packet-size", "dfs.write.packet.size", NULL,
    NULL, NULL
};

enum xml_parse_state {
    HCXML_PARSE_INIT = 0,
    HCXML_PARSE_IN_CONFIG,
    HCXML_PARSE_IN_PROPERTY,
    HCXML_PARSE_IN_NAME,
    HCXML_PARSE_IN_VALUE,
    HCXML_PARSE_IN_FINAL,
};

struct xml_parse_ctx {
    /** Path of the current XML file we're parsing. */
    const char *path;

    /** The Hadoop configuration builder we're populating. */
    struct hconf_builder *bld;

    /** XML parse state. */
    enum xml_parse_state state;

    /** The number of parent elements we are ignoring. */
    int ignored_parents;

    /** Malloced key, if we saw one. */
    char *name;

    /** Malloced value, if we saw one. */
    char *value;

    /** Nonzero if the current property is final. */
    int final;

    /** The XML parser we're using. */
    XML_Parser parser;
};
/**
 * Initialize DEPRECATION_TABLE from DEPRECATIONS.
 */
static void init_deprecation_table(void)
{
    const char *modern_name;
    size_t i = 0;
    struct htable *table = NULL;

    // Allocate the deprecation table.
    table = htable_alloc(16, ht_hash_string, ht_compare_string);
    if (!table) {
        g_deprecation_table_err = hadoop_lerr_alloc(ENOMEM,
                "init_deprecation_table: out of memory.");
        return;
    }
    // Populate the deprecation table.
    while ((modern_name = DEPRECATIONS[i])) {
        const char *old_name;
        while ((old_name = DEPRECATIONS[++i])) {
            int ret = htable_put(table, (void*)old_name, (void*)modern_name);
            if (ret) {
                g_deprecation_table_err = hadoop_lerr_alloc(ret,
                                "init_deprecation_table: htable put of %s "
                                "failed.\n", old_name);
                htable_free(table);
                return;
            }
        }
        i++;
    }
    DEPRECATION_TABLE = table;
}

struct hadoop_err *hconf_builder_alloc(struct hconf_builder **out)
{
    struct hconf_builder *bld = NULL;
    struct hadoop_err *err = NULL;

    uv_once(&g_deprecation_table_once, init_deprecation_table);
    if (g_deprecation_table_err) {
        err = hadoop_err_copy(g_deprecation_table_err);
        goto done;
    }
    bld = calloc(1, sizeof(*bld));
    if (!bld) {
        err = hadoop_lerr_alloc(ENOMEM, "hconf_builder_alloc: OOM");
        goto done;
    }
    bld->table = htable_alloc(128, ht_hash_string, ht_compare_string);
    if (!bld->table) {
        err = hadoop_lerr_alloc(ENOMEM, "hconf_builder_alloc: OOM");
        goto done;
    }
done:
    if (err) {
        if (bld) {
            htable_free(bld->table);
            free(bld);
        }
        return err;
    }
    *out = bld;
    return NULL;
}

static void hconf_builder_free_cb(void *ctx __attribute__((unused)),
                                  void *k, void *v)
{
    struct hconf_builder_entry *entry;

    free(k);
    entry = v;
    free(entry->text);
    free(entry);
}

void hconf_builder_free(struct hconf_builder *bld)
{
    if (!bld)
        return;
    htable_visit(bld->table, hconf_builder_free_cb, NULL);
    htable_free(bld->table);
    hconf_free(bld->conf);
    free(bld);
}

/**
 * Get the most modern version of the given key.
 *
 * @param key           The key
 *
 * @return              The most modern version of the key.
 */
static const char *get_modern_key(const char *key)
{
    const char *ekey;

    ekey = htable_get(DEPRECATION_TABLE, key);
    return ekey ? ekey : key;
}

static struct hadoop_err *hconf_builder_set_internal(struct hconf_builder *bld,
                const char *key, const char *val,
                int set_final, int honor_final)
{
    struct hadoop_err *err = NULL;
    const char *ekey;
    struct hconf_builder_entry *entry;
    char *nkey = NULL;
    struct hconf_builder_entry *nentry = NULL;

    ekey = get_modern_key(key);
    if (val && val[0]) {
        nentry = calloc(1, sizeof(*nentry));
        if (!nentry)
            goto oom;
        nentry->text = strdup(val);
        if (!nentry->text)
            goto oom;
        nentry->final = set_final;
    }
    entry = htable_get(bld->table, ekey);
    if (entry) {
        void *old_key;

        if (honor_final && entry->final) {
            err = hadoop_lerr_alloc(EINVAL, "attempted to override "
                                    "final key %s", key);
            goto error;
        }
        htable_pop(bld->table, ekey, &old_key, (void**)&entry);
        free(old_key);
        free(entry->text);
        free(entry);
    }
    // Now that we've removed any old entry that might have existed, insert a
    // new entry if the val supplied is non-null and non-empty.  Hadoop's
    // configuration treats values that are empty strings the same as values
    // that are not present.
    if (nentry) {
        nkey = strdup(ekey);
        if (!nkey)
            goto oom;
        if (htable_put(bld->table, nkey, nentry))
            goto oom;
    }
    return NULL;

oom:
    bld->oom = 1;
    err = hadoop_lerr_alloc(ENOMEM, "out of memory.");
error:
    free(nkey);
    if (nentry) {
        free(nentry->text);
        free(nentry);
    }
    return err;
}

void hconf_builder_set(struct hconf_builder *bld,
                const char *key, const char *val)
{
    struct hadoop_err *err =
        hconf_builder_set_internal(bld, key, val, 0, 0);
    if (err) {
        fprintf(stderr, "hconf_builder_set(key=%s, val=%s): %s",
                key, val, hadoop_err_msg(err));
        hadoop_err_free(err);
    }
}

/**
 * Translate an hconf_builder entry into an hconf entry.
 * To do this, we need to resolve all the variable references.
 *
 * When we see a reference of the form ${variable.name}, we replace it with
 * the value of that variable within the configuration builder.
 * To prevent infinite expansions, we have two limits.  First of all, we
 * will only expand 20 times.  Second of all, we detect cycles where the entire
 * state of the string has repeated.  This could be done a bit smarter, but
 * it's nice to be compatible.
 */
static void hconf_builder_translate_cb(void *ctx, void *k, void *v)
{
    int i, j;
    struct hconf_builder *bld = ctx;
    char *key = NULL, *text = NULL;
    struct hconf_builder_entry *entry = v;
    int num_expansions = 0;
    char *prev_expansions[MAX_EXPANSIONS];

    key = strdup(k);
    text = strdup(entry->text);
    if ((!key) || (!text)) {
        bld->oom = 1;
        goto done;
    }
    i = 0;
    while (1) {
        char *ntext;
        int repeat;
        struct hconf_builder_entry *nentry;
        size_t slen, rem_len, nentry_len, nlen;


        // Look for the beginning of a variable substitution segment
        i += strcspn(text + i, "$");
        if (text[i] == '\0') {
            // We reached the end of the string without finding the beginning
            // of a substitution.
            break;
        }
        if (text[i + 1] != '{') {
            // We found a dollar sign, but it was not followed by an open
            // bracket.
            i++;
            continue;
        }
        slen = strcspn(text + i + 2, "}");
        if (text[i + 2 + slen] == '\0') {
            // We reached the end of the string without finding a close
            // bracket.
            break;
        }
        if (num_expansions == MAX_EXPANSIONS) {
            // We reached the limit on the maximum number of expansions we'll
            // perform.
            break;
        }
        // Try to expand the text inside the ${ } block.
        text[i + 2 + slen] = '\0';
        nentry = htable_get(bld->table, get_modern_key(text + i + 2));
        text[i + 2 + slen] = '}';
        if (!nentry) {
            // There was no entry corresponding to the text inside the block.
            i += slen + 1;
            continue;
        }
        // Resize the string to fit the new contents.
        rem_len = strlen(text + i + 2 + slen + 1);
        nentry_len = strlen(nentry->text);
        nlen = i + nentry_len + rem_len + 1;
        if (nlen > i + 2 + slen + 1 + rem_len) {
            ntext = realloc(text, i + nentry_len + rem_len + 1);
            if (!ntext) {
                bld->oom = 1;
                goto done;
            }
            text = ntext;
        }
        // First, copy the part after the variable expansion to its new
        // location.  Then, copy the newly expanded text into the position it
        // belongs in.
        memmove(text + i + nentry_len, text + i + 2 + slen + 1, rem_len);
        memcpy(text + i, nentry->text, nentry_len);
        text[i + nentry_len + rem_len] = '\0';
        // Check if we've expanded something to this pattern before.
        // If so, we stop the expansion immediately.
        repeat = 0;
        for (j = 0; j < num_expansions; j++) {
            if (strcmp(prev_expansions[j], text) == 0) {
                repeat = 1;
                break;
            }
        }
        if (repeat) {
            break;
        }
        // Keep track of this expansion in prev_expansions.
        prev_expansions[num_expansions] = strdup(text);
        if (!prev_expansions[num_expansions]) {
            bld->oom = 1;
            goto done;
        }
        num_expansions++;
    }
done:
    for (j = 0; j < num_expansions; j++) {
        free(prev_expansions[j]);
    }
    if (bld->oom || htable_put(bld->conf->table, key, text)) {
        bld->oom = 1;
        free(key);
        free(text);
        return;
    }
}

struct hadoop_err *hconf_build(struct hconf_builder *bld,
                struct hconf **out)
{
    struct hconf *conf = NULL;
    struct hadoop_err *err = NULL;

    if (bld->oom) {
        err = hadoop_lerr_alloc(ENOMEM, "hconf_build: out of memory.");
        goto done;
    }
    conf = calloc(1, sizeof(struct hconf));
    if (!conf) {
        err = hadoop_lerr_alloc(ENOMEM, "hconf_build: out of memory.");
        goto done;
    }
    bld->conf = conf;
    conf->table = htable_alloc(htable_capacity(bld->table),
                            ht_hash_string, ht_compare_string);
    if (!conf->table) {
        err = hadoop_lerr_alloc(ENOMEM, "hconf_build: out of memory.");
        goto done;
    }
    // Translate builder entries into configuration entries.
    htable_visit(bld->table, hconf_builder_translate_cb, bld);
    if (bld->oom) {
        err = hadoop_lerr_alloc(ENOMEM, "hconf_build: out of memory.");
        goto done;
    }
    *out = bld->conf;
    bld->conf = NULL;
    err = NULL;
done:
    hconf_builder_free(bld);
    return err;
}

static void hconf_free_cb(void *ctx __attribute__((unused)), void *k, void *v)
{
    free(k);
    free(v);
}

void hconf_free(struct hconf *conf)
{
    if (!conf)
        return;
    htable_visit(conf->table, hconf_free_cb, NULL);
    htable_free(conf->table);
    free(conf);
}

const char *hconf_get(struct hconf *conf, const char *key)
{
    const char *ekey;
    const char *val;

    ekey = get_modern_key(key);
    val = htable_get(conf->table, ekey);
    if (!val) {
        return NULL;
    }
    return val;
}

int hconf_get_int32(struct hconf *conf, const char *key,
                            int32_t *out)
{
    const char *val = hconf_get(conf, key);
    if (!val)
        return -ENOENT;
    *out = atoi(val);
    return 0;
}

int hconf_get_int64(struct hconf *conf, const char *key,
                            int64_t *out)
{
    const char *val = hconf_get(conf, key);
    if (!val)
        return -ENOENT;
    *out = atoll(val);
    return 0;
}

int hconf_get_float64(struct hconf *conf, const char *key,
                              double *out)
{
    const char *val = hconf_get(conf, key);
    if (!val)
        return -ENOENT;
    *out = atof(val);
    return 0;
}

static int xml_parse_bool(const char *path, XML_Size line_no,
                      const char *str)
{
    if (strcasecmp(str, "false") == 0) {
        return 0;
    } else if (strcasecmp(str, "true") == 0) {
        return 1;
    }
    fprintf(stderr, "hconf_builder_load_xml(%s): on line %lld, "
            "failed to parse '%s' as a boolean.  Assuming false.\n",
            path, (long long)line_no, str);
    return 0;
}

/* first when start element is encountered */
static void xml_start_element(void *data, const char *element,
                const char **attribute __attribute__((unused)))
{
    struct xml_parse_ctx *ctx = data;

    if (ctx->ignored_parents > 0) {
        ctx->ignored_parents++;
        return;
    }
    switch (ctx->state) {
    case HCXML_PARSE_INIT:
        if (!strcmp(element, "configuration")) {
            ctx->state = HCXML_PARSE_IN_CONFIG;
            return;
        }
        break;
    case HCXML_PARSE_IN_CONFIG:
        if (!strcmp(element, "property")) {
            ctx->state = HCXML_PARSE_IN_PROPERTY;
            return;
        }
        break;
    case HCXML_PARSE_IN_PROPERTY:
        if (!strcmp(element, "name")) {
            ctx->state = HCXML_PARSE_IN_NAME;
            return;
        } else if (!strcmp(element, "value")) {
            ctx->state = HCXML_PARSE_IN_VALUE;
            return;
        } else if (!strcmp(element, "final")) {
            ctx->state = HCXML_PARSE_IN_FINAL;
            return;
        }
        break;
    default:
        break;
    }
    fprintf(stderr, "hconf_builder_load_xml(%s): ignoring "
            "element '%s'\n", ctx->path, element);
    ctx->ignored_parents++;
}

/* decrement the current level of the tree */
static void xml_end_element(void *data, const char *el __attribute__((unused)))
{
    struct xml_parse_ctx *ctx = data;
    struct hadoop_err *err = NULL;

    if (ctx->ignored_parents > 0) {
        ctx->ignored_parents--;
        return;
    }
    switch (ctx->state) {
    case HCXML_PARSE_IN_CONFIG:
        ctx->state = HCXML_PARSE_INIT;
        break;
    case HCXML_PARSE_IN_PROPERTY:
        ctx->state = HCXML_PARSE_IN_CONFIG;
        if (!ctx->name) {
            fprintf(stderr, "hconf_builder_load_xml(%s): property "
                    "tag is missing <name> on line %lld\n", ctx->path,
                    (long long)XML_GetCurrentLineNumber(ctx->parser));
        } else if (!ctx->value) {
            fprintf(stderr, "hconf_builder_load_xml(%s): property "
                    "tag is missing <value> on line %lld\n", ctx->path,
                    (long long)XML_GetCurrentLineNumber(ctx->parser));
        } else {
            err = hconf_builder_set_internal(ctx->bld,
                    ctx->name, ctx->value, ctx->final, 1);
            if (err) {
                fprintf(stderr, "hconf_builder_load_xml(%s): on line "
                        "%lld, %s\n", ctx->path,
                        (long long)XML_GetCurrentLineNumber(ctx->parser),
                        hadoop_err_msg(err));
                hadoop_err_free(err);
            }
        }
        free(ctx->name);
        ctx->name = NULL;
        free(ctx->value);
        ctx->value = NULL;
        ctx->final = 0;
        break;
    case HCXML_PARSE_IN_NAME:
        ctx->state = HCXML_PARSE_IN_PROPERTY;
        break;
    case HCXML_PARSE_IN_VALUE:
        ctx->state = HCXML_PARSE_IN_PROPERTY;
        break;
    case HCXML_PARSE_IN_FINAL:
        ctx->state = HCXML_PARSE_IN_PROPERTY;
        break;
    default:
        break;
    }
}

static char *ltstrdup(const char *src, int length)
{
    char *dst = malloc(length + 1);
    if (!dst)
        return NULL;
    memcpy(dst, src, length);
    dst[length] = 0;
    return dst;
}

static void xml_handle_data(void *data, const char *content, int length)
{
    struct xml_parse_ctx *ctx = data;
    char *bool_str;

    switch (ctx->state) {
    case HCXML_PARSE_IN_NAME:
        if (ctx->name) {
            fprintf(stderr, "hconf_builder_load_xml(%s): duplicate "
                    "<name> tag on line %lld\n", ctx->path,
                    (long long)XML_GetCurrentLineNumber(ctx->parser));
        } else {
            ctx->name = ltstrdup(content, length);
            if (!ctx->name) {
                ctx->bld->oom = 1;
            }
        }
        break;
    case HCXML_PARSE_IN_VALUE:
        if (ctx->value) {
            fprintf(stderr, "hconf_builder_load_xml(%s): duplicate "
                    "<value> tag on line %lld\n", ctx->path,
                    (long long)XML_GetCurrentLineNumber(ctx->parser));
        } else {
            ctx->value = ltstrdup(content, length);
            if (!ctx->value) {
                ctx->bld->oom = 1;
            }
        }
        break;
    case HCXML_PARSE_IN_FINAL:
        bool_str = ltstrdup(content, length);
        if (!bool_str) {
            ctx->bld->oom = 1;
        } else {
            ctx->final = xml_parse_bool(ctx->path,
                XML_GetCurrentLineNumber(ctx->parser), bool_str);
            free(bool_str);
        }
        break;
    default:
        break;
    }
}

static struct hadoop_err *hconf_builder_load_xml(struct hconf_builder *bld,
                            const char *path, FILE *fp)
{
    struct hadoop_err *err = NULL;
    struct xml_parse_ctx ctx;
    char *buf = NULL;
    enum XML_Status status;
    int res = 0;

    memset(&ctx, 0, sizeof(ctx));
    ctx.bld = bld;
    ctx.path = path;
    ctx.parser = XML_ParserCreate("UTF-8");
    if (!ctx.parser) {
        err = hadoop_lerr_alloc(ENOMEM, "hconf_builder_load_xml: failed "
                                "to create libexpat XML parser.");
        goto done;
    }
    XML_SetUserData(ctx.parser, &ctx);
    XML_SetElementHandler(ctx.parser, xml_start_element, xml_end_element);
    XML_SetCharacterDataHandler(ctx.parser, xml_handle_data);
    buf = malloc(XML_PARSE_BUF_LEN);
    if (!buf) {
        err = hadoop_lerr_alloc(ENOMEM, "hconf_builder_load_xml: OOM");
        goto done;
    }
    do {
        res = fread(buf, 1, XML_PARSE_BUF_LEN, fp);
        if (res <= 0) {
            if (feof(fp)) {
                res = 0;
            } else {
                int e = errno;
                err = hadoop_lerr_alloc(e, "hconf_builder_load_xml(%s): failed "
                            "to read from file: error %d", ctx.path, e);
                goto done;
            }
        }
        status = XML_Parse(ctx.parser, buf, res, res ? XML_FALSE : XML_TRUE);
        if (status != XML_STATUS_OK) {
            enum XML_Error error = XML_GetErrorCode(ctx.parser);
            err = hadoop_lerr_alloc(EINVAL, "hconf_builder_load_xml(%s): "
                                    "parse error: %s",
                                    ctx.path, XML_ErrorString(error));
            goto done;
        }
    } while (res);
done:
    free(buf);
    free(ctx.name);
    free(ctx.value);
    if (ctx.parser) {
        XML_ParserFree(ctx.parser);
    }
    return err; 
}

struct hadoop_err *hconf_builder_load_xmls(struct hconf_builder *bld,
            const char * const* XMLS, const char *pathlist)
{
    struct hadoop_err *err = NULL;
    char *npathlist = NULL, *dir;
    char *ptr = NULL, *path = NULL;
    int ret, i;
    FILE *fp = NULL;

    npathlist = strdup(pathlist);
    if (!npathlist)
        goto oom;
    // We need to read XML files in a certain order.  For example,
    // core-site.xml must be read in before hdfs-site.xml in libhdfs.
    for (i = 0; XMLS[i]; i++) {
        for (dir = strtok_r(npathlist, ":", &ptr); dir;
                    dir = strtok_r(NULL, ":", &ptr)) {
            if (asprintf(&path, "%s/%s", dir, XMLS[i]) < 0) {
                path = NULL;
                goto oom;
            }
            fp = fopen(path, "r");
            if (!fp) {
                ret = errno;
                if ((ret != ENOTDIR) && (ret != ENOENT)) {
                    fprintf(stderr, "hconf_builder_load_xmls: failed to "
                            "open %s: error %d\n", path, ret);
                }
            } else {
                err = hconf_builder_load_xml(bld, path, fp);
                if (err)
                    goto done;
                fclose(fp);
                fp = NULL;
            }
            free(path);
            path = NULL;
        }
    }
    err = NULL;
    goto done;

oom:
    err = hadoop_lerr_alloc(ENOMEM, "hconf_builder_load_xmls: OOM");
done:
    if (fp) {
        fclose(fp);
    }
    free(npathlist);
    free(path);
    return err;
}

// vim: ts=4:sw=4:tw=79:et
