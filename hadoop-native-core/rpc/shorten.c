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

#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define MAX_LINE_LEN                    16384
#define IFNDEF                          "#ifndef"
#define IFNDEF_LEN                      (sizeof(IFNDEF) - 1)

enum parse_state {
    PARSE_IFNDEF = 0,
    PARSE_STRUCTS_AND_ENUMS,
    PARSE_MESSAGES,
    PARSE_DONE,
};

#define PROTOBUF_C_END_DECLS_STR "PROTOBUF_C_END_DECLS"

static const char *PARSE_STATE_TERMINATORS[] = {
    "PROTOBUF_C_BEGIN_DECLS",
    "/* --- messages --- */",
    PROTOBUF_C_END_DECLS_STR
};

static const char *MESSAGE_SUFFIXES[] = {
    "__INIT",
    "__get_packed_size",
    "__pack",
    "__pack_to_buffer",
    "__unpack",
    "__free_unpacked",
};

#define NUM_MESSAGE_SUFFIXES \
    (sizeof(MESSAGE_SUFFIXES) / sizeof(MESSAGE_SUFFIXES[0]))

static void add_word(char ***words, size_t *num_words, const char *word)
{
    size_t new_num_words;
    char *nword;
    char **nwords;

    new_num_words = *num_words + 1;
    nword = strdup(word);
    if (!nword) {
        fprintf(stderr, "failed to allocate memory for %Zd words\n",
                new_num_words);
        exit(1);
    }
    nwords = realloc(*words, sizeof(char **) * new_num_words);
    if (!nwords) {
        fprintf(stderr, "failed to allocate memory for %Zd words\n",
                new_num_words);
        free(nword);
        exit(1);
    }
    nwords[new_num_words - 1] = nword;
    *num_words = new_num_words;
    *words = nwords;
}

static int has_suffix(const char *str, const char *suffix)
{
    int str_len = strlen(str);
    int suffix_len = strlen(suffix);
    if (str_len < suffix_len)
        return 0;
    return strcmp(str + str_len - suffix_len, suffix) == 0;
}

static int has_message_suffix(const char *word)
{
    size_t i = 0;

    for (i = 0; i < NUM_MESSAGE_SUFFIXES; i++) {
        if (has_suffix(word, MESSAGE_SUFFIXES[i]))
            return 1;
    }
    return 0;
}

static void add_words(char ***words, size_t *num_words,
                    char *line, enum parse_state state)
{
    char *word, *ptr = NULL;

    for (word = strtok_r(line, " ", &ptr); word; 
             word = strtok_r(NULL, " ", &ptr)) {
        if (word[0] == '_')
            continue;
        if (!strstr(word, "__"))
            continue;
        if ((state == PARSE_MESSAGES) && (!has_message_suffix(word)))
            continue;
        add_word(words, num_words, word);
    }
}

static int compare_strings(const void *a, const void *b)
{
    return strcmp(*(char * const*)a, *(char * const*)b);
}

static char *get_last_occurrence(char *haystack, const char *needle)
{
    char *val = NULL, *nval;
    int needle_len = strlen(needle);

    while (1) {
        nval = strstr(haystack, needle);
        if (!nval)
            return val;
        val = nval + needle_len;
        haystack = nval + needle_len;
    }
}

static char *get_second_last_occurrence(char *haystack, const char *needle)
{
    char *pval = NULL, *val = NULL, *nval;
    int needle_len = strlen(needle);

    while (1) {
        nval = strstr(haystack, needle);
        if (!nval)
            return pval;
        pval = val;
        val = nval + needle_len;
        haystack = nval + needle_len;
    }
}

static int has_camel_case(const char *str)
{
    int i, prev_lower = 0;

    for (i = 0; str[i]; i++) {
        if (isupper(str[i])) {
            if (prev_lower)
                return 1;
        } else if (islower(str[i])) {
            prev_lower = 1;
        }
    }
    return 0;
}

static char *get_shortened_occurrence(char *str)
{
    char *last, *slast;
        
    last = get_last_occurrence(str, "__");
    slast = get_second_last_occurrence(str, "__");

    last = get_last_occurrence(str, "__");
    if (!last)
        return NULL;
    if ((!has_message_suffix(str)) && 
            (strstr(last, "_") || has_camel_case(last))) { 
        // Heuristic: if the last bit of the string after the double underscore
        // has another underscore inside, or has mixed case, we assume it's
        // complex enough to use on its own.
        return last;
    }
    // Otherwise, we grab the part of the string after the second-last double
    // underscore.
    slast = get_second_last_occurrence(str, "__");
    return slast ? slast : last;
}

static int output_shortening_macros(char **words, size_t num_words,
                                    const char *out_path, FILE *out)
{
    size_t i;
    const char *prev_word = "";
    const char *shortened;

    for (i = 0; i < num_words; i++) {
        if (strcmp(prev_word, words[i]) == 0) {
            // skip words we've already done
            continue;
        }
        prev_word = words[i];
        shortened = get_shortened_occurrence(words[i]);
        if (shortened) {
            if (fprintf(out, "#define %s %s\n", shortened, words[i]) < 0) {
                fprintf(stderr, "error writing to %s\n", out_path);
                return EIO;
            }
        }
    }
    return 0;
}

/**
 * Remove newlines from a buffer.
 *
 * @param line          The buffer.
 */
static void chomp(char *line)
{
    while (1) {
        int len = strlen(line);
        if (len == 0) {
            return;
        }
        if (line[len - 1] != '\n') {
            return;
        }
        line[len - 1] = '\0';
    }
}

/**
 * Remove most non-alphanumeric characters from a buffer.
 *
 * @param line          The buffer.
 */
static void asciify(char *line)
{
    int i;

    for (i = 0; line[i]; i++) {
        if ((!isalnum(line[i])) && (line[i] != '_') && (line[i] != '#')) {
            line[i] = ' ';
        }
    }
}

static const char *base_name(const char *path)
{
    const char *base;
    
    base = rindex(path, '/');
    if (!base)
        return NULL;
    return base + 1;
}

static int process_file_lines(const char *in_path, const char *out_path,
                    FILE *in, FILE *out, char ***words, size_t *num_words)
{
    int ret;
    char header_guard[MAX_LINE_LEN] = { 0 };
    char line[MAX_LINE_LEN] = { 0 };
    const char *base = base_name(in_path);
    enum parse_state state = PARSE_IFNDEF;

    if (!base) {
        fprintf(stderr, "failed to get basename of %s\n", in_path);
        return EINVAL;
    }
    while (1) {
        if (!fgets(line, MAX_LINE_LEN - 1, in)) {
            if (ferror(in)) {
                ret = errno;
                fprintf(stderr, "error reading %s: %s (%d)\n",
                        in_path, strerror(ret), ret);
                return ret;
            }
            fprintf(stderr, "error reading %s: didn't find " 
                    PROTOBUF_C_END_DECLS_STR, in_path);
            return EINVAL;
        }
        if (strstr(line, PARSE_STATE_TERMINATORS[state])) {
            state = state + 1;
            if (state == PARSE_DONE) {
                break;
            }
            continue;
        }
        chomp(line);
        asciify(line);
        switch (state) {
        case PARSE_IFNDEF:
            if (strncmp(line, IFNDEF, IFNDEF_LEN) == 0) {
                strcpy(header_guard, line + IFNDEF_LEN + 1);
            }
            break;
        default:
            add_words(words, num_words, line, state);
            break;
        }
    }
    if (!header_guard[0]) {
        fprintf(stderr, "failed to find header guard for %s\n", in_path);
        return EINVAL;
    }
    qsort(*words, *num_words, sizeof(char*), compare_strings);
    fprintf(out, "#ifndef %s_S\n", header_guard);
    fprintf(out, "#define %s_S\n\n", header_guard);
    fprintf(out, "#include \"%s\"\n\n", base);
    ret = output_shortening_macros(*words, *num_words, out_path, out);
    if (ret)
        return ret;
    fprintf(out, "\n#endif\n");
    return 0;
}

static int process_file(const char *in_path)
{
    char out_path[PATH_MAX] = { 0 };
    int res, ret = 0;
    FILE *in = NULL, *out = NULL;
    char **words = NULL;
    size_t num_words = 0;
    size_t i;

    res = snprintf(out_path, PATH_MAX, "%s.s", in_path);
    if ((res < 0) || (res >= PATH_MAX)) {
        fprintf(stderr, "snprintf error for %s\n", in_path);
        ret = EINVAL;
        goto done;
    }
    in = fopen(in_path, "r");
    if (!in) {
        ret = errno;
        fprintf(stderr, "failed to open %s for read: error %s (%d)\n",
                in_path, strerror(ret), ret);
        goto done;
    }
    out = fopen(out_path, "w");
    if (!out) {
        ret = errno;
        fprintf(stderr, "failed to open %s for write: error %s (%d)\n",
                out_path, strerror(ret), ret);
        goto done;
    }
    ret = process_file_lines(in_path, out_path, in, out, &words, &num_words);
    for (i = 0; i < num_words; i++) {
        free(words[i]);
    }
    free(words);
    if (ret) {
        goto done;
    }
    if (fclose(out)) {
        ret = errno;
        perror("fclose error");
    }
    out = NULL;
done:
    if (in) {
        fclose(in);
    }
    if (out) {
        fclose(out);
    }
    return ret;
}

static void usage(void)
{
        fprintf(stderr,
"shorten: creates header files with shorter definitions for protobuf-c\n"
"definitions.  Output files will be written to the same paths as input\n"
"files, but with a .s extension tacked on.\n"
"\n"
"usage: shorten [paths-to-headers]\n");
}

int main(int argc, char **argv)
{
    int i, ret, nproc = 0, rval = EXIT_SUCCESS;

    if (argc < 2) {
        usage();
        exit(EXIT_SUCCESS);
    }
    for (i = 1; i < argc; i++) {
        ret = process_file(argv[i]);
        if (ret) {
            fprintf(stderr, "error processing %s\n", argv[i]);
            rval = EXIT_FAILURE;
        } else {
            nproc++;
        }
    }
    //fprintf(stderr, "successfully processed %d files\n", nproc);
    return rval;
}

// vim: ts=4:sw=4:tw=79:et
