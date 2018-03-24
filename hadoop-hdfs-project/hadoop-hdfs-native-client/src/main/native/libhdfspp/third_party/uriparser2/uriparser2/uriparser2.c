#include <stdlib.h>
#include <string.h>
#include <errno.h>

#include "uriparser/Uri.h"
#include "uriparser2.h"

/* copy n bytes from src to dst and add a nul byte. dst must be large enough to hold n + 1 bytes. */
static char *memcpyz(char *dst, const char *src, int n) {
	memcpy(dst, src, n);
	dst[n] = '\0';
	return dst;
}

/* returns the number of chars required to store the range as a string, including the nul byte */
static int range_size(const UriTextRangeA *r) {
	if (r->first && r->first != r->afterLast) {
		return 1 + (r->afterLast - r->first);
	}
	return 0;
}

/* returns the number of chars required to store the path, including the nul byte */
static int path_size(const UriPathSegmentA *ps) {
	if (ps) {
		/* +1 for the nul byte; the extra byte from range_size() is used for the leading slash */
		int size = 1;
		for (; ps != 0; ps = ps->next) {
			size += range_size(&ps->text);
		}
		return size;
	}
	return 0;
}

static int uri_size(const UriUriA *uu) {
	return range_size(&uu->scheme)
		+ range_size(&uu->userInfo) + 1	/* userinfo will be split on : */
		+ range_size(&uu->hostText)
		+ path_size(uu->pathHead)
		+ range_size(&uu->query)
		+ range_size(&uu->fragment);
}

static const char *copy_range(const UriTextRangeA *r, char **buffer) {
	const int size = r->afterLast - r->first;
	if (size) {
		const char *s = *buffer;
		memcpyz(*buffer, r->first, size);
		*buffer += size + 1;
		return s;
	}
	return 0;
}

static const char *copy_path(const UriPathSegmentA *ps, char **buffer) {
	const char *s = *buffer;

	for (; ps != 0; ps = ps->next) {
		**buffer = '/'; (*buffer)++;
		copy_range(&ps->text, buffer);
		if (ps->next) {
			/* chop off trailing null, we'll append at least one more path segment */
			(*buffer)--;
		}
	}

	return s;
}

static int parse_int(const char *first, const char *after_last) {
	const int size = after_last - first;
	if (size) {
		char buffer[size + 1];
		memcpyz(buffer, first, size);
		return atoi(buffer);
	}
	return 0;
}

static void parse_user_info(URI *uri, const UriTextRangeA *r, char **buffer) {
	uri->user = uri->pass = 0;

	const int size = r->afterLast - r->first;
	if (size) {
		char *colon = memchr(r->first, ':', size);

		const int user_size = (colon ? colon : r->afterLast) - r->first;
		const int pass_size = r->afterLast - (colon ? colon + 1 : r->afterLast);

		if (user_size) {
			uri->user = memcpyz(*buffer, r->first, user_size);
			*buffer += user_size + 1;
		}
		if (pass_size) {
			uri->pass = memcpyz(*buffer, colon + 1, pass_size);
			*buffer += pass_size + 1;
		}
	}
}

static void init_uri(const UriUriA *uu, URI *uri, char *buffer) {
	uri->scheme = copy_range(&uu->scheme, &buffer);
	uri->user = 0;
	uri->pass = 0;
	uri->host = copy_range(&uu->hostText, &buffer);
	uri->port = parse_int(uu->portText.first, uu->portText.afterLast);
	uri->path = copy_path(uu->pathHead, &buffer);
	uri->query = copy_range(&uu->query, &buffer);
	uri->fragment = copy_range(&uu->fragment, &buffer);
	parse_user_info(uri, &uu->userInfo, &buffer);
}

/* this function saves the URI components after the URI object itself, so it can be released with a single call to free() */
URI *uri_parse(const char *input) {
	UriParserStateA state;
	UriUriA uu;
	URI *uri;

	state.uri = &uu;
	if (URI_SUCCESS == uriParseUriA(&state, input)) {
		uri = calloc(1, sizeof(*uri) + uri_size(&uu));
		if (uri) {
			init_uri(&uu, uri, (char *) (uri + 1));
		} else {
			/* work around non-conformant malloc() implementations */
			errno = ENOMEM;
		}
	} else {
		uri = 0;
	}

	int saved_errno = errno;
	uriFreeUriMembersA(&uu);
	errno = saved_errno;

	return uri;
}

/* this is a helper function for the C++ constructor that saves the URI components to a separately malloc()'ed buffer */
void *uri_parse2(const char *input, URI *uri) {
	UriParserStateA state;
	char *buffer;
	UriUriA uu;

	state.uri = &uu;
	if (URI_SUCCESS == uriParseUriA(&state, input)) {
		buffer = malloc(uri_size(&uu));
		if (buffer) {
			init_uri(&uu, uri, buffer);
		} else {
			/* work around non-conformant malloc() implementations */
			errno = ENOMEM;
		}
	} else {
		buffer = 0;
	}

	int saved_errno = errno;
	uriFreeUriMembersA(&uu);
	errno = saved_errno;

	return buffer;
}

static char *append(char *dst, const char *src) {
	const int size = strlen(src);
	memcpy(dst, src, size);
	return dst + size;
}

static int power_of_10(int n) {
	int i;
	for (i = 0; n > 0; i++, n /= 10);
	return i;
}

char *uri_build(const URI *uri) {
	int size = 0;

	if (uri->scheme) {
		size += strlen(uri->scheme) + 3;	/* "://" */
	}
	if (uri->user) {
		size += strlen(uri->user) + 1;		/* ":" or "@" */
	}
	if (uri->pass) {
		size += strlen(uri->pass) + 1;		/* "@" */
	}
	if (uri->host) {
		size += strlen(uri->host);
	}
	if (uri->port) {
		size += 1 + power_of_10(uri->port);	/* ":" port */
	}
	if (uri->path) {
		size += strlen(uri->path);
	}
	if (uri->query) {
		size += 1 + strlen(uri->query);		/* "?" query */
	}
	if (uri->fragment) {
		size += 1 + strlen(uri->fragment);	/* "#" fragment */
	}

	char *s = malloc(size + 1);
	if (s) {
		char *p = s;
		if (uri->scheme) {
			p = append(p, uri->scheme);
			*p++ = ':';
			*p++ = '/';
			*p++ = '/';
		}
		if (uri->user) {
			p = append(p, uri->user);
		}
		if (uri->pass) {
			*p++ = ':';
			p = append(p, uri->pass);
		}
		if (uri->user || uri->pass) {
			*p++ = '@';
		}
		if (uri->host) {
			p = append(p, uri->host);
		}
		if (uri->port) {
			p += sprintf(p, ":%d", uri->port);
		}
		if (uri->path) {
			p = append(p, uri->path);
		}
		if (uri->query) {
			*p++ = '?';
			p = append(p, uri->query);
		}
		if (uri->fragment) {
			*p++ = '#';
			p = append(p, uri->fragment);
		}
		*p = '\0';
	}

	return s;
}

/* NULL-safe string comparison. a < b if a is NULL and b is not (and vice versa). */
#define COMPARE(a, b)			\
	if (a && b) {			\
		int n = strcmp(a, b);	\
		if (n) return n;	\
	} else if (a || b) {		\
		return a ? 1 : -1;	\
	}

int uri_compare(const URI *a, const URI *b) {
	COMPARE(a->scheme, b->scheme);
	COMPARE(a->host, b->host);

	if (a->port != b->port) {
		return a->port > b->port ? 1 : -1;
	}

	COMPARE(a->path, b->path);
	COMPARE(a->query, b->query);
	COMPARE(a->fragment, b->fragment);

	COMPARE(a->user, b->user);
	COMPARE(a->pass, b->pass);

	return 0;
}
