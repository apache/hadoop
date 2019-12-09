#ifndef URIPARSER2_H_
#define URIPARSER2_H_

#ifdef __cplusplus
#include <string>
#endif

/**
 * URI object. After the call to uri_parse() fields will be NULL (0 for the port) if their component was absent in the input string.
 */
typedef struct URI {
	const char *scheme;
	const char *user;
	const char *pass;
	const char *host;
	unsigned short port;
	const char *path;
	const char *query;
	const char *fragment;
#ifdef __cplusplus
	const void *const reserved;

	URI(const char *uri = 0);
	~URI();

	bool operator<(const URI& uri) const;
	bool operator>(const URI& uri) const;
	bool operator<=(const URI& uri) const;
	bool operator>=(const URI& uri) const;
	bool operator==(const URI& uri) const;
	bool operator!=(const URI& uri) const;

	std::string to_string() const;
#endif
} URI;

#ifdef __cplusplus

#include <ostream>
#include <cstdlib>

extern "C" void *uri_parse2(const char *uri, URI *target);
extern "C" char *uri_build(const URI *uri);
extern "C" int uri_compare(const URI *a, const URI *b);

inline URI::URI(const char* uri): reserved(uri ? uri_parse2(uri, this) : 0) {
}

inline URI::~URI() {
	free((void *) reserved);
}

inline std::string URI::to_string() const {
	char *s = uri_build(this);	/* FIXME handle NULL return value (ENOMEM) */
	std::string rv(s);
	free(s);
	return rv;
}

inline bool URI::operator<(const URI& uri) const { return uri_compare(this, &uri) < 0; }
inline bool URI::operator>(const URI& uri) const { return uri_compare(this, &uri) > 0; }
inline bool URI::operator<=(const URI& uri) const { return uri_compare(this, &uri) <= 0; }
inline bool URI::operator>=(const URI& uri) const { return uri_compare(this, &uri) >= 0; }
inline bool URI::operator==(const URI& uri) const { return uri_compare(this, &uri) == 0; }
inline bool URI::operator!=(const URI& uri) const { return uri_compare(this, &uri) != 0; }

static inline std::ostream& operator<<(std::ostream& os, const URI& uri) {
	return os << uri.to_string();
}

#else	/* defined(__cplusplus) */

/**
 * Parse URI into its components.
 *
 * @param uri The URI to parse.
 * @return URI object. The caller is responsible for freeing this object. NULL is returned on parse error or out-of-memory conditions (in the latter case errno=ENOMEM).
 */
URI *uri_parse(const char *uri);

/**
 * Create string representation of URI object.
 *
 * @param uri URI object.
 * @return URI as a string. The caller is responsible for freeing this object. NULL is returned on out-of-memory conditions (errno=ENOMEM).
 */
char *uri_build(const URI *uri);

/**
 * Compare two URI objects. Follows the strcmp() contract. The order in which components are compared is as follows: scheme, host, port, path, query, fragment, user, pass.
 * NULL components are always smaller than their non-NULL counterparts. That is, a < b if a->scheme == NULL and b->scheme != NULL.
 *
 * @param a First URI object.
 * @param b Second URI object.
 * @return -1 if a < b, 0 if a == b, 1 if a > b.
 */
int uri_compare(const URI *a, const URI *b);

#endif	/* __cplusplus */

#endif	/* uriparser2.h */
