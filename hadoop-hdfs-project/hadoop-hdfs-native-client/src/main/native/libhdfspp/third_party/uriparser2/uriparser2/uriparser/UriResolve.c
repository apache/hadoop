/*
 * uriparser - RFC 3986 URI parsing library
 *
 * Copyright (C) 2007, Weijia Song <songweijia@gmail.com>
 * Copyright (C) 2007, Sebastian Pipping <webmaster@hartwork.org>
 * All rights reserved.
 *
 * Redistribution  and use in source and binary forms, with or without
 * modification,  are permitted provided that the following conditions
 * are met:
 *
 *     * Redistributions   of  source  code  must  retain  the   above
 *       copyright  notice, this list of conditions and the  following
 *       disclaimer.
 *
 *     * Redistributions  in  binary  form must  reproduce  the  above
 *       copyright  notice, this list of conditions and the  following
 *       disclaimer   in  the  documentation  and/or  other  materials
 *       provided with the distribution.
 *
 *     * Neither  the name of the <ORGANIZATION> nor the names of  its
 *       contributors  may  be  used to endorse  or  promote  products
 *       derived  from  this software without specific  prior  written
 *       permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS  IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT  NOT
 * LIMITED  TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND  FITNESS
 * FOR  A  PARTICULAR  PURPOSE ARE DISCLAIMED. IN NO EVENT  SHALL  THE
 * COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL,    SPECIAL,   EXEMPLARY,   OR   CONSEQUENTIAL   DAMAGES
 * (INCLUDING,  BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES;  LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT  LIABILITY,  OR  TORT (INCLUDING  NEGLIGENCE  OR  OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 */

/* What encodings are enabled? */
#include "UriDefsConfig.h"
#if (!defined(URI_PASS_ANSI) && !defined(URI_PASS_UNICODE))
/* Include SELF twice */
# define URI_PASS_ANSI 1
# include "UriResolve.c"
# undef URI_PASS_ANSI
# define URI_PASS_UNICODE 1
# include "UriResolve.c"
# undef URI_PASS_UNICODE
#else
# ifdef URI_PASS_ANSI
#  include "UriDefsAnsi.h"
# else
#  include "UriDefsUnicode.h"
#  include <wchar.h>
# endif



#ifndef URI_DOXYGEN
# include "Uri.h"
# include "UriCommon.h"
#endif



/* Appends a relative URI to an absolute. The last path segement of
 * the absolute URI is replaced. */
static URI_INLINE UriBool URI_FUNC(MergePath)(URI_TYPE(Uri) * absWork,
		const URI_TYPE(Uri) * relAppend) {
	URI_TYPE(PathSegment) * sourceWalker;
	URI_TYPE(PathSegment) * destPrev;
	if (relAppend->pathHead == NULL) {
		return URI_TRUE;
	}

	/* Replace last segment ("" if trailing slash) with first of append chain */
	if (absWork->pathHead == NULL) {
		URI_TYPE(PathSegment) * const dup = malloc(sizeof(URI_TYPE(PathSegment)));
		if (dup == NULL) {
			return URI_FALSE; /* Raises malloc error */
		}
		dup->next = NULL;
		absWork->pathHead = dup;
		absWork->pathTail = dup;
	}
	absWork->pathTail->text.first = relAppend->pathHead->text.first;
	absWork->pathTail->text.afterLast = relAppend->pathHead->text.afterLast;

	/* Append all the others */
	sourceWalker = relAppend->pathHead->next;
	if (sourceWalker == NULL) {
		return URI_TRUE;
	}
	destPrev = absWork->pathTail;

	for (;;) {
		URI_TYPE(PathSegment) * const dup = malloc(sizeof(URI_TYPE(PathSegment)));
		if (dup == NULL) {
			destPrev->next = NULL;
			absWork->pathTail = destPrev;
			return URI_FALSE; /* Raises malloc error */
		}
		dup->text = sourceWalker->text;
		destPrev->next = dup;

		if (sourceWalker->next == NULL) {
			absWork->pathTail = dup;
			absWork->pathTail->next = NULL;
			break;
		}
		destPrev = dup;
		sourceWalker = sourceWalker->next;
	}

	return URI_TRUE;
}



static int URI_FUNC(AddBaseUriImpl)(URI_TYPE(Uri) * absDest,
		const URI_TYPE(Uri) * relSource,
		const URI_TYPE(Uri) * absBase) {
	if (absDest == NULL) {
		return URI_ERROR_NULL;
	}
	URI_FUNC(ResetUri)(absDest);

	if ((relSource == NULL) || (absBase == NULL)) {
		return URI_ERROR_NULL;
	}

	/* absBase absolute? */
	if (absBase->scheme.first == NULL) {
		return URI_ERROR_ADDBASE_REL_BASE;
	}

	/* [01/32]	if defined(R.scheme) then */
				if (relSource->scheme.first != NULL) {
	/* [02/32]		T.scheme = R.scheme; */
					absDest->scheme = relSource->scheme;
	/* [03/32]		T.authority = R.authority; */
					if (!URI_FUNC(CopyAuthority)(absDest, relSource)) {
						return URI_ERROR_MALLOC;
					}
	/* [04/32]		T.path = remove_dot_segments(R.path); */
					if (!URI_FUNC(CopyPath)(absDest, relSource)) {
						return URI_ERROR_MALLOC;
					}
					if (!URI_FUNC(RemoveDotSegmentsAbsolute)(absDest)) {
						return URI_ERROR_MALLOC;
					}
	/* [05/32]		T.query = R.query; */
					absDest->query = relSource->query;
	/* [06/32]	else */
				} else {
	/* [07/32]		if defined(R.authority) then */
					if (URI_FUNC(IsHostSet)(relSource)) {
	/* [08/32]			T.authority = R.authority; */
						if (!URI_FUNC(CopyAuthority)(absDest, relSource)) {
							return URI_ERROR_MALLOC;
						}
	/* [09/32]			T.path = remove_dot_segments(R.path); */
						if (!URI_FUNC(CopyPath)(absDest, relSource)) {
							return URI_ERROR_MALLOC;
						}
						if (!URI_FUNC(RemoveDotSegmentsAbsolute)(absDest)) {
							return URI_ERROR_MALLOC;
						}
	/* [10/32]			T.query = R.query; */
						absDest->query = relSource->query;
	/* [11/32]		else */
					} else {
	/* [28/32]			T.authority = Base.authority; */
						if (!URI_FUNC(CopyAuthority)(absDest, absBase)) {
							return URI_ERROR_MALLOC;
						}
	/* [12/32]			if (R.path == "") then */
						if (relSource->pathHead == NULL) {
	/* [13/32]				T.path = Base.path; */
							if (!URI_FUNC(CopyPath)(absDest, absBase)) {
								return URI_ERROR_MALLOC;
							}
	/* [14/32]				if defined(R.query) then */
							if (relSource->query.first != NULL) {
	/* [15/32]					T.query = R.query; */
								absDest->query = relSource->query;
	/* [16/32]				else */
							} else {
	/* [17/32]					T.query = Base.query; */
								absDest->query = absBase->query;
	/* [18/32]				endif; */
							}
	/* [19/32]			else */
						} else {
	/* [20/32]				if (R.path starts-with "/") then */
							if (relSource->absolutePath) {
	/* [21/32]					T.path = remove_dot_segments(R.path); */
								if (!URI_FUNC(CopyPath)(absDest, relSource)) {
									return URI_ERROR_MALLOC;
								}
								if (!URI_FUNC(RemoveDotSegmentsAbsolute)(absDest)) {
									return URI_ERROR_MALLOC;
								}
	/* [22/32]				else */
							} else {
	/* [23/32]					T.path = merge(Base.path, R.path); */
								if (!URI_FUNC(CopyPath)(absDest, absBase)) {
									return URI_ERROR_MALLOC;
								}
								if (!URI_FUNC(MergePath)(absDest, relSource)) {
									return URI_ERROR_MALLOC;
								}
	/* [24/32]					T.path = remove_dot_segments(T.path); */
								if (!URI_FUNC(RemoveDotSegmentsAbsolute)(absDest)) {
									return URI_ERROR_MALLOC;
								}

								if (!URI_FUNC(FixAmbiguity)(absDest)) {
									return URI_ERROR_MALLOC;
								}
	/* [25/32]				endif; */
							}
	/* [26/32]				T.query = R.query; */
							absDest->query = relSource->query;
	/* [27/32]			endif; */
						}
						URI_FUNC(FixEmptyTrailSegment)(absDest);
	/* [29/32]		endif; */
					}
	/* [30/32]		T.scheme = Base.scheme; */
					absDest->scheme = absBase->scheme;
	/* [31/32]	endif; */
				}
	/* [32/32]	T.fragment = R.fragment; */
				absDest->fragment = relSource->fragment;

	return URI_SUCCESS;

}



int URI_FUNC(AddBaseUri)(URI_TYPE(Uri) * absDest,
		const URI_TYPE(Uri) * relSource, const URI_TYPE(Uri) * absBase) {
	const int res = URI_FUNC(AddBaseUriImpl)(absDest, relSource, absBase);
	if ((res != URI_SUCCESS) && (absDest != NULL)) {
		URI_FUNC(FreeUriMembers)(absDest);
	}
	return res;
}



#endif
