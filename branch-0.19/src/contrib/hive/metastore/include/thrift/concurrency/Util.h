// Copyright (c) 2006- Facebook
// Distributed under the Thrift Software License
//
// See accompanying file LICENSE or visit the Thrift site at:
// http://developers.facebook.com/thrift/

#ifndef _THRIFT_CONCURRENCY_UTIL_H_
#define _THRIFT_CONCURRENCY_UTIL_H_ 1

#include <config.h>

#include <assert.h>
#include <stddef.h>
#if defined(HAVE_CLOCK_GETTIME)
#include <time.h>
#else // defined(HAVE_CLOCK_GETTIME)
#include <sys/time.h>
#endif // defined(HAVE_CLOCK_GETTIME)

namespace facebook { namespace thrift { namespace concurrency { 

/**
 * Utility methods
 *
 * This class contains basic utility methods for converting time formats,
 * and other common platform-dependent concurrency operations.
 * It should not be included in API headers for other concurrency library
 * headers, since it will, by definition, pull in all sorts of horrid
 * platform dependent crap.  Rather it should be inluded directly in 
 * concurrency library implementation source.
 *
 * @author marc
 * @version $Id:$
 */
class Util {

  static const int64_t NS_PER_S = 1000000000LL;
  static const int64_t MS_PER_S = 1000LL;
  static const int64_t NS_PER_MS = 1000000LL;

 public:

  /**
   * Converts timespec to milliseconds
   *
   * @param struct timespec& result
   * @param time or duration in milliseconds
   */
  static void toTimespec(struct timespec& result, int64_t value) {
    result.tv_sec = value / MS_PER_S; // ms to s   
    result.tv_nsec = (value % MS_PER_S) * NS_PER_MS; // ms to ns
  }

  /**
   * Converts timespec to milliseconds
   */
  static const void toMilliseconds(int64_t& result, const struct timespec& value) {
    result =
      (value.tv_sec * MS_PER_S) +
      (value.tv_nsec / NS_PER_MS) +
      (value.tv_nsec % NS_PER_MS >= 500000 ? 1 : 0);
  }

  /**
   * Get current time as milliseconds from epoch
   */
  static const int64_t currentTime() {
#if defined(HAVE_CLOCK_GETTIME)
    struct timespec now;
    int ret = clock_gettime(CLOCK_REALTIME, &now);
    assert(ret == 0);
    return
      (now.tv_sec * MS_PER_S) +
      (now.tv_nsec / NS_PER_MS) +
      (now.tv_nsec % NS_PER_MS >= 500000 ? 1 : 0) ;
#elif defined(HAVE_GETTIMEOFDAY)
    struct timeval now;
    int ret = gettimeofday(&now, NULL);
    assert(ret == 0);
    return
      (((int64_t)now.tv_sec) * MS_PER_S) +
      (now.tv_usec / MS_PER_S) +
      (now.tv_usec % MS_PER_S >= 500 ? 1 : 0);
#endif // defined(HAVE_GETTIMEDAY)
  }

};

}}} // facebook::thrift::concurrency

#endif // #ifndef _THRIFT_CONCURRENCY_UTIL_H_
