#ifndef NATIVE_LIBHDFSPP_LIB_CROSS_PLATFORM_UTILS
#define NATIVE_LIBHDFSPP_LIB_CROSS_PLATFORM_UTILS

#include <string>

/*
 * The XPlatform namespace contains components that
 * aid in writing cross-platform code.
 */
namespace XPlatform {
class Utils {
 public:
  /*
   * A cross-platform implementation of basename in linux.
   * Please refer https://www.man7.org/linux/man-pages/man3/basename.3.html
   * for more details.
   *
   * @param file_path The input path to get the basename.
   *
   * @returns The trailing component of the given {@link file_path}
   */
  static std::string Basename(const std::string& file_path);
};
}  // namespace XPlatform

#endif
