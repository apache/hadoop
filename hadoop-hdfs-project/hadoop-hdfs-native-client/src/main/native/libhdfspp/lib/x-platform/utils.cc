#include "x-platform/utils.h"

#include <filesystem>
#include <string>
#include <vector>

std::string XPlatform::Utils::Basename(const std::string& file_path) {
  if (file_path.empty()) {
    return ".";
  }

  const std::filesystem::path path(file_path);
  std::vector<std::string> parts;
  for (const auto& part : std::filesystem::path(file_path)) {
    parts.emplace_back(part.string());
  }

  /*Handle the case of trailing slash*/
  if (parts.back().empty()) {
    parts.pop_back();
  }
  return parts.back();
}
