// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/logging.h"

#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include "leveldb/env.h"
#include "leveldb/slice.h"

namespace leveldb {

void AppendNumberTo(std::string* str, uint64_t num) { // NOTE: htt, 添加数值
  char buf[30];
  snprintf(buf, sizeof(buf), "%llu", (unsigned long long) num);
  str->append(buf);
}

void AppendEscapedStringTo(std::string* str, const Slice& value) { // NOTE: htt, 添加字符串，并转义不可打印字符串
  for (size_t i = 0; i < value.size(); i++) {
    char c = value[i];
    if (c >= ' ' && c <= '~') {
      str->push_back(c);
    } else {
      char buf[10];
      snprintf(buf, sizeof(buf), "\\x%02x",
               static_cast<unsigned int>(c) & 0xff);
      str->append(buf);
    }
  }
}

std::string NumberToString(uint64_t num) { // NOTE: htt, 数值的字符串形式
  std::string r;
  AppendNumberTo(&r, num);
  return r;
}

std::string EscapeString(const Slice& value) { // NOTE: htt, 转义不可打印字符串
  std::string r;
  AppendEscapedStringTo(&r, value);
  return r;
}

bool ConsumeChar(Slice* in, char c) { // NOTE: htt, 如果in字符串以c开始，则移除字符
  if (!in->empty() && (*in)[0] == c) {
    in->remove_prefix(1); // NOTE: htt, 移除第一个字符串
    return true;
  } else {
    return false;
  }
}

bool ConsumeDecimalNumber(Slice* in, uint64_t* val) { // NOTE:htt, 将可读整数放入到val，并移动字符串中相应游标跳过整数
  uint64_t v = 0;
  int digits = 0;
  while (!in->empty()) {
    char c = (*in)[0];
    if (c >= '0' && c <= '9') { // NOTE: htt, 整数需要其他字符串来终结
      ++digits;
      const int delta = (c - '0');
      static const uint64_t kMaxUint64 = ~static_cast<uint64_t>(0); // NOTE: htt, 2^64-1的一种方式
      if (v > kMaxUint64/10 ||
          (v == kMaxUint64/10 && delta > kMaxUint64%10)) {
        // Overflow
        return false;
      }
      v = (v * 10) + delta;
      in->remove_prefix(1); // NOTE: htt, 移除第一个字符
    } else { // NOTE:htt, 非数字则终结
      break;
    }
  }
  *val = v;
  return (digits > 0);
}

}  // namespace leveldb
