// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/status.h"
#include <stdio.h>
#include "port/port.h"

namespace leveldb {

const char* Status::CopyState(const char* state) {  // NOTE: htt, 复制 state字符串到字符串数组中
  uint32_t size;
  memcpy(&size, state, sizeof(size));  // NOTE: htt, 获取头部4个长度
  char* result = new char[size + 5];
  memcpy(result, state, size + 5);
  return result;
}

Status::Status(Code code, const Slice& msg,
               const Slice& msg2) {  // NOTE: htt, 根据 code,msg,msg2 生成 state_ 内容
  assert(code != kOk);
  const uint32_t len1 = msg.size();
  const uint32_t len2 = msg2.size();
  const uint32_t size = len1 + (len2 ? (2 + len2) : 0);  // NOTE: htt, 如果 msg2不为空，则追加两个额外的字符 :
  char* result = new char[size + 5];
  memcpy(result, &size, sizeof(size));  // NOTE: htt, state_[0..3] 为size，长度
  result[4] = static_cast<char>(code);  // NOTE: htt, state_[4]为code
  memcpy(result + 5, msg.data(), len1);
  if (len2) {
    result[5 + len1] = ':';  // NOTE: htt, 追加两个字符串 : ，即 msg: msg2
    result[6 + len1] = ' ';
    memcpy(result + 7 + len1, msg2.data(), len2);
  }
  state_ = result;  // NOTE: htt, state_为new对象
}

std::string Status::ToString() const {  // NOTE: htt, 生成异常状态可读字符串
  if (state_ == NULL) {
    return "OK";
  } else {
    char tmp[30];
    const char* type;
    switch (code()) {
      case kOk:
        type = "OK";
        break;
      case kNotFound:
        type = "NotFound: ";
        break;
      case kCorruption:
        type = "Corruption: ";
        break;
      case kNotSupported:
        type = "Not implemented: ";
        break;
      case kInvalidArgument:
        type = "Invalid argument: ";
        break;
      case kIOError:
        type = "IO error: ";
        break;
      default:
        snprintf(tmp, sizeof(tmp), "Unknown code(%d): ", static_cast<int>(code()));
        type = tmp;
        break;
    }
    std::string result(type);
    uint32_t length;
    memcpy(&length, state_, sizeof(length));
    result.append(state_ + 5, length);
    return result;
  }
}

}  // namespace leveldb
