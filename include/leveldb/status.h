// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// A Status encapsulates the result of an operation.  It may indicate success,
// or it may indicate an error with an associated error message.
//
// Multiple threads can invoke const methods on a Status without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same Status must use
// external synchronization.

#ifndef STORAGE_LEVELDB_INCLUDE_STATUS_H_
#define STORAGE_LEVELDB_INCLUDE_STATUS_H_

#include <string>
#include "leveldb/slice.h"

namespace leveldb {

class Status { // NOTE: htt, 状态码，包括生成状态码，判断状态码
 public:
  // Create a success status.
  Status() : state_(NULL) { }
  ~Status() { delete[] state_; } // NOTE: htt, 析构删除state_

  // Copy the specified status.
  Status(const Status& s);
  void operator=(const Status& s);

  // Return a success status.
  static Status OK() { return Status(); }

  // Return error status of an appropriate type.
  static Status NotFound(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kNotFound, msg, msg2); // NOTE: htt, kNotFound的异常状态信息
  }
  static Status Corruption(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kCorruption, msg, msg2); // NOTE: htt, kCorruption异常状态信息
  }
  static Status NotSupported(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kNotSupported, msg, msg2); // NOTE: htt, kNotSupported异常状态消息
  }
  static Status InvalidArgument(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kInvalidArgument, msg, msg2); // NOTE: htt, kInvalidArgument异常状态消息
  }
  static Status IOError(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kIOError, msg, msg2); // NOTE: htt, kIOError异常状态消息
  }

  // Returns true iff the status indicates success.
  bool ok() const { return (state_ == NULL); } // NOTE: htt, 判断是否为 ok

  // Returns true iff the status indicates a NotFound error.
  bool IsNotFound() const { return code() == kNotFound; } // NOTE: htt, 判断是否为 kNotFound

  // Returns true iff the status indicates a Corruption error.
  bool IsCorruption() const { return code() == kCorruption; } // NOTE: htt, 判断是否为 kCorruption

  // Returns true iff the status indicates an IOError.
  bool IsIOError() const { return code() == kIOError; } // NOTE: htt, 判断是否为 kIOError

  // Return a string representation of this status suitable for printing.
  // Returns the string "OK" for success.
  std::string ToString() const;

 private:
  // OK status has a NULL state_.  Otherwise, state_ is a new[] array
  // of the following form:
  //    state_[0..3] == length of message
  //    state_[4]    == code
  //    state_[5..]  == message
  const char* state_; // NOTE: htt, 如果为kOK则state_为NULL; 否则为数组 state_[0..3]为异常消息长度,state_[4]为code, state_[5]为异常消息

  enum Code {
    kOk = 0, // NOTE: htt, kOk 即为成功，此时 state_ 为 NULL
    kNotFound = 1,
    kCorruption = 2,
    kNotSupported = 3,
    kInvalidArgument = 4,
    kIOError = 5
  };

  Code code() const { // NOTE: htt, 解析当前返回码的状态值，如果state_不为NULL，即取 state_[4]
    return (state_ == NULL) ? kOk : static_cast<Code>(state_[4]);
  }

  Status(Code code, const Slice& msg, const Slice& msg2);
  static const char* CopyState(const char* s);
};

inline Status::Status(const Status& s) {
  state_ = (s.state_ == NULL) ? NULL : CopyState(s.state_);
}
inline void Status::operator=(const Status& s) {
  // The following condition catches both aliasing (when this == &s),
  // and the common case where both s and *this are ok.
  if (state_ != s.state_) { // NOTE: htt, state_不相同才进行重新赋值
    delete[] state_;
    state_ = (s.state_ == NULL) ? NULL : CopyState(s.state_);
  }
}

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_STATUS_H_
