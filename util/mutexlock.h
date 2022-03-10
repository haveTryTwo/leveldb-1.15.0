// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_UTIL_MUTEXLOCK_H_
#define STORAGE_LEVELDB_UTIL_MUTEXLOCK_H_

#include "port/port.h"
#include "port/thread_annotations.h"

namespace leveldb {

// Helper class that locks a mutex on construction and unlocks the mutex when
// the destructor of the MutexLock object is invoked.
//
// Typical usage:
//
//   void MyClass::MyMethod() {
//     MutexLock l(&mu_);       // mu_ is an instance variable
//     ... some complex code, possibly with multiple return paths ...
//   }

class SCOPED_LOCKABLE MutexLock { // NOTE: htt, 采用对象方式管理锁，减少锁释放失败可能
 public:
  explicit MutexLock(port::Mutex *mu) EXCLUSIVE_LOCK_FUNCTION(mu)
      : mu_(mu)  {
    this->mu_->Lock(); // NOTE: htt, 构造时，则加锁
  }
  ~MutexLock() UNLOCK_FUNCTION() { this->mu_->Unlock(); } // NOTE: htt, 析构时则放锁

 private:
  port::Mutex *const mu_; // NOTE: htt, 待处理的锁
  // No copying allowed
  MutexLock(const MutexLock&);
  void operator=(const MutexLock&);
};

}  // namespace leveldb


#endif  // STORAGE_LEVELDB_UTIL_MUTEXLOCK_H_
