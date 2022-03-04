// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "port/port_posix.h"

#include <cstdlib>
#include <stdio.h>
#include <string.h>
#include "util/logging.h"

namespace leveldb {
namespace port {

static void PthreadCall(const char* label, int result) { // NOTE: htt, 线程调用，不满足条件则退出
  if (result != 0) { // NOTE: htt, result不为0值，打印并退出
    fprintf(stderr, "pthread %s: %s\n", label, strerror(result));
    abort();
  }
}

Mutex::Mutex() { PthreadCall("init mutex", pthread_mutex_init(&mu_, NULL)); } // NOTE: htt, 初始化锁

Mutex::~Mutex() { PthreadCall("destroy mutex", pthread_mutex_destroy(&mu_)); } // NOTE: htt, 销毁锁

void Mutex::Lock() { PthreadCall("lock", pthread_mutex_lock(&mu_)); } // NOTE: htt, 加锁

void Mutex::Unlock() { PthreadCall("unlock", pthread_mutex_unlock(&mu_)); } // NOTE: htt, 放锁

CondVar::CondVar(Mutex* mu)
    : mu_(mu) {
    PthreadCall("init cv", pthread_cond_init(&cv_, NULL)); // NOTE: ht, 初始化信号量
}

CondVar::~CondVar() { PthreadCall("destroy cv", pthread_cond_destroy(&cv_)); } // NOTE: htt, 销毁信号量

void CondVar::Wait() {
  PthreadCall("wait", pthread_cond_wait(&cv_, &mu_->mu_)); // NOTE: htt, 信号量等待
}

void CondVar::Signal() {
  PthreadCall("signal", pthread_cond_signal(&cv_)); // NOTE: htt,  通知信号量等待,至少激活一个等待线程
}

void CondVar::SignalAll() {
  PthreadCall("broadcast", pthread_cond_broadcast(&cv_)); // NOTE: htt, 激活所有在当前条件阻塞的线程
}

void InitOnce(OnceType* once, void (*initializer)()) {
  PthreadCall("once", pthread_once(once, initializer));
}

}  // namespace port
}  // namespace leveldb
