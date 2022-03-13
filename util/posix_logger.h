// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Logger implementation that can be shared by all environments
// where enough posix functionality is available.

#ifndef STORAGE_LEVELDB_UTIL_POSIX_LOGGER_H_
#define STORAGE_LEVELDB_UTIL_POSIX_LOGGER_H_

#include <algorithm>
#include <stdio.h>
#include <sys/time.h>
#include <time.h>
#include "leveldb/env.h"

namespace leveldb {

class PosixLogger : public Logger { // NOTE: htt, 将日志信息打印到文件中
 private:
  FILE* file_;
  uint64_t (*gettid_)();  // Return the thread id for the current thread // NOTE: htt, 获取当前线程信息
 public:
  PosixLogger(FILE* f, uint64_t (*gettid)()) : file_(f), gettid_(gettid) { }
  virtual ~PosixLogger() {
    fclose(file_); // TODO: htt, 析构才释放，文件存在过大的风险
  }
  virtual void Logv(const char* format, va_list ap) { // NOTE: htt, 打印日志(如果日志信息小,则采用栈内存;如果大,则分配一次堆内存)
    const uint64_t thread_id = (*gettid_)();

    // We try twice: the first time with a fixed-size stack allocated buffer,
    // and the second time with a much larger dynamically allocated buffer.
    char buffer[500];
    for (int iter = 0; iter < 2; iter++) {
      char* base;
      int bufsize;
      if (iter == 0) { // NOTE: htt, 第一次使用默认 500栈内存
        bufsize = sizeof(buffer);
        base = buffer;
      } else { // NOTE: htt, 第二次则分配30K的堆栈内存
        bufsize = 30000;
        base = new char[bufsize];
      }
      char* p = base;
      char* limit = base + bufsize;

      struct timeval now_tv;
      gettimeofday(&now_tv, NULL);
      const time_t seconds = now_tv.tv_sec;
      struct tm t;
      localtime_r(&seconds, &t);
      p += snprintf(p, limit - p,
                    "%04d/%02d/%02d-%02d:%02d:%02d.%06d %llx ",
                    t.tm_year + 1900,
                    t.tm_mon + 1,
                    t.tm_mday,
                    t.tm_hour,
                    t.tm_min,
                    t.tm_sec,
                    static_cast<int>(now_tv.tv_usec),
                    static_cast<long long unsigned int>(thread_id));

      // Print the message
      if (p < limit) {
        va_list backup_ap;
        va_copy(backup_ap, ap);
        p += vsnprintf(p, limit - p, format, backup_ap);
        va_end(backup_ap);
      }

      // Truncate to available space if necessary
      if (p >= limit) { // NOTE: htt, 日志写满则尝试第二次分配
        if (iter == 0) {
          continue;       // Try again with larger buffer
        } else { // NOTE: htt, 如果已经分配了1次，则不再重新分配空间，截断打印内容
          p = limit - 1;
        }
      }

      // Add newline if necessary // NOTE: htt, 判断包括:1.等于base,则设置; 2.不等于base,判断前一个字符,不为\n,则设置
      if (p == base || p[-1] != '\n') {
        *p++ = '\n'; // NOTE: htt, 设置\n(打印显示换行)
      }

      assert(p <= limit);
      fwrite(base, 1, p - base, file_); // NOTE: htt, 将日志打印的文件中
      fflush(file_); // NOTE: htt, 刷新日志
      if (base != buffer) {
        delete[] base; // NOTE: htt, 如果重新分配内存，则进行释放
      }
      break;
    }
  }
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_UTIL_POSIX_LOGGER_H_
