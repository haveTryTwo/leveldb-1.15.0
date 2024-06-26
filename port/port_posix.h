// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// See port_example.h for documentation for the following types/functions.

#ifndef STORAGE_LEVELDB_PORT_PORT_POSIX_H_
#define STORAGE_LEVELDB_PORT_PORT_POSIX_H_

#undef PLATFORM_IS_LITTLE_ENDIAN
#if defined(OS_MACOSX)
#include <machine/endian.h>
#if defined(__DARWIN_LITTLE_ENDIAN) && defined(__DARWIN_BYTE_ORDER)
#define PLATFORM_IS_LITTLE_ENDIAN (__DARWIN_BYTE_ORDER == __DARWIN_LITTLE_ENDIAN)
#endif
#elif defined(OS_SOLARIS)
#include <sys/isa_defs.h>
#ifdef _LITTLE_ENDIAN
#define PLATFORM_IS_LITTLE_ENDIAN true
#else
#define PLATFORM_IS_LITTLE_ENDIAN false
#endif
#elif defined(OS_FREEBSD)
#include <sys/endian.h>
#include <sys/types.h>
#define PLATFORM_IS_LITTLE_ENDIAN (_BYTE_ORDER == _LITTLE_ENDIAN)
#elif defined(OS_OPENBSD) || defined(OS_NETBSD) || defined(OS_DRAGONFLYBSD)
#include <sys/endian.h>
#include <sys/types.h>
#elif defined(OS_HPUX)
#define PLATFORM_IS_LITTLE_ENDIAN false
#elif defined(OS_ANDROID)
// Due to a bug in the NDK x86 <sys/endian.h> definition,
// _BYTE_ORDER must be used instead of __BYTE_ORDER on Android.
// See http://code.google.com/p/android/issues/detail?id=39824
#include <endian.h>
#define PLATFORM_IS_LITTLE_ENDIAN (_BYTE_ORDER == _LITTLE_ENDIAN)
#else
#include <endian.h>
#endif

#include <pthread.h>
#ifdef SNAPPY
#include <snappy.h>
#endif
#ifdef ZSTD
#include <zstd.h>
#endif

#include <stdint.h>
#include <string>
#include "port/atomic_pointer.h"

#ifndef PLATFORM_IS_LITTLE_ENDIAN
#define PLATFORM_IS_LITTLE_ENDIAN (__BYTE_ORDER == __LITTLE_ENDIAN)
#endif

#if defined(OS_MACOSX) || defined(OS_SOLARIS) || defined(OS_FREEBSD) || defined(OS_NETBSD) || defined(OS_OPENBSD) || \
    defined(OS_DRAGONFLYBSD) || defined(OS_ANDROID) || defined(OS_HPUX)
// Use fread/fwrite/fflush on platforms without _unlocked variants
#define fread_unlocked fread
#define fwrite_unlocked fwrite
#define fflush_unlocked fflush
#endif

#if defined(OS_MACOSX) || defined(OS_FREEBSD) || defined(OS_OPENBSD) || defined(OS_DRAGONFLYBSD)
// Use fsync() on platforms without fdatasync()
#define fdatasync fsync
#endif

#if defined(OS_ANDROID) && __ANDROID_API__ < 9
// fdatasync() was only introduced in API level 9 on Android. Use fsync()
// when targetting older platforms.
#define fdatasync fsync
#endif

namespace leveldb {
namespace port {

static const bool kLittleEndian = PLATFORM_IS_LITTLE_ENDIAN;  // NOTE: htt, CPU是否为小端模式，支持不用机型判断
#undef PLATFORM_IS_LITTLE_ENDIAN

class CondVar;

class Mutex {  // NOTE: htt, 互斥锁
 public:
  Mutex();
  ~Mutex();

  void Lock();
  void Unlock();
  void AssertHeld() {}

 private:
  friend class CondVar;  // NOTE:htt, friend类
  pthread_mutex_t mu_;

  // No copying
  Mutex(const Mutex&);
  void operator=(const Mutex&);
};

class CondVar {  // NOTE: htt, 条件变量
 public:
  explicit CondVar(Mutex* mu);
  ~CondVar();
  void Wait();
  void Signal();
  void SignalAll();

 private:
  pthread_cond_t cv_;
  Mutex* mu_;
};

typedef pthread_once_t OnceType;
#define LEVELDB_ONCE_INIT PTHREAD_ONCE_INIT
extern void InitOnce(OnceType* once, void (*initializer)());

inline bool Snappy_Compress(const char* input, size_t length,
                            ::std::string* output) {  // NOTE: htt, snappy压缩{{{
#ifdef SNAPPY
  output->resize(snappy::MaxCompressedLength(length));
  size_t outlen;
  snappy::RawCompress(input, length, &(*output)[0], &outlen);
  output->resize(outlen);
#ifdef _TEST_
  fprintf(stderr, "has snappy!\n");
#endif
  return true;
#endif

#ifdef _TEST_
  fprintf(stderr, "no snappy!\n");
#endif
  return false;
} /*}}}*/

inline bool Snappy_GetUncompressedLength(const char* input, size_t length,
                                         size_t* result) {  // NOTE: htt, 获取snappy解压缩后的长度{{{
#ifdef SNAPPY
  return snappy::GetUncompressedLength(input, length, result);
#else
  return false;
#endif
} /*}}}*/

inline bool Snappy_Uncompress(const char* input, size_t length,
                              char* output) {  // NOTE: htt, snappy解压缩{{{
#ifdef SNAPPY
  return snappy::RawUncompress(input, length, output);
#else
  return false;
#endif
} /*}}}*/

inline bool Zstd_Compress(const char* input, size_t length,
                          ::std::string* output) {  // NOTE: htt, zstd压缩{{{
#ifdef ZSTD
  size_t compress_len = ZSTD_compressBound(length);
  if (ZSTD_isError(compress_len) != 0) return false;

  output->resize(compress_len);
  size_t ret_compress_len = ZSTD_compress(&(*output)[0], compress_len, input, length, 0);
  if (ZSTD_isError(ret_compress_len) != 0) return false;
  output->resize(ret_compress_len);
  return true;
#endif

  return false;
} /*}}}*/

inline bool Zstd_GetUncompressedLength(const char* input, size_t length,
                                       size_t* result) {  // NOTE: htt, 获取ztsd解压缩后的长度{{{
#ifdef ZSTD
  unsigned long long const uncompress_len = ZSTD_getFrameContentSize(input, length);
  if (uncompress_len == ZSTD_CONTENTSIZE_ERROR || uncompress_len == ZSTD_CONTENTSIZE_UNKNOWN) {
    return false;
  }
  *result = uncompress_len;
  return true;
#else
  return false;
#endif
} /*}}}*/

inline bool Zstd_Uncompress(const char* input, size_t length, char* output,
                            size_t* output_len) {  // NOTE: htt, 解压缩{{{
#ifdef ZSTD
  size_t ret_uncompress_len = ZSTD_decompress(output, *output_len, input, length);
  if (ZSTD_isError(ret_uncompress_len) != 0) {
    return false;
  }
  *output_len = ret_uncompress_len;  // NOTE:htt, 返回实际的解压后的大小
  return true;
#else
  return false;
#endif
} /*}}}*/

inline bool GetHeapProfile(void (*func)(void*, const char*, int), void* arg) { return false; }

}  // namespace port
}  // namespace leveldb

#endif  // STORAGE_LEVELDB_PORT_PORT_POSIX_H_
