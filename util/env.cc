// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/env.h"

namespace leveldb {

Env::~Env() {
}

SequentialFile::~SequentialFile() {
}

RandomAccessFile::~RandomAccessFile() {
}

WritableFile::~WritableFile() {
}

Logger::~Logger() {
}

FileLock::~FileLock() {
}

void Log(Logger* info_log, const char* format, ...) { // NOTE: htt, 打印日志
  if (info_log != NULL) {
    va_list ap;
    va_start(ap, format);
    info_log->Logv(format, ap); // NOTE: htt, 打印日志
    va_end(ap);
  }
}

static Status DoWriteStringToFile(Env* env, const Slice& data,
                                  const std::string& fname,
                                  bool should_sync) { // NOTE:htt, 将data写入 fname新文件中,并根据sync来判断是否同步数据
  WritableFile* file;
  Status s = env->NewWritableFile(fname, &file);
  if (!s.ok()) {
    return s;
  }
  s = file->Append(data);
  if (s.ok() && should_sync) {
    s = file->Sync(); // NOTE: htt, 如果刷盘则Sync()
  }
  if (s.ok()) {
    s = file->Close(); // NOTE: htt, close()文件会带来刷盘操作?
  }
  delete file;  // Will auto-close if we did not close above
  if (!s.ok()) {
    env->DeleteFile(fname); // NOTE: htt, 出现失败则删除文件 TODO: htt, 是否考虑文件已有数据等情况
  }
  return s;
}

Status WriteStringToFile(Env* env, const Slice& data,
                         const std::string& fname) {
  return DoWriteStringToFile(env, data, fname, false);
}

Status WriteStringToFileSync(Env* env, const Slice& data,
                             const std::string& fname) {
  return DoWriteStringToFile(env, data, fname, true); // NOTE: htt, 待刷盘写
}

Status ReadFileToString(Env* env, const std::string& fname, std::string* data) { // NOTE: htt, 读取文件内容到data中
  data->clear();
  SequentialFile* file;
  Status s = env->NewSequentialFile(fname, &file);
  if (!s.ok()) {
    return s;
  }
  static const int kBufferSize = 8192;
  char* space = new char[kBufferSize];
  while (true) {
    Slice fragment;
    s = file->Read(kBufferSize, &fragment, space);
    if (!s.ok()) {
      break;
    }
    data->append(fragment.data(), fragment.size());
    if (fragment.empty()) { // NOTE: htt, 读取完成
      break;
    }
  }
  delete[] space;
  delete file;
  return s;
}

EnvWrapper::~EnvWrapper() {
}

}  // namespace leveldb
