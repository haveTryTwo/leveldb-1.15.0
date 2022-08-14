// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "helpers/memenv/memenv.h"

#include "leveldb/env.h"
#include "leveldb/status.h"
#include "port/port.h"
#include "util/mutexlock.h"
#include <map>
#include <string.h>
#include <string>
#include <vector>

namespace leveldb {

namespace {

class FileState { // NOTE: htt, 内存数组，用于写入和读取数据
 public:
  // FileStates are reference counted. The initial reference count is zero
  // and the caller must call Ref() at least once.
  FileState() : refs_(0), size_(0) {}

  // Increase the reference count.
  void Ref() { // NOTE: htt, 增加当前对象引用
    MutexLock lock(&refs_mutex_);
    ++refs_; // NOTE: htt, 增加引用个数
  }

  // Decrease the reference count. Delete if this is the last reference.
  void Unref() { // NOTE: htt, 减少引用; 如果当前没有引用，则删除当前对象
    bool do_delete = false;

    {
      MutexLock lock(&refs_mutex_);
      --refs_;
      assert(refs_ >= 0);
      if (refs_ <= 0) {
        do_delete = true;
      }
    }

    if (do_delete) {
      delete this;
    }
  }

  uint64_t Size() const { return size_; }
  // NOTE: htt, 从blocks_读取 [offset, offset+n]的数据
  Status Read(uint64_t offset, size_t n, Slice* result, char* scratch) const {
    if (offset > size_) {
      return Status::IOError("Offset greater than file size.");
    }
    const uint64_t available = size_ - offset;
    if (n > available) {
      n = available;
    }
    if (n == 0) {
      *result = Slice();
      return Status::OK();
    }

    size_t block = offset / kBlockSize;
    size_t block_offset = offset % kBlockSize;

    if (n <= kBlockSize - block_offset) { // NOTE: htt, 如果数据直接在blocks_一个单元内，直接返回
      // The requested bytes are all in the first block.
      *result = Slice(blocks_[block] + block_offset, n); // NOTE: htt, 引用具体对象
      return Status::OK();
    }
    // NOTE: htt, 如果复制长度超过一个 blocks_单元，则复制到scratch，并返回scratch的引用
    size_t bytes_to_copy = n;
    char* dst = scratch;

    while (bytes_to_copy > 0) {
      size_t avail = kBlockSize - block_offset;
      if (avail > bytes_to_copy) {
        avail = bytes_to_copy;
      }
      memcpy(dst, blocks_[block] + block_offset, avail);

      bytes_to_copy -= avail;
      dst += avail;
      block++;
      block_offset = 0;
    }

    *result = Slice(scratch, n);
    return Status::OK();
  }
  // NOTE: htt, 添加数据，如果超过一个单元，则创建的新block，并继续复制数据
  Status Append(const Slice& data) {
    const char* src = data.data();
    size_t src_len = data.size();

    while (src_len > 0) {
      size_t avail;
      size_t offset = size_ % kBlockSize;

      if (offset != 0) {
        // There is some room in the last block.
        avail = kBlockSize - offset;
      } else {
        // No room in the last block; push new one.
        blocks_.push_back(new char[kBlockSize]); // NOTE: htt, 创建新的空间
        avail = kBlockSize;
      }

      if (avail > src_len) {
        avail = src_len;
      }
      memcpy(blocks_.back() + offset, src, avail);
      src_len -= avail;
      src += avail;
      size_ += avail;
    }

    return Status::OK();
  }

 private:
  // Private since only Unref() should be used to delete it.
  ~FileState() {
    for (std::vector<char*>::iterator i = blocks_.begin(); i != blocks_.end();
         ++i) {
      delete [] *i;
    }
  }

  // No copying allowed.
  FileState(const FileState&);
  void operator=(const FileState&);

  port::Mutex refs_mutex_; // NOTE: htt, 锁
  int refs_;  // Protected by refs_mutex_;

  // The following fields are not protected by any mutex. They are only mutable
  // while the file is being written, and concurrent access is not allowed
  // to writable files.
  std::vector<char*> blocks_; // NOTE: htt, 保存数据的内存数组
  uint64_t size_; // NOTE: htt, 内存blocks_有数据长度和

  enum { kBlockSize = 8 * 1024 };
};

class SequentialFileImpl : public SequentialFile { // NOTE: htt, 从内存数组中顺序读取数据
 public:
  explicit SequentialFileImpl(FileState* file) : file_(file), pos_(0) {
    file_->Ref(); // NOTE: htt, 增加当前对象引用
  }

  ~SequentialFileImpl() {
    file_->Unref(); // NOTE: htt, 减少引用; 如果当前没有引用，则删除当前对象
  }
  
  virtual Status Read(size_t n, Slice* result, char* scratch) { // NOTE: htt, 读取数据
    Status s = file_->Read(pos_, n, result, scratch);
    if (s.ok()) {
      pos_ += result->size();
    }
    return s;
  }

  virtual Status Skip(uint64_t n) { // NOTE: htt, 内存数组跳过n个字符
    if (pos_ > file_->Size()) {
      return Status::IOError("pos_ > file_->Size()");
    }
    const size_t available = file_->Size() - pos_;
    if (n > available) {
      n = available;
    }
    pos_ += n;
    return Status::OK();
  }

 private:
  FileState* file_;
  size_t pos_; // NOTE: htt, file_内存数组读取位置
};

class RandomAccessFileImpl : public RandomAccessFile { // NOTE: htt, 从内存数组中随机读取数据
 public:
  explicit RandomAccessFileImpl(FileState* file) : file_(file) {
    file_->Ref(); // NOTE: htt, 增加引用
  }

  ~RandomAccessFileImpl() {
    file_->Unref(); // NOTE: htt, 减少引用，如果为0则删除对象
  }

  virtual Status Read(uint64_t offset, size_t n, Slice* result,
                      char* scratch) const {
    return file_->Read(offset, n, result, scratch); // NOTE: htt, 随机读取数据
  }

 private:
  FileState* file_; // NOTE: htt, 内存数组对象
};

class WritableFileImpl : public WritableFile { // NOTE: htt, 通过内存数组写入数据
 public:
  WritableFileImpl(FileState* file) : file_(file) {
    file_->Ref(); // NOTE: htt, 增加引用
  }

  ~WritableFileImpl() {
    file_->Unref(); // NOTE: htt, 减少引用，如果为0则删除对象
  }

  virtual Status Append(const Slice& data) { // NOTE: htt, 添加数据
    return file_->Append(data);
  }

  virtual Status Close() { return Status::OK(); }
  virtual Status Flush() { return Status::OK(); }
  virtual Status Sync() { return Status::OK(); }

 private:
  FileState* file_; // NOTE: htt, 内存数据对象
};

class NoOpLogger : public Logger { // NOTE: htt, 空日志操作
 public:
  virtual void Logv(const char* format, va_list ap) { }
};

class InMemoryEnv : public EnvWrapper {// NOTE: htt, 模拟内存环境，用于创建 顺序/随机/读写文件, 创建加锁文件,日志文件;
 public:
  explicit InMemoryEnv(Env* base_env) : EnvWrapper(base_env) { }

  virtual ~InMemoryEnv() {
    for (FileSystem::iterator i = file_map_.begin(); i != file_map_.end(); ++i){
      i->second->Unref(); // NOTE: htt, 内存数组逐一减少引用，如果为0则删除
    }
  }

  // Partial implementation of the Env interface.
  virtual Status NewSequentialFile(const std::string& fname,
                                   SequentialFile** result) { // NOTE: htt, 创建顺序读取文件对象
    MutexLock lock(&mutex_);
    if (file_map_.find(fname) == file_map_.end()) {
      *result = NULL;
      return Status::IOError(fname, "File not found");
    }

    *result = new SequentialFileImpl(file_map_[fname]);
    return Status::OK();
  }

  virtual Status NewRandomAccessFile(const std::string& fname,
                                     RandomAccessFile** result) { // NOTE: htt, 创建随机读取文件对象
    MutexLock lock(&mutex_);
    if (file_map_.find(fname) == file_map_.end()) { // NOTE: htt, 读取文件必须存在
      *result = NULL;
      return Status::IOError(fname, "File not found");
    }

    *result = new RandomAccessFileImpl(file_map_[fname]);
    return Status::OK();
  }

  virtual Status NewWritableFile(const std::string& fname,
                                 WritableFile** result) { // NOTE: htt, 创建写入文件对象
    MutexLock lock(&mutex_);
    if (file_map_.find(fname) != file_map_.end()) { // NOTE: htt, 写入文件如果存在则删除
      DeleteFileInternal(fname);
    }

    FileState* file = new FileState();
    file->Ref();
    file_map_[fname] = file;

    *result = new WritableFileImpl(file);
    return Status::OK();
  }

  virtual bool FileExists(const std::string& fname) { // NOTE: htt, 判断文件是否存在
    MutexLock lock(&mutex_);
    return file_map_.find(fname) != file_map_.end();
  }

  virtual Status GetChildren(const std::string& dir,
                             std::vector<std::string>* result) { // NOTE: htt, 获取目录下的文件
    MutexLock lock(&mutex_);
    result->clear();

    for (FileSystem::iterator i = file_map_.begin(); i != file_map_.end(); ++i){
      const std::string& filename = i->first;

      if (filename.size() >= dir.size() + 1 && filename[dir.size()] == '/' &&
          Slice(filename).starts_with(Slice(dir))) {
        result->push_back(filename.substr(dir.size() + 1));
      }
    }

    return Status::OK();
  }

  void DeleteFileInternal(const std::string& fname) { // NOTE: htt, 从file_map_移除文件名
    if (file_map_.find(fname) == file_map_.end()) {
      return;
    }

    file_map_[fname]->Unref(); // NOTE: htt, 减少对象引用，如果为0则删除
    file_map_.erase(fname); // NOTE: htt, file_map_中移除文件名
  }

  virtual Status DeleteFile(const std::string& fname) { // NOTE: htt, 加锁后如果文件名存在则移除
    MutexLock lock(&mutex_);
    if (file_map_.find(fname) == file_map_.end()) {
      return Status::IOError(fname, "File not found");
    }

    DeleteFileInternal(fname);
    return Status::OK();
  }

  virtual Status CreateDir(const std::string& dirname) { // NOTE: htt, 内存数组无需创建目录
    return Status::OK();
  }

  virtual Status DeleteDir(const std::string& dirname) {
    return Status::OK();
  }

  virtual Status GetFileSize(const std::string& fname, uint64_t* file_size) { // NOTE: htt, 获取文件大小
    MutexLock lock(&mutex_);
    if (file_map_.find(fname) == file_map_.end()) {
      return Status::IOError(fname, "File not found");
    }

    *file_size = file_map_[fname]->Size();
    return Status::OK();
  }

  virtual Status RenameFile(const std::string& src,
                            const std::string& target) { // NOTE: htt, 模拟复制文件
    MutexLock lock(&mutex_);
    if (file_map_.find(src) == file_map_.end()) {
      return Status::IOError(src, "File not found");
    }

    DeleteFileInternal(target);
    file_map_[target] = file_map_[src];
    file_map_.erase(src);
    return Status::OK();
  }

  virtual Status LockFile(const std::string& fname, FileLock** lock) { // NOTE: htt, 模拟锁
    *lock = new FileLock;
    return Status::OK();
  }

  virtual Status UnlockFile(FileLock* lock) {
    delete lock;
    return Status::OK();
  }

  virtual Status GetTestDirectory(std::string* path) {
    *path = "/test";
    return Status::OK();
  }

  virtual Status NewLogger(const std::string& fname, Logger** result) { // NOTE: htt, 模拟空日志
    *result = new NoOpLogger;
    return Status::OK();
  }

 private:
  // Map from filenames to FileState objects, representing a simple file system.
  typedef std::map<std::string, FileState*> FileSystem; // NOTE: htt, 文件名->文件内存数组
  port::Mutex mutex_;
  FileSystem file_map_;  // Protected by mutex_., 模拟文件系统
};

}  // namespace

Env* NewMemEnv(Env* base_env) {
  return new InMemoryEnv(base_env); // NOTE: htt, 创建内存文件环境对象，以便模拟读写文件
}

}  // namespace leveldb
