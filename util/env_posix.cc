// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <deque>
#include <set>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#if defined(LEVELDB_PLATFORM_ANDROID)
#include <sys/stat.h>
#endif
#include "leveldb/env.h"
#include "leveldb/slice.h"
#include "port/port.h"
#include "util/logging.h"
#include "util/mutexlock.h"
#include "util/posix_logger.h"

namespace leveldb {

namespace {

static Status IOError(const std::string& context, int err_number) {
  return Status::IOError(context, strerror(err_number)); // NOTE: htt, IOError
}

class PosixSequentialFile: public SequentialFile { // NOTE: htt, 顺序文件读取
 private:
  std::string filename_; // NOTE: htt, 文件名
  FILE* file_; // NOTE: htt, FILE*指针

 public:
  PosixSequentialFile(const std::string& fname, FILE* f)
      : filename_(fname), file_(f) { }
  virtual ~PosixSequentialFile() { fclose(file_); }

  virtual Status Read(size_t n, Slice* result, char* scratch) { // NOTE: htt, 读取文件
    Status s;
    size_t r = fread_unlocked(scratch, 1, n, file_); // NOTE: htt, 读取内容到 scratch
    *result = Slice(scratch, r); // NOTE: htt, 返回 Slice(), 方便调用者使用
    if (r < n) {
      if (feof(file_)) { // NOTE: htt, 文件读完
        // We leave status as ok if we hit the end of the file
      } else {
        // A partial read with an error: return a non-ok status
        s = IOError(filename_, errno); // NOTE: htt, 文件读取异常
      }
    }
    return s;
  }

  virtual Status Skip(uint64_t n) { // NOTE: htt, 跳过文件中n个字符
    if (fseek(file_, n, SEEK_CUR)) {
      return IOError(filename_, errno);
    }
    return Status::OK();
  }
};

// pread() based random-access
class PosixRandomAccessFile: public RandomAccessFile { // NOTE: htt, 随机读文件
 private:
  std::string filename_; // NOTE: htt, 文件名
  int fd_;

 public:
  PosixRandomAccessFile(const std::string& fname, int fd)
      : filename_(fname), fd_(fd) { }
  virtual ~PosixRandomAccessFile() { close(fd_); }

  virtual Status Read(uint64_t offset, size_t n, Slice* result,
                      char* scratch) const {
    Status s;
    ssize_t r = pread(fd_, scratch, n, static_cast<off_t>(offset)); // NOTE:htt, 从offset偏移位置开始读取
    *result = Slice(scratch, (r < 0) ? 0 : r);
    if (r < 0) {
      // An error: return a non-ok status
      s = IOError(filename_, errno); // NOTE: htt, 读取失败,返回异常
    }
    return s;
  }
};

// Helper class to limit mmap file usage so that we do not end up
// running out virtual memory or running into kernel performance
// problems for very large databases.
class MmapLimiter { // NOTE: htt, 保证mmaps使用个数,避免使用过多mmap
 public:
  // Up to 1000 mmaps for 64-bit binaries; none for smaller pointer sizes.
  MmapLimiter() {
    SetAllowed(sizeof(void*) >= 8 ? 1000 : 0); // NOTE: htt, 对于64bit 设置1000个mmaps
  }

  // If another mmap slot is available, acquire it and return true.
  // Else return false.
  bool Acquire() { // NOTE: htt, 尝试获取一个mmaps
    if (GetAllowed() <= 0) {
      return false;
    }
    MutexLock l(&mu_); // NOTE: htt, 加锁
    intptr_t x = GetAllowed();
    if (x <= 0) { // NOTE: htt, mmaps个数已使用完毕，则返回false
      return false;
    } else {
      SetAllowed(x - 1); // NOTE: htt, 减少mmaps空闲个数
      return true;
    }
  }

  // Release a slot acquired by a previous call to Acquire() that returned true.
  void Release() { // NOTE: htt, 增加mmaps个数
    MutexLock l(&mu_); // NOTE: htt, 加锁
    SetAllowed(GetAllowed() + 1);
  }

 private:
  port::Mutex mu_; // NOTE: htt, 锁
  port::AtomicPointer allowed_;

  intptr_t GetAllowed() const {
    return reinterpret_cast<intptr_t>(allowed_.Acquire_Load()); // NOTE: htt, 带内存栅栏读取
  }

  // REQUIRES: mu_ must be held
  void SetAllowed(intptr_t v) {
    allowed_.Release_Store(reinterpret_cast<void*>(v)); // NOTE: htt, 带内存栅栏设置
  }

  MmapLimiter(const MmapLimiter&);
  void operator=(const MmapLimiter&);
};

// mmap() based random-access
class PosixMmapReadableFile: public RandomAccessFile { // NOTE: htt, 从mmap()中读取数据
 private:
  std::string filename_; // NOTE: htt, 文件名
  void* mmapped_region_; // NOTE: htt, mmap()文件对应地址
  size_t length_; // NOTE: htt, mmap()文件长度
  MmapLimiter* limiter_; // NOTE: htt, mmap个数限制

 public:
  // base[0,length-1] contains the mmapped contents of the file.
  PosixMmapReadableFile(const std::string& fname, void* base, size_t length,
                        MmapLimiter* limiter)
      : filename_(fname), mmapped_region_(base), length_(length),
        limiter_(limiter) {
  }

  virtual ~PosixMmapReadableFile() {
    munmap(mmapped_region_, length_); // NOTE: htt, 释放mappings
    limiter_->Release(); // NOTE: htt, 释放mmap使用个数
  }

  virtual Status Read(uint64_t offset, size_t n, Slice* result,
                      char* scratch) const { // NOTE: htt, 从mmap()地址中读取数据
    Status s;
    if (offset + n > length_) { // NOTE: htt, 超过文件长度
      *result = Slice();
      s = IOError(filename_, EINVAL); // NOTE: htt, 已读完
    } else {
      *result = Slice(reinterpret_cast<char*>(mmapped_region_) + offset, n); // NOTE: htt, result直接指向mapping映射地址
    }
    return s;
  }
};

class PosixWritableFile : public WritableFile { // NOTE: htt, 写入文件操作，包括添加记录，刷用户态数据，刷内核态数据
 private:
  std::string filename_;
  FILE* file_;

 public:
  PosixWritableFile(const std::string& fname, FILE* f)
      : filename_(fname), file_(f) { }

  ~PosixWritableFile() {
    if (file_ != NULL) {
      // Ignoring any potential errors
      fclose(file_); // NOTE: htt, 关闭文件，同时最后设置 file_=NULL;
    }
  }

  virtual Status Append(const Slice& data) { // NOTE: htt, 追加内容到文件
    size_t r = fwrite_unlocked(data.data(), 1, data.size(), file_);
    if (r != data.size()) {
      return IOError(filename_, errno);
    }
    return Status::OK();
  }

  virtual Status Close() { // NOTE: htt, 关闭FILE
    Status result;
    if (fclose(file_) != 0) {
      result = IOError(filename_, errno);
    }
    file_ = NULL;
    return result;
  }

  virtual Status Flush() { // NOTE: htt, 将用户空间(FILE流中数据)刷入到内核空间
    if (fflush_unlocked(file_) != 0) {
      return IOError(filename_, errno);
    }
    return Status::OK();
  }

  Status SyncDirIfManifest() { // NOTE: htt, 将MANIFEST-xx文件所在的目录内存entry刷盘
    const char* f = filename_.c_str();
    const char* sep = strrchr(f, '/');
    Slice basename;
    std::string dir;
    if (sep == NULL) {
      dir = ".";
      basename = f;
    } else {
      dir = std::string(f, sep - f); // NOTE: htt, 目录
      basename = sep + 1; // NOTE: htt, 文件名
    }
    Status s;
    if (basename.starts_with("MANIFEST")) { // NOTE: htt, 如果是MANIFEST-xx文件
      int fd = open(dir.c_str(), O_RDONLY);
      if (fd < 0) {
        s = IOError(dir, errno);
      } else {
        if (fsync(fd) < 0) { // NOTE: htt, 将MANIFEST-xx文件所在的目录内存entry刷盘
          s = IOError(dir, errno);
        }
        close(fd);
      }
    }
    return s;
  }

  virtual Status Sync() { // NOTE: htt, 将目录entry信息刷盘，同时将用户态数据刷入内核，内核态数据刷入磁盘
    // Ensure new files referred to by the manifest are in the filesystem.
    Status s = SyncDirIfManifest();// NOTE: htt, 将MANIFEST-xx文件所在的目录内存entry刷盘
    if (!s.ok()) {
      return s;
    }
    if (fflush_unlocked(file_) != 0 || // NOTE: htt, 用户态数据刷入内核
        fdatasync(fileno(file_)) != 0) { // NOTE: htt, 将内核态数据刷入磁盘
      s = Status::IOError(filename_, strerror(errno));
    }
    return s;
  }
};

static int LockOrUnlock(int fd, bool lock) { // NOTE: htt, 对文件fd加写锁 或 放锁
  errno = 0;
  struct flock f;
  memset(&f, 0, sizeof(f));
  f.l_type = (lock ? F_WRLCK : F_UNLCK);
  f.l_whence = SEEK_SET;
  f.l_start = 0;
  f.l_len = 0;        // Lock/unlock entire file
  return fcntl(fd, F_SETLK, &f); // NOTE: htt,  // NOTE: htt, 对文件fd加写锁 或 放锁
}

class PosixFileLock : public FileLock { // NOTE: htt, 文件锁，包括 fd 和 文件名
 public:
  int fd_;
  std::string name_; // NOTE: htt, 文件名称
};

// Set of locked files.  We keep a separate set instead of just
// relying on fcntrl(F_SETLK) since fcntl(F_SETLK) does not provide
// any protection against multiple uses from the same process.
class PosixLockTable { // NOTE: htt, 操作一组待加锁或放锁文件名
 private:
  port::Mutex mu_;
  std::set<std::string> locked_files_; // NOTE: htt, 一组加锁文件名
 public:
  bool Insert(const std::string& fname) {
    MutexLock l(&mu_);
    return locked_files_.insert(fname).second;
  }
  void Remove(const std::string& fname) { // NOTE: htt, 移除待加锁的文件名
    MutexLock l(&mu_);
    locked_files_.erase(fname);
  }
};

class PosixEnv : public Env { // NOTE: htt, PosixEnv 环境对象,用于创建 顺序/随机/读写文件, 创建加锁文件,日志文件;调度线程,创建用户线程
 public:
  PosixEnv();
  virtual ~PosixEnv() {
    fprintf(stderr, "Destroying Env::Default()\n");
    abort();
  }

  virtual Status NewSequentialFile(const std::string& fname,
                                   SequentialFile** result) { // NOTE: htt, 创建顺序读取文件
    FILE* f = fopen(fname.c_str(), "r");
    if (f == NULL) {
      *result = NULL;
      return IOError(fname, errno);
    } else {
      *result = new PosixSequentialFile(fname, f);
      return Status::OK();
    }
  }

  virtual Status NewRandomAccessFile(const std::string& fname,
                                     RandomAccessFile** result) { // NOTE: htt, 读取文件，如果mapping个数足够则采用mmap(),否则直接读取
    *result = NULL;
    Status s;
    int fd = open(fname.c_str(), O_RDONLY);
    if (fd < 0) {
      s = IOError(fname, errno);
    } else if (mmap_limit_.Acquire()) { // NOTE: htt, 如果mapping个数可以获得，则采用mmap()
      uint64_t size;
      s = GetFileSize(fname, &size);
      if (s.ok()) {
        void* base = mmap(NULL, size, PROT_READ, MAP_SHARED, fd, 0); // NOTE: htt, 映射整个文件到mapping
        if (base != MAP_FAILED) {
          *result = new PosixMmapReadableFile(fname, base, size, &mmap_limit_); // NOTE: htt, maping映射文件
        } else {
          s = IOError(fname, errno);
        }
      }
      close(fd); // NOTE: htt, 关闭fd(不会umap当前映射区)
      if (!s.ok()) {
        mmap_limit_.Release(); // NOTE: htt, 出现异常则增加mmap映射个数
      }
    } else {
      *result = new PosixRandomAccessFile(fname, fd); // NOTE: htt, 直接读取文件
    }
    return s;
  }

  virtual Status NewWritableFile(const std::string& fname,
                                 WritableFile** result) { // NOTE: htt, 打开写文件
    Status s;
    FILE* f = fopen(fname.c_str(), "w");
    if (f == NULL) {
      *result = NULL;
      s = IOError(fname, errno);
    } else {
      *result = new PosixWritableFile(fname, f);
    }
    return s;
  }

  virtual bool FileExists(const std::string& fname) { // NOTE :htt, 文件是否存在
    return access(fname.c_str(), F_OK) == 0;
  }

  virtual Status GetChildren(const std::string& dir,
                             std::vector<std::string>* result) { // NOTE: htt, 读取目录下文件
    result->clear();
    DIR* d = opendir(dir.c_str());
    if (d == NULL) {
      return IOError(dir, errno);
    }
    struct dirent* entry;
    while ((entry = readdir(d)) != NULL) {
      result->push_back(entry->d_name);
    }
    closedir(d);
    return Status::OK();
  }

  virtual Status DeleteFile(const std::string& fname) { // NOTE: htt, 删除文件
    Status result;
    if (unlink(fname.c_str()) != 0) { // NOTE: htt, 删除文件link,当link为0时并且无其他进程打开文件则删除文件
      result = IOError(fname, errno);
    }
    return result;
  }

  virtual Status CreateDir(const std::string& name) { // NOTE: htt, 创建目录
    Status result;
    if (mkdir(name.c_str(), 0755) != 0) {
      result = IOError(name, errno);
    }
    return result;
  }

  virtual Status DeleteDir(const std::string& name) { // NOTE: htt, 删目录
    Status result;
    if (rmdir(name.c_str()) != 0) {
      result = IOError(name, errno);
    }
    return result;
  }

  virtual Status GetFileSize(const std::string& fname, uint64_t* size) { // NOTE: htt, 获取文件长度
    Status s;
    struct stat sbuf;
    if (stat(fname.c_str(), &sbuf) != 0) {
      *size = 0;
      s = IOError(fname, errno);
    } else {
      *size = sbuf.st_size;
    }
    return s;
  }

  virtual Status RenameFile(const std::string& src, const std::string& target) { // NOTE: htt, 重命名文件
    Status result;
    if (rename(src.c_str(), target.c_str()) != 0) {
      result = IOError(src, errno);
    }
    return result;
  }

  virtual Status LockFile(const std::string& fname, FileLock** lock) { // NOTE: htt, 打开锁文件(文件加锁成功)
    *lock = NULL;
    Status result;
    int fd = open(fname.c_str(), O_RDWR | O_CREAT, 0644);
    if (fd < 0) {
      result = IOError(fname, errno);
    } else if (!locks_.Insert(fname)) { // NOTE: htt, 将文件插入锁文件列表中
      close(fd);
      result = Status::IOError("lock " + fname, "already held by process");
    } else if (LockOrUnlock(fd, true) == -1) { // NOTE: htt, 对文件进行加锁
      result = IOError("lock " + fname, errno);
      close(fd);
      locks_.Remove(fname);
    } else {
      PosixFileLock* my_lock = new PosixFileLock; // NOTE: htt, 创建加锁文件
      my_lock->fd_ = fd;
      my_lock->name_ = fname;
      *lock = my_lock;
    }
    return result;
  }

  virtual Status UnlockFile(FileLock* lock) { // NOTE: htt, 释放锁
    PosixFileLock* my_lock = reinterpret_cast<PosixFileLock*>(lock);
    Status result;
    if (LockOrUnlock(my_lock->fd_, false) == -1) { // NOTE: htt, 放锁
      result = IOError("unlock", errno);
    }
    locks_.Remove(my_lock->name_); // NOTE: htt, 从锁文件列表中删除
    close(my_lock->fd_);
    delete my_lock; // NOTE: htt, 删除锁文件对象
    return result;
  }

  virtual void Schedule(void (*function)(void*), void* arg);

  virtual void StartThread(void (*function)(void* arg), void* arg);

  virtual Status GetTestDirectory(std::string* result) { // NOTE: htt, 创建测试目录
    const char* env = getenv("TEST_TMPDIR");
    if (env && env[0] != '\0') {
      *result = env;
    } else {
      char buf[100];
      snprintf(buf, sizeof(buf), "/tmp/leveldbtest-%d", int(geteuid()));
      *result = buf;
    }
    // Directory may already exist
    CreateDir(*result);
    return Status::OK();
  }

  static uint64_t gettid() { // NOTE: htt, 获取线程ID
    pthread_t tid = pthread_self();
    uint64_t thread_id = 0;
    memcpy(&thread_id, &tid, std::min(sizeof(thread_id), sizeof(tid)));
    return thread_id;
  }

  virtual Status NewLogger(const std::string& fname, Logger** result) { // NOTE: htt, 创建日志文件对象
    FILE* f = fopen(fname.c_str(), "w");
    if (f == NULL) {
      *result = NULL;
      return IOError(fname, errno);
    } else {
      *result = new PosixLogger(f, &PosixEnv::gettid);
      return Status::OK();
    }
  }

  virtual uint64_t NowMicros() { // NOTE: htt, 获取系统当前时间对应的微秒值
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return static_cast<uint64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
  }

  virtual void SleepForMicroseconds(int micros) {
    usleep(micros);
  }

 private:
  void PthreadCall(const char* label, int result) {
    if (result != 0) {
      fprintf(stderr, "pthread %s: %s\n", label, strerror(result));
      abort();
    }
  }

  // BGThread() is the body of the background thread
  void BGThread();
  static void* BGThreadWrapper(void* arg) {
    reinterpret_cast<PosixEnv*>(arg)->BGThread();
    return NULL;
  }

  pthread_mutex_t mu_; // NOTE: htt, 线程锁
  pthread_cond_t bgsignal_;
  pthread_t bgthread_; // NOTE: htt, 后台线程
  bool started_bgthread_;

  // Entry per Schedule() call
  struct BGItem { void* arg; void (*function)(void*); }; // NOTE: htt, 后台线程参数, function + arg
  typedef std::deque<BGItem> BGQueue;
  BGQueue queue_; // NOTE: htt, 一组后台线程参数

  PosixLockTable locks_;// NOTE: htt, 操作一组待加锁或放锁文件名
  MmapLimiter mmap_limit_; // NOTE: htt, 保证mmaps使用个数,避免使用过多mmap
};

PosixEnv::PosixEnv() : started_bgthread_(false) {
  PthreadCall("mutex_init", pthread_mutex_init(&mu_, NULL));
  PthreadCall("cvar_init", pthread_cond_init(&bgsignal_, NULL));
}

void PosixEnv::Schedule(void (*function)(void*), void* arg) { // NOTE: htt, 启动线程,并将<function, arg>放入等待队列中
  PthreadCall("lock", pthread_mutex_lock(&mu_)); // NOTE: htt, 先加锁

  // Start background thread if necessary
  if (!started_bgthread_) {
    started_bgthread_ = true; // NOTE: htt, 启动一个后台线程
    PthreadCall(
        "create thread",
        pthread_create(&bgthread_, NULL,  &PosixEnv::BGThreadWrapper, this)); // NOTE: htt, 启动线程
  }

  // If the queue is currently empty, the background thread may currently be
  // waiting.
  if (queue_.empty()) {
    PthreadCall("signal", pthread_cond_signal(&bgsignal_)); // NOTE: htt, 发送线程信号
  }

  // Add to priority queue
  queue_.push_back(BGItem()); // NOTE: htt, 将<function, arg> 放入队列中
  queue_.back().function = function;
  queue_.back().arg = arg;

  PthreadCall("unlock", pthread_mutex_unlock(&mu_)); // NOTE: htt, 放锁
}

void PosixEnv::BGThread() { // NOTE: htt, 循环执行后台线程
  while (true) {
    // Wait until there is an item that is ready to run
    PthreadCall("lock", pthread_mutex_lock(&mu_)); // NOTE: htt, 加锁
    while (queue_.empty()) {
      PthreadCall("wait", pthread_cond_wait(&bgsignal_, &mu_)); // NOTE: htt, 等待线程信息
    }

    void (*function)(void*) = queue_.front().function; // NOTE: htt, 取队首<function, arg>
    void* arg = queue_.front().arg;
    queue_.pop_front(); // NOTE: htt, 出队

    PthreadCall("unlock", pthread_mutex_unlock(&mu_)); // NOTE: htt, 放锁
    (*function)(arg); // NOTE: htt, 执行 (*function)(arg);
  }
}

namespace {
struct StartThreadState { // NOTE: htt, 启动线程状态
  void (*user_function)(void*);
  void* arg;
};
}
static void* StartThreadWrapper(void* arg) { // NOTE: htt, 执行 (*function)(arg)
  StartThreadState* state = reinterpret_cast<StartThreadState*>(arg);
  state->user_function(state->arg); // NOTE: htt, 执行(*function)(arg)
  delete state; // NOTE: htt, 释放暂存
  return NULL;
}

void PosixEnv::StartThread(void (*function)(void* arg), void* arg) { // NOTE: htt, 创建线程执行 (*function)(arg)
  pthread_t t;
  StartThreadState* state = new StartThreadState;
  state->user_function = function;
  state->arg = arg;
  PthreadCall("start thread",
              pthread_create(&t, NULL,  &StartThreadWrapper, state)); // NOTE: htt, 创建线程执行
}

}  // namespace

static pthread_once_t once = PTHREAD_ONCE_INIT;
static Env* default_env; // NOTE: htt, 本文件私有对象
static void InitDefaultEnv() { default_env = new PosixEnv; }

Env* Env::Default() { // NOTE: htt, 默认方式，构建 Env* 对象
  pthread_once(&once, InitDefaultEnv); // NOTE: htt, 执行一次 DefaultEnv的初始化
  return default_env;
}

}  // namespace leveldb
