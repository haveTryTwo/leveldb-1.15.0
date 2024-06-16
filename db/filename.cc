// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/filename.h"
#include <ctype.h>
#include <stdio.h>
#include "db/dbformat.h"
#include "leveldb/env.h"
#include "util/logging.h"

namespace leveldb {

// A utility routine: write "data" to the named file and Sync() it.
extern Status WriteStringToFileSync(Env* env, const Slice& data, const std::string& fname);

static std::string MakeFileName(const std::string& name, uint64_t number,
                                const char* suffix) {  // NOTE:htt, 文件名: ${name}/${number}.${suffix}
  char buf[100];
  snprintf(buf, sizeof(buf), "/%06llu.%s", static_cast<unsigned long long>(number), suffix);
  return name + buf;
}

std::string LogFileName(const std::string& name, uint64_t number) {  // NOTE:htt, WAL日志文件名, ${name}/${number}.log
  assert(number > 0);
  return MakeFileName(name, number, "log");
}

std::string TableFileName(const std::string& name, uint64_t number) {  // NOTE:htt, 表文件名, ${name}/${number}.ldb
  assert(number > 0);
  return MakeFileName(name, number, "ldb");  // NOTE:htt, 表文件名, ${name}/${number}.ldb
}

std::string SSTTableFileName(const std::string& name, uint64_t number) {  // NOTE:htt, sst文件名, ${name}/${number}.sst
  assert(number > 0);
  return MakeFileName(name, number, "sst");  // NOTE:htt, sst文件名, ${name}/${number}.sst
}

std::string DescriptorFileName(const std::string& dbname,
                               uint64_t number) {  // NOTE:htt, 描述文件, ${dbname}/MANIFEST-${number}
  assert(number > 0);
  char buf[100];
  snprintf(buf, sizeof(buf), "/MANIFEST-%06llu", static_cast<unsigned long long>(number));
  return dbname + buf;
}

std::string CurrentFileName(const std::string& dbname) {  // NOTE:htt, 当前文件名, {dbname}/CURRENT
  return dbname + "/CURRENT";
}

std::string LockFileName(const std::string& dbname) {  // NOTE:htt, 锁文件名, $dbname}/LOCK
  return dbname + "/LOCK";
}

std::string TempFileName(const std::string& dbname,
                         uint64_t number) {  // NOTE:htt, 临时文件名, ${dbname}/${number}.dbtmp
  assert(number > 0);
  return MakeFileName(dbname, number, "dbtmp");
}

std::string InfoLogFileName(const std::string& dbname) {  // NOTE:htt, 日志信息, {dbname}/LOG
  return dbname + "/LOG";
}

// Return the name of the old info log file for "dbname".
std::string OldInfoLogFileName(const std::string& dbname) {  // NOTE:htt, old日志信息, {dbname}/LOG.old
  return dbname + "/LOG.old";
}

// Owned filenames have the form:
//    dbname/CURRENT
//    dbname/LOCK
//    dbname/LOG
//    dbname/LOG.old
//    dbname/MANIFEST-[0-9]+
//    dbname/[0-9]+.(log|sst|ldb)
bool ParseFileName(const std::string& fname, uint64_t* number,
                   FileType* type) {  // NOTE:htt, 从fname中获取 number 和 文件类型
  Slice rest(fname);
  if (rest == "CURRENT") {  // NOTE:htt, CURRENT文件
    *number = 0;
    *type = kCurrentFile;
  } else if (rest == "LOCK") {  // NOTE:htt, LOCK文件
    *number = 0;
    *type = kDBLockFile;
  } else if (rest == "LOG" || rest == "LOG.old") {  // NOTE:htt, LOG文件
    *number = 0;
    *type = kInfoLogFile;
  } else if (rest.starts_with("MANIFEST-")) {  // NOTE:htt, MANIFEST-文件
    rest.remove_prefix(strlen("MANIFEST-"));
    uint64_t num;
    if (!ConsumeDecimalNumber(&rest, &num)) {  // NOTE:htt, 获取整数值
      return false;
    }
    if (!rest.empty()) {
      return false;
    }
    *type = kDescriptorFile;
    *number = num;
  } else {
    // Avoid strtoull() to keep filename format independent of the
    // current locale
    uint64_t num;
    if (!ConsumeDecimalNumber(&rest, &num)) {  // NOTE:htt, ${number}.${suffix}, 获取number
      return false;
    }
    Slice suffix = rest;
    if (suffix == Slice(".log")) {  // NOTE:htt, WAL日志
      *type = kLogFile;
    } else if (suffix == Slice(".sst") || suffix == Slice(".ldb")) {  // NOTE:htt, sst或ldb文件
      *type = kTableFile;
    } else if (suffix == Slice(".dbtmp")) {  // NOTE:htt, dbtmp文件
      *type = kTempFile;
    } else {
      return false;
    }
    *number = num;
  }
  return true;
}

Status SetCurrentFile(Env* env, const std::string& dbname,
                      uint64_t descriptor_number) {  // NOTE:htt, 将MANIFEST-${number}值写入到${dbname}/CURRENT
  // Remove leading "dbname/" and add newline to manifest file name
  std::string manifest =
      DescriptorFileName(dbname, descriptor_number);  // NOTE:htt, 构建MANIFEST文件, ${dbname}/MANIFEST-${number}
  Slice contents = manifest;                          // NOTE:htt, contents内容为 ${dbname}/MANIFEST-${number}
  assert(contents.starts_with(dbname + "/"));
  contents.remove_prefix(dbname.size() + 1);                  // NOTE:htt, contents内容为 MANIFEST-${number}
  std::string tmp = TempFileName(dbname, descriptor_number);  // NOTE:htt, 构建tmp文件, ${dbname}/${number}.dbtmp
  Status s = WriteStringToFileSync(env, contents.ToString() + "\n",
                                   tmp);  // NOTE:htt, 将contents内容写入${dbname}/${number}.dbtmp
  if (s.ok()) {
    s = env->RenameFile(tmp, CurrentFileName(dbname));  // NOTE:htt, 将${dbname}/${number}.dbtmp重命名${dbname}/CURRENT
  }
  if (!s.ok()) {
    env->DeleteFile(tmp);  // NOTE:htt, 操作失败则删除 ${dbname}/${number}.dbtmp
  }
  return s;
}

}  // namespace leveldb
