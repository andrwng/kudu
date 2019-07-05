/**
 * A MergeOperator for rocksdb that implements string append.
 * @author Deon Nicholas (dnicholas@fb.com)
 * Copyright 2013 Facebook
 */

#pragma once
#include "kudu/rocksdb/merge_operator.h"
#include "kudu/rocksdb/slice.h"

namespace rocksdb {

class StringAppendOperator : public AssociativeMergeOperator {
 public:
  // Constructor: specify delimiter
  explicit StringAppendOperator(char delim_char);

  virtual bool Merge(const Slice& key,
                     const Slice* existing_value,
                     const Slice& value,
                     std::string* new_value,
                     Logger* logger) const override;

  virtual const char* Name() const override;

 private:
  char delim_;         // The delimiter is inserted between elements

};

} // namespace rocksdb
