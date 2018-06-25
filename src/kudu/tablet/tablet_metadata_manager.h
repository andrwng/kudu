#pragma once

#include <string>
#include <vector>

#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/macros.h"
#include "kudu/util/status.h"

namespace kudu {

class BlockId;
class FsManager;

namespace tablet {

class SuperBlockUpdatePB;
class TabletSuperBlockPB;
class TabletMetadata;

class AbsTabletMetadataManager {
 public:
  virtual Status Init() const = 0;
  virtual Status Load(const std::string& tablet_id,
              scoped_refptr<TabletMetadata>* tmeta,
              std::vector<BlockId>* orphaned_blocks = nullptr) = 0;
  virtual Status Delete(const std::string& tablet_id) = 0;
  virtual Status Flush(const TabletSuperBlockPB& pb) const = 0;

  virtual bool Exists(const std::string& tablet_id) const = 0;
  virtual Status GetSize(const std::string& tablet_id, uint64_t* size) const = 0;
};

class TabletMetadataManager : public RefCountedThreadSafe<TabletMetadataManager>,
                              public AbsTabletMetadataManager {
 public:
  explicit TabletMetadataManager(FsManager* fs_manager);
  virtual Status Init() const;
  virtual Status Load(const std::string& tablet_id,
              scoped_refptr<TabletMetadata>* tmeta,
              std::vector<BlockId>* orphaned_blocks = nullptr);
  virtual Status Delete(const std::string& tablet_id);
  virtual Status Flush(const TabletSuperBlockPB& pb) const;

  virtual bool Exists(const std::string& tablet_id) const;
  virtual Status GetSize(const std::string& tablet_id, uint64_t* size) const;
  virtual ~TabletMetadataManager() {};
 private:
  friend class RefCountedThreadSafe<TabletMetadataManager>;

  FsManager* const fs_manager_;
  DISALLOW_COPY_AND_ASSIGN(TabletMetadataManager);
};

}  // namespace tablet
}  // namespace kudu
