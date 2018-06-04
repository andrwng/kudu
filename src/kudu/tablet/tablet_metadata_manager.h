#pragma once

#include <string>

#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/macros.h"
#include "kudu/util/status.h"

namespace kudu {

class FsManager;

namespace tablet {

class TabletSuperBlockPB;
class TabletMetadata;

class TabletMetadataManager : public RefCountedThreadSafe<TabletMetadataManager> {
 public:
  explicit TabletMetadataManager(FsManager* fs_manager);
  Status Init() const;
  Status Load(const std::string& tablet_id, scoped_refptr<TabletMetadata>* tmeta);
  Status Delete(const std::string& tablet_id);
  Status Flush(const TabletSuperBlockPB& pb) const;
  bool Exists(const std::string& tablet_id) const;
  Status GetSize(const std::string& tablet_id, uint64_t* size) const;
 private:
  friend class RefCountedThreadSafe<TabletMetadataManager>;

  FsManager* const fs_manager_;
  DISALLOW_COPY_AND_ASSIGN(TabletMetadataManager);
};

}  // namespace tablet
}  // namespace kudu
