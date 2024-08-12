#include <iostream>
#include <cstdio>
#include <cstdlib>
#include <openssl/sha.h>
#include <assert.h>
#include <memory>
#include <mutex>
#include <string.h>
#include <unordered_map>
#include <vector>
#include <algorithm>
#include <time.h>
#include <shared_mutex>

namespace ROCKSDB_NAMESPACE {

   //============================Deduplication=========================//
    struct ChunkEntry{
        int chunk_ref_count; // 4 bytes
        uint64_t blob_file_number;  // 8 bytes
        uint64_t blob_offset; // 8 bytes
    };

    class DedupMapTable{
    private:
        DedupMapTable() = default;
        static DedupMapTable *deduptable;
        std::unordered_map<std::string, ChunkEntry *> DedupTable;
        std::shared_mutex mutex_;
        // std::atomic<bool> dedup_flag;
        std::vector<uint64_t> wal_number;
        std::string wal_dir;
        //~DedupMapTable = default;

    public:
        // std::unordered_map<std::string, ChunkEntry *> DedupTable;
        // std::shared_mutex mutex_;
        DedupMapTable(DedupMapTable const&) = delete;
        void operator=(const DedupMapTable &) = delete;

        /*using mutex_type = std::shared_timed_mutex;
        using read_only_lock  = std::shared_lock<std::mutex>;
        using updatable_lock = std::unique_lock<std::mutex>;
        mutex_type mtx;*/

        static DedupMapTable *getDedupTable();
        static bool do_dedup(std::string, uint64_t*, uint64_t*);
        static Slice do_dedup_chunk(std::string, uint64_t*, uint64_t*);
        static void dedup_chunk(Slice *blob, std::string* new_blob);
        static std::string GetHexRepresentation(const unsigned char *, size_t);
        // static void update_offset(std::string, uint64_t, uint64_t);
        static void update_offset_chunk(std::string, uint64_t, uint64_t);
    
    };
}