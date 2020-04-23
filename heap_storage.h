#pragma once

#include "db_cxx.h"
#include "storage_engine.h"

/**
    @class SlottedPage - heap file implementation of DbBlock.


*/

class SlottedPage : public DbBlock {
    public:
        SlottedPage(Dbt &block, BlockID block_id, bool is_new = false);

        virtual ~SlottedPage() {}

        SlottedPage(const SlottedPage &other) = delete;

        SlottedPage(SlottedPage &&temp) = delete;

        SlottedPage &operator=(const SlottedPage &other) = delete;

        SlottedPage &operator=(SlottedPage &temp) = delete;

        virtual RecordID add(const Dbt *data);

        virtual Dbt *get(RecordID record_id);

        virtual void put(RecordID record_id, const Dbt &data);

        virtual void del(RecordID record_id);

        virtual RecordIDs *ids(void);
    
    protected:
        u_int16_t num_records;
        u_int16_t end_free;

        virtual void get_header(u_int16_t &size, u_int16_t &loc, RecordID id = 0);

        virtual void put_header(RecordID id = 0, u_int16_t size = 0, u_int16_t loc = 0);

        virtual bool has_room(u_int16_t size);

        virtual void slide(u_int16_t start, u_int16_t end);

        virtual u_int16_t get_n(u_int16_t offset);

        virtual void put_n(u_int16_t offset, u_int16_t n);

        virtual void *address(u_int16_t offset);
};

/**
    @class HeapFile - heap file implementation of DbFile

*/

class HeapFile : public DbFile {
    public:
        HeapFile(std::string name) : DbFile(name), dbfilename(""), last(0), closed(true), db(_DB_ENV, 0) {}

        virtual ~HeapFile() {}

        HeapFile(const HeapFile &other) = delete;

        HeapFile(HeapFile &&temp) = delete;

        HeapFile &operator=(const HeapFile &other) = delete;

        HeapFile &operator=(HeapFile &&temp) = delete;

        virtual void create(void);

        virtual void drop(void);

        virtual void open(void);

        virtual void close(void);

        virtual SlottedPage *get_new(void);

        virtual SlottedPage *get(BlockID block_id);

        virtual void put(DbBlock *block);

        virtual BlockIDs *block_ids();

        virtual u_int32_t get_last_block_id() { return last; }

    protected:
        std::string dbfilename;
        u_int32_t last;
        bool closed;
        Db db;

        virtual void db_open(uint flags = 0);
};

/**
 * @class HeapTable - Heap storage engine (implementation of DbRelation)
 */

class HeapTable : public DbRelation {
    public:
        HeapTable(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes);

        virtual ~HeapTable() {}

        HeapTable(const HeapTable &other) = delete;

        HeapTable(HeapTable &&temp) = delete;

        HeapTable &operator=(const HeapTable &other) = delete;

        HeapTable &operator=(HeapTable &&temp) = delete;

        virtual void create();

        virtual void create_if_not_exists();

        virtual void drop();

        virtual void open();

        virtual void close();

        virtual Handle insert(const ValueDict *row);

        virtual void update(const Handle handle, const ValueDict *new_values);

        virtual void del(const Handle handle);

        virtual Handles *select();

        virtual Handles *select(const ValueDict *where);

        virtual ValueDict *project(Handle handle);

        virtual ValueDict *project(Handle handle, const ColumnNames *column_names);

    protected:
        HeapFile file;

        virtual ValueDict *validate(const ValueDict *row);

        virtual Handle append(const ValueDict *row);

        virtual Dbt *marshal(const ValueDict *row);

        virtual ValueDict *unmarshal(Dbt *data);
};

bool test_heap_storage();
