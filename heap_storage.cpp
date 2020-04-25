#include "heap_storage.h"
#include <utility>
#include <vector>
#include <cstring>
#include <exception>
#include <map>

using namespace std;

typedef u_int16_t u16;
const unsigned int BLOCK_SZ = 4096;
​
​
/*
            ----------------------
~~~~~~~~~~~~|   SLOTTED PAGE     |~~~~~~~~~~~~
            ----------------------
*/
​
​
// Constructor
SlottedPage::SlottedPage(Dbt &block, BlockID block_id, bool is_new) : DbBlock(block, block_id, is_new) 
{
    if (is_new) 
    {
        this->num_records = 0;
        this->end_free = DbBlock::BLOCK_SZ - 1;
        put_header();
    } 
    else 
    {
        get_header(this->num_records, this->end_free);
    }
}
​
// Add a new record to the block. Return its id.
RecordID SlottedPage::add(const Dbt* data) 
{
    if (!has_room(data->get_size()))
        throw DbBlockNoRoomError(" Not enough room for new record");
    u16 id = ++this->num_records;
    u16 size = (u16) data->get_size();
    this->end_free -= size;
    u16 loc = this->end_free + 1;
    put_header();
    put_header(id, size, loc);
    memcpy(this->address(loc), data->get_data(), size);
    return id;
}
​
// Get a record from the block. Return None if it has been deleted.
Dbt* SlottedPage::get(RecordID record_id) const 
{
	u16 size
    u16 loc;
    get_header(size, loc, record_id);
​
    if (loc == 0) // make sure there's something there to actually get
        return nullptr; 
​
    return new Dbt(this->address(loc), size);
}
​
// Mark the given record_id as deleted by changing its size to zero and its location to 0.
// Compact the rest of the data in the block. But keep the record ids the same for everyone.
void SlottedPage::del(RecordID record_id)
{
    u16 size;
    u16 loc;
    get_header(size,loc,record_id);
    
    if (loc == 0) // only update if there's something there
      return;
    
    put_header(record_id,0,0);
    slide(loc, loc + size);
}
​
// Replace the record with the given data.
void SlottedPage::put(RecordID record_id, const Dbt &data)
{
	u16 size
    u16 loc;
    u16 updated_size = (u16) data.get_size();
    get_header(size, loc, record_id);
​
    if (updated_size > size) 
    {
        u16 extra_space = updated_size - size;
        if (!has_room(extra_space))
    		throw DbBlockNoRoomError(" Not enough room for enlarged record");
		
        // move the data over and update
        slide(loc, loc - extra_space);
		memcpy(this->address(loc-extra_space), data.get_data(), updated_size);
	} 
​
    else 
    {
		memcpy(this->address(loc), data.get_data(), updated_size);
        slide(loc+updated_size, loc+size);
	}
​
    get_header(size, loc, record_id);
    put_header(record_id, updated_size, loc);
}
​
// Sequence of all non-deleted record ids.
RecordIDs* SlottedPage::ids(void) const 
{
	u16 size;
    u16 loc;
    RecordIDs *ids = new vector<RecordID>();
​
	for (RecordID i = 1; i <= this->num_records; i++)
    {
	    get_header(size, loc, record_id);
	    
        if (loc != 0)
	    	ids->push_back((RecordID)i);
	}
	return ids;
}
​
// Calculate if we have room to store a record with given size. The size should include the 4 bytes
// for the header, too, if this is an add.
bool SlottedPage::has_room(u16 size) 
{
    u16 available = this->end_free - (this->num_records + 2) * 4;
    return size <= available;
}
​
// Get 2-byte integer at given offset in block.
u16 SlottedPage::get_n(u16 offset) 
{
    return *(u16*)this->address(offset);
}
​
// Put a 2-byte integer at given offset in block.
void SlottedPage::put_n(u16 offset, u16 n) 
{
    *(u16*)this->address(offset) = n;
}
​
// Make a void* pointer for a given offset into the data block.
void* SlottedPage::address(u16 offset) 
{
    return (void*)((char*)this->block.get_data() + offset);
}
​
// Store the size and offset for given id. For id of zero, store the block header.
void SlottedPage::put_header(RecordID id, u16 size, u16 loc) {
    if (id == 0) { // called the put_header() version and using the default params
        size = this->num_records;
        loc = this->end_free;
    }
    put_n(4*id, size);
    put_n(4*id + 2, loc);
}
​
// Get the headers size and location
void SlottedPage::get_header(u16& size, u16& loc, RecordID id)
{
    size = this->get_n(4 * id);
    loc = this->get_n(4 * id + 2);
}
​
/*
If start < end, then remove data from offset start up to but not including offset end by sliding data
that is to the left of start to the right. If start > end, then make room for extra data from end to start
by sliding data that is to the left of start to the left.
Also fix up any record headers whose data has slid. Assumes there is enough room if it is a left
shift (end < start).
*/
void SlottedPage::slide(u16 start, u16 end)
{
    int move_over = end - start;
    
    if (move_over == 0) // no space
        return; 
​
    // set up slide
    int space = start - (this->end_free + 1U);
    void *destination = this->address((u16)(this->end_free + 1 + move_over));
    void *from = this->address((u16)(this->end_free + 1));
    char temp[space];
    
    // copy
    memcpy(temp, from, space);
    memcpy(destination, temp, space);
​
    // fix up headers
    RecordIDs* record_ids = ids();
	for (auto const& record_id : *record_ids) 
    {
		u16 size
        16 loc;
		get_header(size, loc, record_id);
​
		if (loc <= start) 
        {
			loc += move_over;
			put_header(record_id, size, loc);
		}
	}
    this->end_free += move_over;
    put_header();
    delete record_ids;
}
​
/*
            ----------------------
~~~~~~~~~~~~|      HEAP FILE     |~~~~~~~~~~~~
            ----------------------
*/
​
// Create the physical file
void HeapFile::create()
{
    this->db_open(DB_CREATE | DB_EXCL);
    SlottedPage* block = this->get_new();
    this->put(block);
    delete block;
}
​
/*
    FIX ME | THIS DOESN'T SEEM FULLY CORRECT? I MIGHT HAVE TO DO OTHER CHECKS
*/
// Wrapper for Berkeley DB open, which does both open and creation.
void HeapFile::db_open(uint flags)
{
    if(!this->closed)
        return;
​
    this->db_filename = filepath + '/' + name + ".db";
    this->db.open(nullptr, this->db_filename.c_str(), nullptr, DB_RECNO, flags, 0644);
​
    this->closed = false;
}
​
// Drop the physical file, close but don't set to true
void HeapFile::drop(void) 
{
   close();
   Db db(_DB_ENV, 0);
   db.remove(this->dbfilename.c_str(), nullptr, 0);
}
​
// Open the physical file
void HeapFile::open()
{
    this->db_open();
}
​
// Closes the physical file, close and set to true
void HeapFile::close()
{
    this->db.close(0);
    this->closed = true;
}
​
// Write a block back to the database file.
void HeapFile::put(DbBlock* block)
{
    BlockID id = block->get_block_id();
    Dbt key(&id, sizeof(id));
    this->db.put(nullptr, &key, block->get_block(), 0);
}
​
// Sequence of all block ids.
BlockIDs* HeapFile::block_ids()
{
    BlockIDs* blocks = new BlockIDs();
    for(BlockID i = 1; i <= this->last; i++)
        blocks->push_back(i);
    return blocks;    
}
​
// Get a block from the database file.
SlottedPage* HeapFile::get(BlockID block_id)
{
    Dbt key(&block_id, sizeof(block_id));
    Dbt data;
    this->db.get(nullptr, &key, &data, 0);
    SlottedPage* slotted_page = new SlottedPage(data, block_id, false);
    return slotted_page;
}
​
// Allocate a new block for the database file.
// Returns the new empty DbBlock that is managing the records in this block and its block id.
SlottedPage* HeapFile::get_new(void) 
{
    char block[DbBlock::BLOCK_SZ];
    std::memset(block, 0, sizeof(block));
    Dbt data(block, sizeof(block));
​
    int block_id = ++this->last;
    Dbt key(&block_id, sizeof(block_id));
​
    // write out an empty block and read it back in so Berkeley DB is managing the memory
    SlottedPage* page = new SlottedPage(data, this->last, true);
    this->db.put(nullptr, &key, &data, 0); // write it out with initialization applied
    this->db.get(nullptr, &key, &data, 0);
    return page;
}

/*
            ----------------------
~~~~~~~~~~~~|   HEAPTABLE         |~~~~~~~~~~~~
            ----------------------
*/
​

HeapTable::HeapTable(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes) 
: DbRelation(table_name, column_names, column_attributes) {
    file = HeapFile(table_name);
}


void HeapTable::create(){
    this->file.create();
}

void HeapTable::create_if_not_exists() {
    try {
        this->open();
    } catch(DbException &e) {
        this->create();
    }
}

void HeapTable::drop() {
    this->file.drop();
}

void HeapTable::open() {
    file->open();
}

void HeapTable::close() {
    file->close();
}

Handle HeapTable::insert(const ValueDict *row) {
    this.open();
    return this->append(this->validate(row));
}

void HeapTable::update(const Handle handle, const ValueDict *new_values) {
    cout << "Not Implemented" << endl;
}

void HeapTable::del(const Handle handle) {
    cout << "Not Implemented" << endl;
}

Handles* HeapTable::select() {
    Handles* handles = new Handles();
    BlockIDs* block_ids = file.block_ids();
    for (auto const& block_id: *block_ids) {
        SlottedPage* block = file.get(block_id);
        RecordIDs* record_ids = block->ids();
        for (auto const& record_id: *record_ids)
            handles->push_back(Handle(block_id, record_id));
        delete record_ids;
        delete block;
    }
    delete block_ids;
    return handles;
}

Handles* HeapTable::select(const ValueDict *where) {
    Handles* handles = new Handles();
    BlockIDs* block_ids = file.block_ids();
    for (auto const& block_id: *block_ids) {
        SlottedPage* block = file.get(block_id);
        RecordIDs* record_ids = block->ids();
        for (auto const& record_id: *record_ids)
            handles->push_back(Handle(block_id, record_id));
        delete record_ids;
        delete block;
    }
    delete block_ids;
    return handles;
}

ValueDict* HeapTable::project(Handle handle){
    BlockID block_id = handle.first;
    RecordID record_id = handle.second;
    SlottedPage* block = file.get(block_id);
    Dbt* data = block->get(record_id);
    ValueDict* row = unmarshal(data);

    delete data;
    return row;
}

ValueDict* HeapTable::project(Handle handle, const ColumnNames *column_names){
    // NotImplemented
    return NULL;
}

ValueDict* HeapTable::validate(const ValueDict *row){
    ValueDict* full_row;

    for(auto const& column_name : this->column_names) {
        ValueDict::const_iterator itr = row->find(column_name);
        Value value;
        if(itr == row.end()) {
            throw DbRelationError("don't know how to handle NULLs, defaults, etc. yet");
        } else {
            value = itr->second;
        }

        full_row.insert(pair<Identifier, Value>(itr.first, itr.second));
    }

    return full_row;
}

Handle HeapTable::append(const ValueDict *row){
    Dbt *data = this->marshal(row);
    SlottedPage *block = this->file.get(this->file.get_last_block_i());
    RecordID record_id;
    try{
        record_id = block.add(data);
    } catch(DbBlockNoRoomError &e){
        block = this->file.get_new();
        record_id = block.add(data);
    }
    this->file.put(block);
    delete data;
    delete block;
    Handle handle(this->file.get_last_block_id(), record_id);
    return handle;
}

Dbt* HeapTable::marshal(const ValueDict *row) {
    char *bytes = new char[DbBlock::BLOCK_SZ]; // more than we need (we insist that one row fits into DbBlock::BLOCK_SZ)
    uint offset = 0;
    uint col_num = 0;
    for (auto const& column_name: this->column_names) {
        ColumnAttribute ca = this->column_attributes[col_num++];
        ValueDict::const_iterator column = row->find(column_name);
        Value value = column->second;
        if (ca.get_data_type() == ColumnAttribute::DataType::INT) {
            *(int32_t*) (bytes + offset) = value.n;
            offset += sizeof(int32_t);
        } else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT) {
            uint size = value.s.length();
            *(u16*) (bytes + offset) = size;
            offset += sizeof(u16);
            memcpy(bytes+offset, value.s.c_str(), size); // assume ascii for now
            offset += size;
        } else {
            throw DbRelationError("Only know how to marshal INT and TEXT");
        }
    }
    char *right_size_bytes = new char[offset];
    memcpy(right_size_bytes, bytes, offset);
    delete[] bytes;
    Dbt *data = new Dbt(right_size_bytes, offset);
    return data;
}

ValueDict* HeapTable::unmarshal(Dbt *data){

}
