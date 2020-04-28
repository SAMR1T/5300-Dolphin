#include "heap_storage.h"
#include <utility>
#include <vector>
#include <cstring>
#include <exception>
#include <map>

using namespace std;

typedef u_int16_t u16;
const unsigned int BLOCK_SZ = 4096;


/*
            ----------------------
~~~~~~~~~~~~|   SLOTTED PAGE     |~~~~~~~~~~~~
            ----------------------
*/

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

// Get a record from the block. Return None if it has been deleted.
Dbt* SlottedPage::get(RecordID record_id){
	u16 size;
    u16 loc;
    get_header(size, loc, record_id);

    if (loc == 0) // make sure there's something there to actually get
        return nullptr;
    return new Dbt(this->address(loc), size);
}

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

// Replace the record with the given data.
void SlottedPage::put(RecordID record_id, const Dbt &data)
{
	u16 size;
    u16 loc;
    u16 updated_size = (u16) data.get_size();
    get_header(size, loc, record_id);

    if (updated_size > size) 
    {
        u16 extra_space = updated_size - size;
        if (!has_room(extra_space))
    		throw DbBlockNoRoomError(" Not enough room for enlarged record");

        // move the data over and update
        slide(loc, loc - extra_space);
		memcpy(this->address(loc-extra_space), data.get_data(), updated_size);
	}

    else 
    {
		memcpy(this->address(loc), data.get_data(), updated_size);
        slide(loc+updated_size, loc+size);
	}

    get_header(size, loc, record_id);
    put_header(record_id, updated_size, loc);
}

// Sequence of all non-deleted record ids.
RecordIDs* SlottedPage::ids(void)
{
	u16 size;
    u16 loc;
    RecordIDs *ids = new vector<RecordID>();

	for (RecordID i = 1; i <= this->num_records; i++)
    {
	    get_header(size, loc, i);
	    
        if (loc != 0)
	    	ids->push_back((RecordID)i);
	}
	return ids;
}

// Calculate if we have room to store a record with given size. The size should include the 4 bytes
// for the header, too, if this is an add.
bool SlottedPage::has_room(u16 size) 
{
    u16 available = this->end_free - (this->num_records + 2) * 4;
    return size <= available;
}

// Get 2-byte integer at given offset in block.
u16 SlottedPage::get_n(u16 offset) 
{
    return *(u16*)this->address(offset);
}

// Put a 2-byte integer at given offset in block.
void SlottedPage::put_n(u16 offset, u16 n) 
{
    *(u16*)this->address(offset) = n;
}

// Make a void* pointer for a given offset into the data block.
void* SlottedPage::address(u16 offset) 
{
    return (void*)((char*)this->block.get_data() + offset);
}

// Store the size and offset for given id. For id of zero, store the block header.
void SlottedPage::put_header(RecordID id, u16 size, u16 loc) {
    if (id == 0) { // called the put_header() version and using the default params
        size = this->num_records;
        loc = this->end_free;
    }
    put_n(4*id, size);
    put_n(4*id + 2, loc);
}

// Get the headers size and location
void SlottedPage::get_header(u16& size, u16& loc, RecordID id)
{
    size = this->get_n(4 * id);
    loc = this->get_n(4 * id + 2);
}

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
    // set up slide
    int space = start - (this->end_free + 1U);
    void *destination = this->address((u16)(this->end_free + 1 + move_over));
    void *from = this->address((u16)(this->end_free + 1));
    char temp[space]; 
    // copy
    memcpy(temp, from, space);
    memcpy(destination, temp, space);

    // fix up headers
    RecordIDs* record_ids = ids();
    for (auto const& record_id : *record_ids) 
    {
		u16 size;
        u16 loc;
		get_header(size, loc, record_id);

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

/*
            ------------------
~~~~~~~~~~~~|   HEAPFILE     |~~~~~~~~~~~~
            ------------------
*/
// HeapFile::HeapFile(string name) : DbFile(name), dbfilename(""), last(0), closed(true), db(_DB_ENV, 0) {
// 	this->dbfilename = this->name + ".db";
// }

// Create the physical file
void HeapFile::create(){
    //cout << "HeapFile creation" << endl;
    this->db_open(DB_CREATE | DB_EXCL);
    //cout << "db opened" << endl;
    SlottedPage* block = this->get_new();
    this->put(block);
    delete block;
}

/**
    Wrapper for Berkeley DB open, which does both open and creation.
    @param flags flags parameter
*/
// 
void HeapFile::db_open(uint flags) {
    if(!this->closed)
        return;
    // try{
    //     this->db.set_re_len(DbBlock::BLOCK_SZ);
    //     this->db.open(nullptr, this->dbfilename.c_str(), nullptr, DB_RECNO, flags, 0);
    // }catch (DbException &e) {
    //     cerr << "Fail to open db: " << e.what() << endl;
    //     //return;
    // }
    //this->dbfilename = filepath + '/' + name + ".db";
    this->db.set_re_len(DbBlock::BLOCK_SZ);
    this->db.open(nullptr, this->dbfilename.c_str(), nullptr, DB_RECNO, flags, 0);
    //cout << "Comes here" << endl;
    if(flags) {
        //cout << "Comes 0" << endl;
        this->last = 0;
    } else {
        //cout << "Comes 999" << endl;
        DB_BTREE_STAT* db_bt_stat;
        this->db.stat(nullptr, &db_bt_stat, DB_FAST_STAT);
        this->last = db_bt_stat->bt_ndata;
    }
    this->closed = false;
}

// Drop the physical file, close but don't set to true
void HeapFile::drop(void) {
   close();
   Db db(_DB_ENV, 0);
   db.remove(this->dbfilename.c_str(), nullptr, 0);
}

// Open the physical file
void HeapFile::open(){
    this->db_open();
}

// Closes the physical file, close and set to true
void HeapFile::close(){
    this->db.close(0);
    this->closed = true;
}

// Write a block back to the database file.
void HeapFile::put(DbBlock* block)
{
    BlockID id = block->get_block_id();
    Dbt key(&id, sizeof(id));
    this->db.put(nullptr, &key, block->get_block(), 0);
}

// Sequence of all block ids.
BlockIDs* HeapFile::block_ids()
{
    BlockIDs* blocks = new BlockIDs();
    for(BlockID i = 1; i <= this->last; i++)
        blocks->push_back(i);
    return blocks;    
}

// Get a block from the database file.
SlottedPage* HeapFile::get(BlockID block_id)
{
    Dbt key(&block_id, sizeof(block_id));
    Dbt data;
    this->db.get(nullptr, &key, &data, 0);
    SlottedPage* slotted_page = new SlottedPage(data, block_id, false);
    return slotted_page;
}

// Allocate a new block for the database file.
// Returns the new empty DbBlock that is managing the records in this block and its block id.
SlottedPage* HeapFile::get_new(void) 
{
    char block[DbBlock::BLOCK_SZ];
    std::memset(block, 0, sizeof(block));
    Dbt data(block, sizeof(block));

    int block_id = ++this->last;
    Dbt key(&block_id, sizeof(block_id));

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

/**
    Constructor
*/

HeapTable::HeapTable(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes) :
	DbRelation(table_name, column_names, column_attributes), file(table_name) {
}

/**
    Create a new table
*/


void HeapTable::create(){
    this->file.create();
}

/**
    Create a new table if not exist
*/
void HeapTable::create_if_not_exists() {
    try {
        this->open();
    } catch(DbException &e) {
        this->create();
    }
}

/**
    Drop a table
*/
void HeapTable::drop() {
    this->file.drop();
}

/**
    Open an existing table
*/
void HeapTable::open() {
    file.open();
}

/**
    Close an open table
*/
void HeapTable::close() {
    file.close();
}

/**
    Execute: INSERT INTO <table_name> ( <row_keys> ) VALUES ( <row_values> )
    @param row  a dictionary keyed by column names
    @returns    a handle to the new row
*/
Handle HeapTable::insert(const ValueDict *row) {
    this->open();
    return this->append(this->validate(row));
}

/**
    Execute: UPDATE INTO <table_name> SET <new_valus> WHERE <handle>
    @param handle     the row to update
    @param new_values a dictionary keyd by column names for changing columns
*/
void HeapTable::update(const Handle handle, const ValueDict *new_values) {
    cout << "Not Implemented" << endl;
}

/**
    Execute: DELETE FROM <table_name> WHERE <handle>
    @param handle     the row to delete
*/
void HeapTable::del(const Handle handle) {
    cout << "Not Implemented" << endl;
}

/**
    Conceptually, execute: SELECT <handle> FROM <table_name> WHERE 1
     @returns  a pointer to a list of handles for qualifying rows (caller frees)
*/
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

/**
    Return a sequence of all values for handle (SELECT *).
    @param handle  row to get values from
    @returns       dictionary of values from row (keyed by all column names)
*/
ValueDict* HeapTable::project(Handle handle){
    BlockID block_id = handle.first;
    RecordID record_id = handle.second;
    SlottedPage* block = file.get(block_id);
    Dbt* data = block->get(record_id);
    ValueDict* row = unmarshal(data);

    // Testing element stored in row after unmarshal
    // for(auto const& column_name : this->column_names) {
    //     ValueDict::const_iterator itr = row->find(column_name);
    //     cout << itr->first << " : " << itr->second.s << endl;
    // }
    delete data;
    return row;
}

ValueDict* HeapTable::project(Handle handle, const ColumnNames *column_names){
    // NotImplemented
    return NULL;
}

/**
    Check if all the columns all the columns are there and fill in any missing column
    @param row the row neeed to be verified
    @return the checked and filled row
*/
ValueDict* HeapTable::validate(const ValueDict *row){
    ValueDict* full_row = new ValueDict();

    for(auto const& column_name : this->column_names) {
        ValueDict::const_iterator itr = row->find(column_name);
        Value value;
        if(itr == row->end()) {
            throw DbRelationError("don't know how to handle NULLs, defaults, etc. yet");
        } else {
            value = itr->second;
        }

        full_row->insert(pair<Identifier, Value>(itr->first, itr->second));
    }

    return full_row;
}

/**
    Append a row into table
    @param row that will be appended
    @return Handle store the pair of (block_id of the block it added, record_id)
*/
Handle HeapTable::append(const ValueDict *row){
    // Testing element stored in row
    // for(ValueDict::const_iterator itr = row->begin(); itr != row->end(); ++itr) {
    //     cout << itr->first << " : " << itr->second.s << endl;
    // }
    Dbt *data = this->marshal(row);
    SlottedPage *block = this->file.get(this->file.get_last_block_id());
    RecordID record_id;
    try{
        record_id = block->add(data);
    } catch(DbBlockNoRoomError &e){
        block = this->file.get_new();
        record_id = block->add(data);
    }
    this->file.put(block);
    delete data;
    delete block;
    Handle handle(this->file.get_last_block_id(), record_id);
    return handle;
}

/**
    return the bits to go into the file
    caller responsible for freeing the returned Dbt and its enclosed ret->get_data().
*/
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
            //cout << "This col name is " << column_name << endl;
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

/**
    unparse the dits from file 
    return row converrted from the bit
*/
ValueDict* HeapTable::unmarshal(Dbt *data){
    ValueDict* row = new ValueDict();
    char *bytes = (char*)data->get_data();
    uint offset = 0;
    uint col_num = 0;
    Value value;


    for(auto const& column_name: this->column_names) {

        ColumnAttribute col_attr = this->column_attributes[col_num++];
        value.data_type = col_attr.get_data_type();

        if(col_attr.get_data_type() == ColumnAttribute::DataType::INT) {
            value.n = *(int32_t*)(bytes + offset);
            offset += sizeof(int32_t);
            row->insert(make_pair(column_name, value.n));
        } else if(col_attr.get_data_type() == ColumnAttribute::DataType::TEXT) {
            u16 size = *(u16*)(bytes + offset);
            offset += sizeof(u16);
            char* text = new char[size];
            memcpy(text, bytes + offset, size);
            string text_string(text);
            value.s = text_string;
            offset += size;
            row->insert(make_pair(column_name, value.s));
        } else {
            throw DbRelationError("Only know how to unmarshal INT and TEXT");
        }
    }

    return row;
}

/**
 * Print out given failure message and return false.
 * @param message reason for failure
 * @return false
 */
bool assertion_failure(string message) {
    cout << "FAILED TEST: " << message << endl;
    return false;
}
/**
 * Testing function for SlottedPage.
 * @return true if testing succeeded, false otherwise */
bool test_slotted_page() {
    // construct one
    char blank_space[DbBlock::BLOCK_SZ];
    Dbt block_dbt(blank_space, sizeof(blank_space));
    SlottedPage slot(block_dbt, 1, true);
    // add a record
    char rec1[] = "hello";
    Dbt rec1_dbt(rec1, sizeof(rec1));
    RecordID id = slot.add(&rec1_dbt);
    if (id != 1)
        return assertion_failure("add id 1");
    // get it back
    Dbt *get_dbt = slot.get(id);
    string expected(rec1, sizeof(rec1));
    string actual((char *) get_dbt->get_data(), get_dbt->get_size());
    if (expected != actual)
        return assertion_failure("get 1 back " + actual);
    delete get_dbt;
    // add another record and fetch it back
    char rec2[] = "goodbye";
    Dbt rec2_dbt(rec2, sizeof(rec2));
    id = slot.add(&rec2_dbt);
    if (id != 2)
        return assertion_failure("add id 2");
    // get it back
    get_dbt = slot.get(id);
    expected = string(rec2, sizeof(rec2));
    actual = string((char *) get_dbt->get_data(), get_dbt->get_size());
    if (expected != actual)
        return assertion_failure("get 2 back " + actual);
    delete get_dbt;
    // test put with expansion (and slide and ids)
    char rec1_rev[] = "something much bigger";
    rec1_dbt = Dbt(rec1_rev, sizeof(rec1_rev));
    slot.put(1, rec1_dbt);
    // check both rec2 and rec1 after expanding put
    get_dbt = slot.get(2);
    expected = string(rec2, sizeof(rec2));
    actual = string((char *) get_dbt->get_data(), get_dbt->get_size());
    if (expected != actual)
        return assertion_failure("get 2 back after expanding put of 1 " + actual);
    delete get_dbt;
    get_dbt = slot.get(1);
    expected = string(rec1_rev, sizeof(rec1_rev));
    actual = string((char *) get_dbt->get_data(), get_dbt->get_size());
    if (expected != actual)
        return assertion_failure("get 1 back after expanding put of 1 " + actual);
    delete get_dbt;
    // test put with contraction (and slide and ids)
    rec1_dbt = Dbt(rec1, sizeof(rec1));
    slot.put(1, rec1_dbt);
    // check both rec2 and rec1 after contracting put
    get_dbt = slot.get(2);
    expected = string(rec2, sizeof(rec2));
    actual = string((char *) get_dbt->get_data(), get_dbt->get_size());
    if (expected != actual)
        return assertion_failure("get 2 back after contracting put of 1 " + actual);
    delete get_dbt;
    get_dbt = slot.get(1);
    expected = string(rec1, sizeof(rec1));
    actual = string((char *) get_dbt->get_data(), get_dbt->get_size());
    if (expected != actual)
        return assertion_failure("get 1 back after contracting put of 1 " + actual);
    delete get_dbt;
    // test del (and ids)
    RecordIDs *id_list = slot.ids();
    if (id_list->size() != 2 || id_list->at(0) != 1 || id_list->at(1) != 2)
        return assertion_failure("ids() with 2 records");
    delete id_list;
    slot.del(1);
    id_list = slot.ids();
    if (id_list->size() != 1 || id_list->at(0) != 2)
        return assertion_failure("ids() with 1 record remaining");
    delete id_list;
    get_dbt = slot.get(1);
    if (get_dbt != nullptr)
        return assertion_failure("get of deleted record was not null");
    // try adding something too big
    rec2_dbt = Dbt(nullptr, DbBlock::BLOCK_SZ - 10); // too big, but only because we have a record in there
    try {
        slot.add(&rec2_dbt);
        return assertion_failure("failed to throw when add too big");
    } catch (const DbBlockNoRoomError &exc) {
        // test succeeded - this is the expected path
    } catch (...) {
        // Note that this won't catch segfault signals -- but in that case we also know the test failed
        return assertion_failure("wrong type thrown when add too big");
    }
    return true;
}

bool test_heap_storage(){
    ColumnNames column_names;
	column_names.push_back("a");
	column_names.push_back("b");
	ColumnAttributes column_attributes;
	ColumnAttribute ca(ColumnAttribute::INT);
	column_attributes.push_back(ca);
	ca.set_data_type(ColumnAttribute::TEXT);
	column_attributes.push_back(ca);
    //cout << "create table1" << endl;
    HeapTable table1("_test_create_drop_cpp", column_names, column_attributes);
    table1.create();
    cout << "create ok" << endl;
    table1.drop();  // drop makes the object unusable because of BerkeleyDB restriction -- maybe want to fix this some day
    cout << "drop ok" << endl;

    HeapTable table("_test_data_cpp", column_names, column_attributes);
    table.create_if_not_exists();
    cout << "create_if_not_exsts ok" << endl;

    ValueDict row;
    row["a"] = Value(12);
    row["b"] = Value("Hello!");
    cout << "try insert" <<endl;
    table.insert(&row);
    cout << "insert ok" << endl;
    Handles* handles = table.select();
    cout << "select ok " << handles->size() << endl;
    ValueDict *result = table.project((*handles)[0]);
    cout << "project ok" << endl;
    Value value = (*result)["a"];
    cout << "Test value stored in the db" << endl;
    //cout << value.n << endl;
    //cout << result->size() << endl;
    if (value.n != 12)
    	return false;
    value = (*result)["b"];
    //cout << value.n << endl;
    if (value.s != "Hello!")
		return false;
    table.drop();

    cout << "Test slotted page" << endl;
    if(!test_slotted_page())
        return false;
    return true;
    //return test_slotted_page();
}
