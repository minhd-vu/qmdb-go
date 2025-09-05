package qmdb

/*
#cgo LDFLAGS: -lqmdb_sys
#include <stdint.h>
#include <stdlib.h>

// Core functions
int qmdb_init_dir(const char* dir);
void* qmdb_new(const char* dir);
void qmdb_free(void* handle);
int qmdb_start_block(void* handle, int64_t height, void* tasks_manager);
void* qmdb_get_shared(void* handle);
void qmdb_shared_free(void* shared);
int qmdb_flush(void* handle);

// Shared handle functions
int qmdb_shared_insert_extra_data(void* shared, int64_t height, const char* data);
void qmdb_shared_add_task(void* shared, int64_t task_id);
size_t qmdb_shared_read_entry(void* shared, int64_t height, const uint8_t* key_hash, size_t key_hash_len, const uint8_t* key, size_t key_len, uint8_t* buf, size_t buf_len, int* found);

// ChangeSet functions
void* qmdb_changeset_new();
void qmdb_changeset_free(void* changeset);
int qmdb_changeset_add_op(void* changeset, uint8_t op_type, uint8_t shard_id, const uint8_t* key_hash, size_t key_hash_len, const uint8_t* key, size_t key_len, const uint8_t* value, size_t value_len);
void qmdb_changeset_sort(void* changeset);

// TasksManager functions
void* qmdb_tasks_manager_new(void** changesets, size_t changeset_count, int64_t last_task_id);
void qmdb_tasks_manager_free(void* manager);

// Utility functions
int qmdb_hash(const uint8_t* input, size_t input_len, uint8_t* output);
size_t qmdb_byte0_to_shard_id(uint8_t byte0);

// Constants
extern uint8_t QMDB_OP_CREATE;
extern uint8_t QMDB_OP_WRITE;
extern uint8_t QMDB_OP_DELETE;
extern uint64_t QMDB_IN_BLOCK_IDX_BITS;
*/
import "C"
import (
	"fmt"
	"unsafe"
)

// Wrapper types
type QmdbHandle struct {
	ptr unsafe.Pointer
}

type QmdbSharedHandle struct {
	ptr unsafe.Pointer
}

type QmdbChangeSet struct {
	ptr unsafe.Pointer
}

type QmdbTasksManager struct {
	ptr unsafe.Pointer
}

// Constants
var (
	OpCreate       = uint8(C.QMDB_OP_CREATE)
	OpWrite        = uint8(C.QMDB_OP_WRITE)
	OpDelete       = uint8(C.QMDB_OP_DELETE)
	InBlockIdxBits = uint64(C.QMDB_IN_BLOCK_IDX_BITS)
)

// Core functions
func InitDir(dir string) error {
	cDir := C.CString(dir)
	defer C.free(unsafe.Pointer(cDir))

	result := C.qmdb_init_dir(cDir)
	if result != 0 {
		return fmt.Errorf("failed to initialize directory")
	}
	return nil
}

func New(dir string) (*QmdbHandle, error) {
	cDir := C.CString(dir)
	defer C.free(unsafe.Pointer(cDir))

	ptr := C.qmdb_new(cDir)
	if ptr == nil {
		return nil, fmt.Errorf("failed to create qmdb instance")
	}

	return &QmdbHandle{ptr: ptr}, nil
}

func (h *QmdbHandle) Free() {
	if h.ptr != nil {
		C.qmdb_free(h.ptr)
		h.ptr = nil
	}
}

func (h *QmdbHandle) StartBlock(height int64, tasksManager *QmdbTasksManager) error {
	result := C.qmdb_start_block(h.ptr, C.int64_t(height), tasksManager.ptr)
	if result != 0 {
		return fmt.Errorf("failed to start block")
	}
	return nil
}

func (h *QmdbHandle) GetShared() *QmdbSharedHandle {
	ptr := C.qmdb_get_shared(h.ptr)
	if ptr == nil {
		return nil
	}
	return &QmdbSharedHandle{ptr: ptr}
}

func (h *QmdbHandle) Flush() error {
	result := C.qmdb_flush(h.ptr)
	if result != 0 {
		return fmt.Errorf("failed to flush")
	}
	return nil
}

// Shared handle functions
func (s *QmdbSharedHandle) Free() {
	if s.ptr != nil {
		C.qmdb_shared_free(s.ptr)
		s.ptr = nil
	}
}

func (s *QmdbSharedHandle) InsertExtraData(height int64, data string) error {
	cData := C.CString(data)
	defer C.free(unsafe.Pointer(cData))

	result := C.qmdb_shared_insert_extra_data(s.ptr, C.int64_t(height), cData)
	if result != 0 {
		return fmt.Errorf("failed to insert extra data")
	}
	return nil
}

func (s *QmdbSharedHandle) AddTask(taskId int64) {
	C.qmdb_shared_add_task(s.ptr, C.int64_t(taskId))
}

func (s *QmdbSharedHandle) ReadEntry(height int64, keyHash []byte, key []byte) ([]byte, bool, error) {
	buf := make([]byte, 4096) // DEFAULT_ENTRY_SIZE
	var found C.int

	var keyPtr *C.uint8_t
	if len(key) > 0 {
		keyPtr = (*C.uint8_t)(unsafe.Pointer(&key[0]))
	}

	size := C.qmdb_shared_read_entry(
		s.ptr,
		C.int64_t(height),
		(*C.uint8_t)(unsafe.Pointer(&keyHash[0])),
		C.size_t(len(keyHash)),
		keyPtr,
		C.size_t(len(key)),
		(*C.uint8_t)(unsafe.Pointer(&buf[0])),
		C.size_t(len(buf)),
		&found,
	)

	return buf[:size], found != 0, nil
}

// ChangeSet functions
func NewChangeSet() *QmdbChangeSet {
	ptr := C.qmdb_changeset_new()
	return &QmdbChangeSet{ptr: ptr}
}

func (c *QmdbChangeSet) Free() {
	if c.ptr != nil {
		C.qmdb_changeset_free(c.ptr)
		c.ptr = nil
	}
}

func (c *QmdbChangeSet) AddOp(opType uint8, shardId uint8, keyHash []byte, key []byte, value []byte) error {
	var keyPtr *C.uint8_t
	if len(key) > 0 {
		keyPtr = (*C.uint8_t)(unsafe.Pointer(&key[0]))
	}

	var valuePtr *C.uint8_t
	if len(value) > 0 {
		valuePtr = (*C.uint8_t)(unsafe.Pointer(&value[0]))
	}

	result := C.qmdb_changeset_add_op(
		c.ptr,
		C.uint8_t(opType),
		C.uint8_t(shardId),
		(*C.uint8_t)(unsafe.Pointer(&keyHash[0])),
		C.size_t(len(keyHash)),
		keyPtr,
		C.size_t(len(key)),
		valuePtr,
		C.size_t(len(value)),
	)

	if result != 0 {
		return fmt.Errorf("failed to add operation")
	}
	return nil
}

func (c *QmdbChangeSet) Sort() {
	C.qmdb_changeset_sort(c.ptr)
}

func NewTasksManager(changeSets []*QmdbChangeSet, lastTaskId int64) (*QmdbTasksManager, error) {
	if len(changeSets) == 0 {
		return nil, fmt.Errorf("empty changeset list")
	}

	ptrs := make([]unsafe.Pointer, len(changeSets))
	for i, cs := range changeSets {
		ptrs[i] = cs.ptr
	}

	ptr := C.qmdb_tasks_manager_new(
		(*unsafe.Pointer)(&ptrs[0]),
		C.size_t(len(changeSets)),
		C.int64_t(lastTaskId),
	)

	if ptr == nil {
		return nil, fmt.Errorf("failed to create tasks manager")
	}

	return &QmdbTasksManager{ptr: ptr}, nil
}

func (t *QmdbTasksManager) Free() {
	if t.ptr != nil {
		C.qmdb_tasks_manager_free(t.ptr)
		t.ptr = nil
	}
}

// Utility functions
func Hash(input []byte) ([32]byte, error) {
	var output [32]byte
	result := C.qmdb_hash(
		(*C.uint8_t)(unsafe.Pointer(&input[0])),
		C.size_t(len(input)),
		(*C.uint8_t)(unsafe.Pointer(&output[0])),
	)

	if result != 0 {
		return output, fmt.Errorf("failed to hash")
	}
	return output, nil
}

func Byte0ToShardId(byte0 uint8) uint {
	return uint(C.qmdb_byte0_to_shard_id(C.uint8_t(byte0)))
}

