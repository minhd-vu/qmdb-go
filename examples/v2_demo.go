package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"strings"

	"github.com/minhd-vu/qmdb-go"
)

func main() {
	adsDir := "ADS"

	if err := qmdb.InitDir(adsDir); err != nil {
		log.Fatalf("Failed to init dir: %v", err)
	}

	ads, err := qmdb.New(adsDir)
	if err != nil {
		log.Fatalf("Failed to create QMDB: %v", err)
	}
	defer ads.Free()

	var changeSets []*qmdb.QmdbChangeSet

	for i := 0; i < 10; i++ {
		for j := 0; j < 2; j++ {
			cset := qmdb.NewChangeSet()

			var k [32]byte
			var v [32]byte
			k[0] = byte(i)
			k[1] = byte(j)

			for n := 0; n < 5; n++ {
				k[2] = byte(n)
				v[0] = byte(n)
				for idx := 1; idx < 32; idx++ {
					v[idx] = 1
				}

				kh, err := qmdb.Hash(k[:])
				if err != nil {
					log.Fatalf("Failed to hash key: %v", err)
				}

				shardId := uint8(qmdb.Byte0ToShardId(kh[0]))

				if err := cset.AddOp(qmdb.OpCreate, shardId, kh[:], k[:], v[:]); err != nil {
					log.Fatalf("Failed to add operation: %v", err)
				}
			}

			cset.Sort()
			changeSets = append(changeSets, cset)
		}
	}

	height := int64(1)
	taskCount := int64(len(changeSets))
	lastTaskId := (height << qmdb.InBlockIdxBits) | (taskCount - 1)

	tasksManager, err := qmdb.NewTasksManager(changeSets, lastTaskId)
	if err != nil {
		log.Fatalf("Failed to create tasks manager: %v", err)
	}

	if err := ads.StartBlock(height, tasksManager); err != nil {
		log.Fatalf("Failed to start block: %v", err)
	}

	sharedAds := ads.GetShared()
	defer sharedAds.Free()

	if err := sharedAds.InsertExtraData(height, ""); err != nil {
		log.Fatalf("Failed to insert extra data: %v", err)
	}

	for idx := int64(0); idx < taskCount; idx++ {
		taskId := (height << qmdb.InBlockIdxBits) | idx
		sharedAds.AddTask(taskId)
	}

	if err := ads.Flush(); err != nil {
		log.Fatalf("Failed to flush: %v", err)
	}

	var k [32]byte
	k[0] = 1
	k[1] = 1
	k[2] = 3

	kh, err := qmdb.Hash(k[:])
	if err != nil {
		log.Fatalf("Failed to hash key for read: %v", err)
	}

	buf, found, err := sharedAds.ReadEntry(-1, kh[:], []byte{})
	if err != nil {
		log.Fatalf("Failed to read entry: %v", err)
	}

	first32 := binary.LittleEndian.Uint32(buf[:4])
	keyLen := int(first32 & 0xff)
	valueLen := int(first32 >> 8)
	value := buf[5+keyLen : 5+keyLen+valueLen]

	entryStr := strings.Replace(fmt.Sprintf("%v", buf), " ", ", ", -1)
	valueStr := strings.Replace(fmt.Sprintf("%v", value), " ", ", ", -1)

	fmt.Printf("entry=%s value=%s ok=%t\n", entryStr, valueStr, found)
}
