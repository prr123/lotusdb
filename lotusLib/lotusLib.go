// lotuslib
// library to simplify operaton on lotusdb
// Author: prr azulsoftware
// Date: 16 Sept 2023
// copyright (c) 2023 prr, azul software
//

package lotusLib

import (
	"fmt"
	"log"
	"math/rand"
	"time"
	"os"
//	"unsafe"
//	"sort"

	yaml "github.com/goccy/go-yaml"
	"github.com/lotusdblabs/lotusdb/v2"
//	"github.com/dgryski/go-t1ha"
)

type DBObj struct {
	DirPath string
	TabNam string
	Dbg bool
	Opt lotusdb.Options
	Batch lotusdb.BatchOptions
	Write lotusdb.WriteOptions
	IterOpt lotusdb.IteratorOptions
	Db *lotusdb.DB
}

type hash struct {
	Hash uint64
	Idx int
}

type LotusDbOption struct {
	// DirPath specifies the directory path where all the database files will be stored.
	DirPath string `yaml:"DirPath"`

	TabNam string `yam:"TableName"`

	// MemtableSize represents the maximum size in bytes for a memtable.
	// It means that each memtable will occupy so much memory.
	// Default value is 64MB.
//	MemtableSize uint32 `yaml:"MemoryTableSize"`
	MemtableSize string `yaml:"MemoryTableSize"`

	// MemtableNums represents maximum number of memtables to keep in memory before flushing.
	// Default value is 15.
	MemtableNums int `yaml:"MemTableNumber"`

	// BlockCache specifies the size of the block cache in number of bytes.
	// A block cache is used to store recently accessed data blocks, improving read performance.
	// If BlockCache is set to 0, no block cache will be used.
//	BlockCache uint32
	BlockCache string `yaml:"BlockCache"`

	// Sync is whether to synchronize writes through os buffer cache and down onto the actual disk.
	// Setting sync is required for durability of a single write operation, but also results in slower writes.
	//
	// If false, and the machine crashes, then some recent writes may be lost.
	// Note that if it is just the process that crashes (machine does not) then no writes will be lost.
	//
	// In other words, Sync being false has the same semantics as a write
	// system call. Sync being true means write followed by fsync.
	Sync bool `yaml:"Sync"`

	// BytesPerSync specifies the number of bytes to write before calling fsync.
//	BytesPerSync uint32
	BytesPerSyncStr string `yaml:"SyncSize"`

	// PartitionNum specifies the number of partitions to use for the index and value log.
	PartitionNum int `yaml:"Partitions"`

	// KeyHashFunction specifies the hash function for sharding.
	// It is used to determine which partition a key belongs to.
	// Default value is xxhash.
//	KeyHashFunction func([]byte) uint64

	// ValueLogFileSize size of a single value log file.
	// Default value is 1GB.
//	ValueLogFileSize int64
	ValueLogFileSize string `yaml:"VLogSize"`

	// indexType.
	// default value is bptree.
//	IndexType IndexType
	IndexType string `yaml:"IndexType"`


	// writing entries to disk after reading the specified number of entries.
	CompactBatchCount int `yaml:"CompactBatchCount"`

	// WaitMemSpaceTimeout specifies the timeout for waiting for space in the memtable.
	// When all memtables are full, it will be flushed to disk by the background goroutine.
	// But if the flush speed is slower than the write speed, there may be no space in the memtable.
	// So the write operation will wait for space in the memtable, and the timeout is specified by WaitMemSpaceTimeout.
	// If the timeout is exceeded, the write operation will fail, you can try again later.
	// Default value is 100ms.
	WaitMemSpaceTimeout time.Duration `yaml:"FlushWaitTime"`

	Batch BatchOpt `yaml:"Batch"`

	Write WriteOpt `yaml:"WriteOpt"`

	IterOpt IteratorOptions `yaml:"InterOpt"`
}

type BatchOpt struct {
	// Sync has the same semantics as Options.Sync.
	Sync bool `yaml:"Sync"`

	// ReadOnly specifies whether the batch is read only.
	ReadOnly bool `yaml:"ReadOnly"`
}

// WriteOptions set optional params for PutWithOptions and DeleteWithOptions.
// If you use Put and Delete (without options), that means to use the default values.
type WriteOpt struct {
	// Sync is whether to synchronize writes through os buffer cache and down onto the actual disk.
	// Setting sync is required for durability of a single write operation, but also results in slower writes.
	//
	// If false, and the machine crashes, then some recent writes may be lost.
	// Note that if it is just the process that crashes (machine does not) then no writes will be lost.
	//
	// In other words, Sync being false has the same semantics as a write
	// system call. Sync being true means write followed by fsync.

	// Default value is false.
	Sync bool `yaml:"Sync"`

	// DisableWal if true, writes will not first go to the write ahead log, and the write may get lost after a crash.
	// Setting true only if don`t care about the data loss.
	// Default value is false.
	DisableWal bool `yaml:"DisableWAL"`
}

// IteratorOptions is the options for the iterator.
type IteratorOptions struct {
	// Prefix filters the keys by prefix.
//	Prefix []byte
	Prefix string `yaml:"Prefix"`

	// Reverse indicates whether the iterator is reversed.
	// false is forward, true is backward.
	Reverse bool  `yaml:"Reverse"`
}




func InitDb(dirPath, tabNam string, dbg bool) (dbpt *DBObj, err error){

	db := DBObj {
		Opt: lotusdb.DefaultOptions,
		Dbg: dbg,
	}

	db.Opt.DirPath = dirPath + "/" +tabNam
	db.DirPath = dirPath
	db.TabNam = tabNam

	options := db.Opt

    ldb, err := lotusdb.Open(options)
    if err != nil { log.Fatalf("lotusdb.Open: %v", err)}
	db.Db = ldb

	dbp := &db

	return dbp, nil
}

func (dbpt *DBObj) Close () (err error){
	ldb := *dbpt.Db
	err = ldb.Close()
	return err
}

func (dbpt *DBObj) LoadOption (filNam string) (err error){

	yamlFilPath := (*dbpt).DirPath + "/" + filNam
//	log.Printf("yaml path: %s\n", yamlFilPath)

	yamlData, err := os.ReadFile(yamlFilPath)
	if err != nil {return fmt.Errorf("ReadFile %v\n", err)}

	optObj := LotusDbOption {}

	err = yaml.Unmarshal(yamlData, &optObj)
	if err != nil {return fmt.Errorf("UnMarshal %v\n", err)}

//	log.Printf("load optData: %v\n", optObj)

	opt := lotusdb.Options{}

	opt.DirPath = optObj.DirPath + "/" + optObj.TabNam

	var itmp uint32
	_, err = fmt.Sscanf(optObj.MemtableSize,"%d", &itmp)
	if err != nil {return fmt.Errorf("MemtableSize %v", err)}
	opt.MemtableSize = itmp
	opt.MemtableNums = optObj.MemtableNums

	_, err = fmt.Sscanf(optObj.BlockCache,"%d", &itmp)
	if err != nil {return fmt.Errorf("BlockCache %v", err)}
	opt.BlockCache = itmp

	opt.Sync = optObj.Sync

	_, err = fmt.Sscanf(optObj.BytesPerSyncStr,"%d", &itmp)
    if err != nil {return fmt.Errorf("BytesPerSync %v", err)}
    opt.BytesPerSync = itmp

	opt.PartitionNum = optObj.PartitionNum

	var itmp64 int64
	_, err = fmt.Sscanf(optObj.ValueLogFileSize,"%d", &itmp64)
    if err != nil {return fmt.Errorf("ValueLogFileSize %v", err)}
    opt.ValueLogFileSize = itmp64

//	opt.IndexType = optObj.IndexType

	opt.CompactBatchCount = optObj.CompactBatchCount

	(*dbpt).Opt = opt

//	opt.WaitMemSpaceTimeout = optObj.WaitMemSpaceTimeout

//	fmt.Printf("optObj.Batch: %v\n", optObj.Batch)
	(*dbpt).Batch.Sync = optObj.Batch.Sync
	(*dbpt).Batch.ReadOnly = optObj.Batch.ReadOnly

	(*dbpt).Write.Sync = optObj.Write.Sync
	(*dbpt).Write.DisableWal = optObj.Write.DisableWal

	(*dbpt).IterOpt.Prefix = []byte(optObj.IterOpt.Prefix)
	(*dbpt).IterOpt.Reverse = optObj.IterOpt.Reverse
	return nil
}

func (dbpt *DBObj) SaveOption (filNam string) (err error){

	opt := (*dbpt).Opt

	yamlFilPath := (*dbpt).DirPath + "/" + filNam
	//log.Printf("yaml path: %s\n", yamlFilPath)

	lotOpt := LotusDbOption{}

	lotOpt.DirPath = (*dbpt).DirPath
	lotOpt.TabNam = (*dbpt).TabNam

	// todo parse number to allow use of K, M and G for 1000, 1,000,000 and 1,000,000,000
	lotOpt.MemtableSize = fmt.Sprintf("%d",opt.MemtableSize)
	lotOpt.MemtableNums = opt.MemtableNums

	lotOpt.BlockCache = fmt.Sprintf("%d",opt.BlockCache)
	lotOpt.Sync = opt.Sync

	lotOpt.BytesPerSyncStr = fmt.Sprintf("%d",opt.BytesPerSync)
	lotOpt.PartitionNum = opt.PartitionNum

	lotOpt.ValueLogFileSize = fmt.Sprintf("%d",opt.ValueLogFileSize)
//	lotOpt.IndexType = fmt.Sprintf("%d",opt.IndexType)
	lotOpt.CompactBatchCount = opt.CompactBatchCount
//	lotOpt.WaitMemSpaceTimeout = opt.WaitMemSpaceTimeout

	lotOpt.Batch.Sync = (*dbpt).Batch.Sync
	lotOpt.Batch.ReadOnly = (*dbpt).Batch.ReadOnly

	lotOpt.Write.Sync = (*dbpt).Write.Sync
	lotOpt.Write.DisableWal = (*dbpt).Write.DisableWal

	lotOpt.IterOpt.Prefix = string((*dbpt).IterOpt.Prefix)
	lotOpt.IterOpt.Reverse = (*dbpt).IterOpt.Reverse


	optData, err := yaml.Marshal(lotOpt)
	if err != nil {return fmt.Errorf("Marshal %v\n", err)}

	//log.Printf("*** optData:\n%s\n", string(optData))

	err = os.WriteFile(yamlFilPath, optData, 0666)
	if err != nil {return fmt.Errorf("WriteFile %v\n", err)}

	return err
}



func (dbpt *DBObj) ValidateOpts() error {

	options := (*dbpt).Opt

//	if options.IndexType == Hash {
//		return errors.New("hash index is not supported yet")
//	}

	if options.DirPath == "" {
		return fmt.Errorf("the database directory path cannot be empty")
	}
	if options.MemtableSize <= 0 {
		log.Printf("MemtableSize is 0 resetting to 64\n")
		options.MemtableSize = 64 << 20 // 64MB
	}
	if options.MemtableNums <= 0 {
		log.Printf("MemtableNums is 0 resetting to 15\n")
		options.MemtableNums = 15
	}
	if options.PartitionNum <= 0 {
		log.Printf("PartitionNum is 0 resetting to 5\n")
		options.PartitionNum = 5
	}
	if options.ValueLogFileSize <= 0 {
		log.Printf("ValueLogFileSize is 0 resetting to 1GB\n")
		options.ValueLogFileSize = 1 << 30 // 1GB
	}
	return nil
}


func (dbpt *DBObj) FillRan (level int) (keyList, valList []string, err error){

	db := (*dbpt).Db

	keyList = make([]string, level)
	valList = make([]string, level)
	for i:=0; i<level; i++ {
		keydat := GenRanData(5, 25)
		valdat := GenRanData(5, 40)
		keyList[i] = string(keydat)
		valList[i] = string(valdat)
		err = db.Put(keydat, valdat, nil)
		if err != nil {return keyList, valList, fmt.Errorf("Put[%d] %v", level, err)}
	}
	return keyList, valList, nil
}


func (dbp *DBObj) AddEntry (key, val string) (err error){

	db := (*dbp).Db

	err = db.Put([]byte(key), []byte(val), nil)
	if err != nil {return fmt.Errorf("Put: %v", err)}

	return nil
}


func (dbp *DBObj) UpdEntry (key, val string) (err error){

    db := (*dbp).Db
	res, err := db.Exist([]byte(key))
	if err != nil {return fmt.Errorf("Exist: %v", err)}
	if !res {return fmt.Errorf("key %s does not exist!", key)}

    err = db.Put([]byte(key), []byte(val), nil)
    if err != nil {return fmt.Errorf("Put: %v", err)}

    return nil
}


func (dbp *DBObj) DelEntry (key string) (err error){

	db := (*dbp).Db
	// todo replace nil with write options
	err = db.Delete([]byte(key), nil)
	if err != nil {return fmt.Errorf("Put: %v", err)}

	return nil
}

func (dbp *DBObj) GetVal (key string) (valstr string, err error){

	db := (*dbp).Db
	val, err := db.Get([]byte(key))
	if err != nil {
//key not found in database
//		errStr := err.Error()
//		fmt.Printf("errStr: %s\n", errStr)
		return "", fmt.Errorf("Get: %v", err)}

	return string(val), nil
}

func (dbp *DBObj) FindKey (key string) (res bool, err error){

	db := (*dbp).Db
	res, err = db.Exist([]byte(key))
	if err != nil {return false, fmt.Errorf("Exist: %v", err)}

	return res, nil
}


func (dbp *DBObj) Backup() (err error){

	db := (*dbp).Db
	err = db.Sync()
	if err != nil {return fmt.Errorf("could not sync db: %v!", err)}
	return nil
}

/*
func (dbp *DBObj) Backup (tabNam string) (err error){

	return nil
}


func (dbp *DBObj) Load(tabNam string) (err error){
	var numEntries uint32

}

func (dbp *DBObj) SortHash(){

    db := *dbp
	num := (*db.Entries)
	hashList := (*db.HashList)[:num]
	for i:=0; i< len(hashList); i++ {
		fmt.Printf("%d hash: %d idx: %d\n", i,hashList[i].Hash, hashList[i].Idx) 
	}
	fmt.Println("***")
	sort.Slice(hashList, func(i, j int) bool {
		return hashList[i].Hash < hashList[j].Hash
	})

	for i:=0; i< len(hashList); i++ {
		fmt.Printf("%d hash: %d idx: %d\n", i,hashList[i].Hash, hashList[i].Idx) 
	}
	fmt.Println("***")

	dbp.HashList = &hashList
	dbp = &db
}
*/

func GenRanData (rangeStart, rangeEnd int) (bdat []byte) {

	var seededRand = rand.New(rand.NewSource(time.Now().UnixNano()))

//    rangeStart := 5
//    rangeEnd := 25
    offset := rangeEnd - rangeStart

    randLength := seededRand.Intn(offset) + rangeStart
    bdat = make([]byte, randLength)

    charset := "abcdefghijklmnopqrstuvw0123456789"
    for i := range bdat {
        bdat[i] = charset[seededRand.Intn(len(charset)-1)]
    }
	return bdat
}




func PrintDb(dbp *DBObj) {

    db := *dbp
//  dbg := db.Dbg
	opt := db.Opt

    fmt.Printf("******* LotusDb: %s *******\n", db.DirPath)
    fmt.Printf("Dir:    %s\n",db.DirPath)
    fmt.Printf("TabNam: %s\n",db.TabNam)
	fmt.Printf("Options:\n")
	fmt.Printf("  Dir Path:  %s\n", opt.DirPath)
	fmt.Printf("  MemtableSize: %d\n", opt.MemtableSize)
	fmt.Printf("  MemtableNums: %d\n", opt.MemtableNums)
	fmt.Printf("  BlockCache:   %d\n", opt.BlockCache)
	fmt.Printf("  Sync:         %t\n", opt.Sync)
	fmt.Printf("  BytesPerSync: %d\n", opt.BytesPerSync)
	fmt.Printf("  PartitionNum: %d\n", opt.PartitionNum)
//	fmt.Printf("  WaitMemSpaceTimeout: %s\n", opt.WaitMemSpaceTimeout)

	batch := db.Batch
	fmt.Printf("  Batch:\n")
	fmt.Printf("    Sync:       %t\n", batch.Sync)
	fmt.Printf("    ReadOnly:   %t\n", batch.ReadOnly)

	writeOpt := db.Write
	fmt.Printf("  Write:\n")
	fmt.Printf("    Sync:       %t\n", writeOpt.Sync)
	fmt.Printf("    DisableWal: %t\n", writeOpt.DisableWal)

	iterOpt := db.IterOpt
	fmt.Printf("  Iterator:\n")
	prefix := "-"
	if len(iterOpt.Prefix) >0 {prefix = string(iterOpt.Prefix)} 
    fmt.Printf("    Prefix:      %s\n", prefix)
    fmt.Printf("    Reverse:    %t\n", iterOpt.Reverse)

    fmt.Printf("********* End LotusDb *******\n")
    return
}

