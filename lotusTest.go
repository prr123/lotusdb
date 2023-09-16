// https://github.com/lotusdblabs/lotusdb



package main

import (
	"log"
	"github.com/lotusdblabs/lotusdb/v2"
)

func main() {
	// Set Options
	options := lotusdb.DefaultOptions
	options.DirPath = "db/lotusdbTest"

	// Open LotusDB
	db, err := lotusdb.Open(options)
	if err != nil { log.Fatalf("error -- Open: %v", err)}
	defer func() {
		err = db.Close()
		if err != nil { log.Fatalf("error -- Close: %v", err)}
	}()

	// Put Key-Value
	key := []byte("Key1")
	valStr := []byte("Val1")
	putOptions := &lotusdb.WriteOptions{
		Sync:       true,
		DisableWal: false,
	}
	err = db.Put(key, valStr, putOptions)
	if err != nil { log.Fatalf("error -- db.Put: %v", err)}

	// Get Key-Value
	rdVal, err := db.Get(key)
	if err != nil { log.Fatalf("error -- db.Get: %v", err)}
	if string(rdVal) != string(valStr) {log.Fatalf("error -- stored value: %s not equal to retrieved value: %s!\n", valStr, rdVal)}
	log.Printf("val for key[%s] match: %s expecting %s\n",key, valStr, rdVal)

	// Delete Key-Value
	err = db.Delete(key, putOptions)
	if err != nil { log.Fatalf("error -- db.Delete: %v", err)}
	rdVal, err = db.Get(key)
	if err == nil {log.Fatalf("error - key found in db!\n")}
	log.Printf("rdval after delete:[%d] %s\n", len(rdVal), rdVal)

	// Start Compaction of Value Log
	err = db.Compact()
	if err != nil { log.Fatalf("error -- db.Compact: %v", err)}

	log.Printf("lotusdb -- test success\n")
}
