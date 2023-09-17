package lotusLib

import (
	"log"
//	"fmt"
	"testing"
	"os"
    "math/rand"
    "time"
)

func TestDb(t *testing.T) {

	db, err := InitDb("testLotusDb", "LotusDbDat", false)
	if err != nil {t.Errorf("error -- could not initialise Db: %v", err)}

	PrintDb(db)
	err = db.Close()
	if err != nil {t.Errorf("error -- could not close Db: %v", err)}

}

func TestSaveOpt (t *testing.T) {

	dirPath := "testLotusDb"
	err := os.RemoveAll(dirPath)
	if err != nil {t.Errorf("error -- could not remove files: %v", err)}

	db, err := InitDb(dirPath, "LotusDbDat", false)
	if err != nil {t.Errorf("error -- could not initialise Db: %v", err)}

	
	err = db.SaveOption("config.yaml")
	if err != nil {t.Errorf("error -- could not save Config Option: %v", err)}

	err = db.LoadOption("config.yaml")
	if err != nil {t.Errorf("error -- could not load Config Option: %v", err)}

	PrintDb(db)

	err = db.Close()
	if err != nil {t.Errorf("error -- could not close Db: %v", err)}

}


func TestAddEntry(t *testing.T) {

	_, err := os.Stat("testDb")
	if err == nil {
		err1 := os.RemoveAll("testDb")
		if err1 != nil {t.Errorf("error -- could not remove files: %v", err1)}
	}

	db, err := InitDb("testLotusDb", "LotusDbDat", false)
	if err != nil {t.Errorf("error -- could not initialise Db: %v", err)}

	err = db.AddEntry("key1", "val1")
	if err != nil {t.Errorf("error -- AddEntry: %v", err)}

	res, err := db.FindKey("key1")
	if err != nil {t.Errorf("error -- FindKey: %v", err)}
	if !res {t.Errorf("error -- key \"key1\" not found!")}

	res, err = db.FindKey("key2")
	if err != nil {t.Errorf("error -- FindKey: %v", err)}
	if res {t.Errorf("error -- non-existent key \"key2\" found!")}

	valdat, err := db.GetVal("key1")
	if err != nil {t.Errorf("error -- GetVal for \"key1\": %v", err)}

	if string(valdat) != "val1" {t.Errorf("values for \"key1\" do not agree: %s is not %s!", string(valdat), "val1")}

	valdat, err = db.GetVal("key2")
	if err == nil {t.Errorf("error -- GetVal for key \"key2\": %s", string(valdat))}
	if len(valdat) > 0 {t.Errorf("error -- length of valdat[%d] >0",len(valdat))}

	err = db.Close()
	if err != nil {t.Errorf("error -- could not close Db: %v", err)}

}


func TestUpdEntry(t *testing.T) {

	db, err := InitDb("testLotusDb", "LotusDbDat", false)
	if err != nil {t.Errorf("error -- could not initialise Db: %v", err)}

	valstr, err := db.GetVal("key1")
	if err != nil {t.Errorf("error -- GetVal for \"key1\": %v", err)}

	if valstr != "val1" {t.Errorf("values for \"key1\" do not agree: %s is not %s!", valstr, "val1")}

	err = db.UpdEntry("key1", "nval1")
	if err != nil {t.Errorf("error -- UpdEntry for \"key1\": %v", err)}

	nvalstr, err := db.GetVal("key1")
	if err != nil {t.Errorf("error -- GetVal for \"key1\": %v", err)}
	if nvalstr != "nval1" {t.Errorf("error - values do not match: %s %s",nvalstr, "nval1")}


	err = db.Close()
	if err != nil {t.Errorf("error -- could not close Db: %v", err)}

}


func TestDelEntry(t *testing.T) {

	db, err := InitDb("testLotusDb", "LotusDbDat", false)
	if err != nil {t.Errorf("error -- could not initialise Db: %v", err)}

	err = db.AddEntry("key1", "val1")
	if err != nil {t.Errorf("error -- AddEntry: %v", err)}

	res, err := db.FindKey("key1")
	if err!=nil {t.Errorf("error -- FindKey: %v", err)}
	if !res {t.Errorf("error -- FindKey: \"key1\" not found!")}

	err = db.DelEntry("key1")
	if err != nil {t.Errorf("error -- DelEntry: %v", err)}

	res, err = db.FindKey("key1")
	if err!=nil {t.Errorf("error -- FindKey: %v", err)}
	if res {t.Errorf("error -- FindKey: \"key1\" found! Should not exist!")}

	err = db.Close()
	if err != nil {t.Errorf("error -- could not close Db: %v", err)}

}


func TestBckupAndLoad(t *testing.T) {

	_, err := os.Stat("testLotusDb")
	if err == nil {
		err1 := os.RemoveAll("testLotusDb")
		if err1 != nil {t.Errorf("error -- could not remove files: %v", err1)}
	}

	db, err := InitDb("testLotusDb", "LotusDbDat", false)
	if err != nil {t.Errorf("error -- could not initialise Db: %v", err)}

	keys, vals, err := db.FillRan(5)
	if err != nil {t.Errorf("error -- FillRan: %v", err)}

	err = db.Backup()
	if err != nil {t.Errorf("error -- Backup: %v", err)}

	err = db.Close()
	if err != nil {t.Errorf("error -- could not close Db: %v", err)}

	db, err = InitDb("testLotusDb", "LotusDbDat", false)
	if err != nil {t.Errorf("error -- could not initialise Db: %v", err)}

	valstr, err := db.GetVal(keys[0])
	if err != nil {t.Errorf("error -- GetVal for \"key1\": %v", err)}
	if valstr != vals[0] {t.Errorf("error - values do not match: %s %s",vals[0], valstr)}

	err = db.Close()
	if err != nil {t.Errorf("error -- could not close Db: %v", err)}

}


func TestGet(t *testing.T) {

	var seededRand = rand.New(rand.NewSource(time.Now().UnixNano()))

//	os.RemoveAll("testDb")
	numEntries := 100
	_, err := os.Stat("testLotusDb")
	if err == nil {
		err1 := os.RemoveAll("testLotusDb")
		if err1 != nil {t.Errorf("error -- could not remove files: %v", err1)}
	}

	db, err := InitDb("testLotusDb", "LotusDbDat", false)
	if err != nil {t.Errorf("error -- could not initialise Db: %v", err)}

    keyList, valList, err := db.FillRan(numEntries)
    if err != nil {t.Errorf("error -- FillRan: %v", err)}

	kidx := seededRand.Intn(numEntries)
	keyStr := keyList[kidx]
	valstr, err := db.GetVal(keyStr)
	if err != nil {t.Errorf("invalid keyStr!")}

	if valstr != valList[kidx]  {t.Errorf("values do not agree: %s is not %s!", valstr, valList[kidx])}

}


func BenchmarkGet(b *testing.B) {

	var seededRand = rand.New(rand.NewSource(time.Now().UnixNano()))

	numEntries := 100
	_, err := os.Stat("testLotusDb")
	if err == nil {
		err1 := os.RemoveAll("testLotusDb")
		if err1 != nil {log.Fatalf("error -- could not remove files: %v", err1)}
	}

	db, err := InitDb("testLotusDb", "LotusDbDat", false)
	if err != nil {log.Fatalf("error -- could not initialise Db: %v", err)}

    keyList, valList, err := db.FillRan(numEntries)
    if err != nil {log.Fatalf("error -- FillRan: %v", err)}

//    err = kv.Backup("testDbNew_Backup.dat")
//    if err != nil {log.Fatalf("error -- Backup: %v", err)}

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		kidx := seededRand.Intn(numEntries)
		keyStr := keyList[kidx]
		valstr, err := db.GetVal(keyStr)
		if err != nil {log.Fatalf("GetVal err invalid keyStr!")}
		if len(valstr) < 1 {log.Fatalf("invalid valstr!")}
		if valstr != valList[kidx]  {log.Fatalf("values do not agree[%d]: %s is not %s!", n, valstr, valList[kidx])}
	}
}


