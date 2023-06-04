package datastore

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDb_Put(t *testing.T) {
	dir, err := ioutil.TempDir("", "test-db")
	dirInt64, err := ioutil.TempDir("", "test-db-Int64")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := NewDb(dir, 45)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	dbInt64, err := NewDb(dirInt64, 45)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	pairs := [][]string{
		{"1", "a"},
		{"2", "b"},
		{"3", "c"},
	}

	type int64Data struct {
		key   string
		value int64
	}

	pairsInt64 := []int64Data{
		{"6", int64(123)},
		{"7", int64(234)},
	}

	outFile, err := os.Open(filepath.Join(dir, outFileName+"0"))
	if err != nil {
		t.Fatal(err)
	}

	t.Run("put/get int64", func(t *testing.T) {
		for _, pair := range pairsInt64 {
			err := dbInt64.PutInt64(pair.key, pair.value)
			if err != nil {
				t.Errorf("Cannot put %s: %s", pair.key, err)
			}
			value, err := dbInt64.GetInt64(pair.key)
			if err != nil {
				t.Errorf("Cannot get %s: %s", pair.key, err)
			}
			if value != pair.value {
				t.Errorf("Bad value returned expected %d, got %d", pair.value, value)
			}
		}
	})

	t.Run("put/get", func(t *testing.T) {
		for _, pair := range pairs {
			err := db.Put(pair[0], pair[1])
			if err != nil {
				t.Errorf("Cannot put %s: %s", pairs[0], err)
			}
			value, err := db.Get(pair[0])
			if err != nil {
				t.Errorf("Cannot get %s: %s", pairs[0], err)
			}
			if value != pair[1] {
				t.Errorf("Bad value returned expected %s, got %s", pair[1], value)
			}
		}
	})

	outInfo, err := outFile.Stat()
	if err != nil {
		t.Fatal(err)
	}
	size1 := outInfo.Size()

	t.Run("file growth", func(t *testing.T) {
		for _, pair := range pairs {
			err := db.Put(pair[0], pair[1])
			if err != nil {
				t.Errorf("Cannot put %s: %s", pairs[0], err)
			}
		}
		outInfo, err := outFile.Stat()
		if err != nil {
			t.Fatal(err)
		}
		if size1 != outInfo.Size() {
			t.Errorf("Unexpected size (%d vs %d)", size1, outInfo.Size())
		}
	})

	t.Run("new db process", func(t *testing.T) {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		db, err = NewDb(dir, 45)
		if err != nil {
			t.Fatal(err)
		}

		for _, pair := range pairs {
			value, err := db.Get(pair[0])
			if err != nil {
				t.Errorf("Cannot get %s: %s", pair, err)
			}
			if value != pair[1] {
				t.Errorf("Bad value returned expected %s, got %s", pair[1], value)
			}
		}
	})
}

func TestDb_Segmentation(t *testing.T) {
	dir, err := ioutil.TempDir("", "test-db")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := NewDb(dir, 35)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	t.Run("new file", func(t *testing.T) {
		db.Put("1", "a")
		db.Put("2", "b")
		db.Put("3", "c")
		db.Put("2", "e")

		actual := len(db.segments)
		if actual != 2 {
			t.Errorf("Bad segmentation. Expected 2 files, but received %d.", len(db.segments))
		}
	})

	t.Run("starting segmentation", func(t *testing.T) {
		db.Put("4", "44")
		actual := len(db.segments)

		if actual != 3 {
			t.Errorf("Bad segmentation. Expected 3 files, but received %d.", len(db.segments))
		}

		time.Sleep(2 * time.Second)

		actual = len(db.segments)
		if actual != 2 {
			t.Errorf("Bad segmentation. Expected 2 files, but received %d.", len(db.segments))
		}
	})

	t.Run("new values", func(t *testing.T) {
		actual, _ := db.Get("2")

		if actual != "e" {
			t.Errorf("Bad segmentation. Expected value: e, Actual one: %s", actual)
		}
	})

	t.Run("check size", func(t *testing.T) {
		file, err := os.Open(db.segments[0].filePath)
		defer file.Close()

		if err != nil {
			t.Error(err)
		}
		inf, _ := file.Stat()
		actual := inf.Size()

		if actual != int64(45) {
			t.Errorf("Bad segmentation. Expected size %d, Actual one: %d", int64(45), actual)
		}
	})
}
