package bbolt_test

import (
	"bytes"
	"encoding/binary"
	"reflect"
	"strconv"
	"testing"

	bolt "go.etcd.io/bbolt"
)

const NUM_OF_KEYS = 16777216

const (
	dataBucket = "data"
	dbName     = "data.db"
)

func TestCursor_InsertData(t *testing.T) {
	insertData()
}

func insertData() error {
	db, err := bolt.Open(dbName, 0600, nil)
	var insertedCount int
	db.Update(func(tx *bolt.Tx) error {
		bucket, _ := tx.CreateBucketIfNotExists([]byte(dataBucket))
		c := bucket.Cursor()
		k, _ := c.Last()
		if len(k) == 0 {
			insertedCount = NUM_OF_KEYS
			return nil
		}

		uInsertedCount, _ := binary.ReadUvarint(bytes.NewBuffer(k))
		insertedCount = int(uInsertedCount)
		return nil
	})
	insertedCount = NUM_OF_KEYS - (NUM_OF_KEYS - insertedCount)
	for j := insertedCount; j > 0; j -= 10000 {
		err = db.Update(func(tx *bolt.Tx) error {
			bucket := tx.Bucket([]byte(dataBucket))
			for i := j; i <= j+10000; i++ {
				key, n := make([]byte, 8), []byte{} // Pre-allocate a byte slice with size 8 for the key

				binary.BigEndian.PutUint64(key, uint64(i))
				n = bytes.TrimLeft(key, "\x00")
				value := []byte(strconv.Itoa(i))
				if err != nil {
					return err
				}
				bucket.Put(n, value)
			}
			return err
		})
	}

	return err
}

func TestCursor_SeekReverse(t *testing.T) {
	db, _ := bolt.Open(dbName, 0600, nil)

	type args struct {
		prefix []byte
	}
	tests := []struct {
		name  string
		args  args
		wantK []byte
		wantV []byte
	}{
		struct {
			name  string
			args  args
			wantK []byte
			wantV []byte
		}{
			name: "find last with aaa prefix",
			args: args{
				prefix: []byte("aaa"),
			},
			wantK: []byte{97, 97, 97},
			wantV: []byte{54, 51, 56, 49, 57, 50, 49},
		},
		struct {
			name  string
			args  args
			wantK []byte
			wantV []byte
		}{
			name: "find last with aa prefix",
			args: args{
				prefix: []byte("aa"),
			},
			wantK: []byte{97, 97, 255},
			wantV: []byte{54, 51, 56, 50, 48, 55, 57},
		},
		struct {
			name  string
			args  args
			wantK []byte
			wantV []byte
		}{
			name: "find last with a prefix",
			args: args{
				prefix: []byte("a"),
			},
			wantK: []byte{97, 255, 255},
			wantV: []byte{54, 52, 50, 50, 53, 50, 55},
		},
	}
	for _, tt := range tests {
		db.View(func(tx *bolt.Tx) error {
			t.Run(tt.name, func(t *testing.T) {
				b := tx.Bucket([]byte(dataBucket))
				c := b.Cursor()
				gotK, gotV := c.SeekReverse(tt.args.prefix)
				if !reflect.DeepEqual(gotK, tt.wantK) {
					t.Errorf("Cursor.SeekReverseSlow() gotK = %v, want %v", gotK, tt.wantK)
					t.Errorf("Cursor.SeekReverseSlow() gotK = %s, want %s", gotK, tt.wantK)

				}
				if !reflect.DeepEqual(gotV, tt.wantV) {
					t.Errorf("Cursor.SeekReverseSlow() gotV = %v, want %v", gotV, tt.wantV)
					t.Errorf("Cursor.SeekReverseSlow() gotV = %s, want %s", gotV, tt.wantV)

				}
			})
			return nil
		})
	}

}

func TestCursor_SeekReverse_Large(t *testing.T) {
	db, _ := bolt.Open(dbName, 0600, nil)

	type args struct {
		prefix []byte
	}
	tests := []struct {
		name  string
		args  args
		wantK []byte
		wantV []byte
	}{
		struct {
			name  string
			args  args
			wantK []byte
			wantV []byte
		}{
			name: "find last with aaa prefix",
			args: args{
				prefix: []byte("aaa"),
			},
			wantK: []byte{97, 97, 97},
			wantV: []byte{54, 51, 56, 49, 57, 50, 49},
		},
		struct {
			name  string
			args  args
			wantK []byte
			wantV []byte
		}{
			name: "find last with aa prefix",
			args: args{
				prefix: []byte("aa"),
			},
			wantK: []byte{97, 97, 255},
			wantV: []byte{54, 51, 56, 50, 48, 55, 57},
		},
		struct {
			name  string
			args  args
			wantK []byte
			wantV []byte
		}{
			name: "find last with a prefix",
			args: args{
				prefix: []byte("a"),
			},
			wantK: []byte{97, 255, 255},
			wantV: []byte{54, 52, 50, 50, 53, 50, 55},
		},
	}

	for _, tt := range tests {
		db.View(func(tx *bolt.Tx) error {
			t.Run(tt.name, func(t *testing.T) {
				c := tx.Bucket([]byte(dataBucket)).Cursor()
				prefix, highest := tt.args.prefix, tt.args.prefix
				if len(prefix) == 1 {
					highest = append(highest, 255)
					highest = append(highest, 255)

				} else if len(prefix) == 2 {
					highest = append(highest, 255)
				}
				gotK, gotV := c.SeekCustom(&highest, bolt.CompareTreeElements)

				if !reflect.DeepEqual(gotK, tt.wantK) {
					t.Errorf("Cursor.SeekReverseSlow() gotK = %v, want %v", gotK, tt.wantK)
					t.Errorf("Cursor.SeekReverseSlow() gotK = %s, want %s", gotK, tt.wantK)
				}
				if !reflect.DeepEqual(gotV, tt.wantV) {
					t.Errorf("Cursor.SeekReverseSlow() gotV = %v, want %v", gotV, tt.wantV)
					t.Errorf("Cursor.SeekReverseSlow() gotV = %s, want %s", gotV, tt.wantV)
				}
			})
			return nil
		})
	}
}

func TestCursor_SeekReverseSlow(t *testing.T) {
	db, _ := bolt.Open(dbName, 0600, nil)

	type args struct {
		prefix []byte
	}
	tests := []struct {
		name  string
		args  args
		wantK []byte
		wantV []byte
	}{
		struct {
			name  string
			args  args
			wantK []byte
			wantV []byte
		}{
			name: "find last with aaa prefix",
			args: args{
				prefix: []byte("aaa"),
			},
			wantK: []byte{97, 97, 97},
			wantV: []byte{54, 51, 56, 49, 57, 50, 49},
		},
		struct {
			name  string
			args  args
			wantK []byte
			wantV []byte
		}{
			name: "find last with aa prefix",
			args: args{
				prefix: []byte("aa"),
			},
			wantK: []byte{97, 97, 255},
			wantV: []byte{54, 51, 56, 50, 48, 55, 57},
		},
		struct {
			name  string
			args  args
			wantK []byte
			wantV []byte
		}{
			name: "find last with a prefix",
			args: args{
				prefix: []byte("a"),
			},
			wantK: []byte{97, 255, 255},
			wantV: []byte{54, 52, 50, 50, 53, 50, 55},
		},
	}
	for _, tt := range tests {
		db.View(func(tx *bolt.Tx) error {
			t.Run(tt.name, func(t *testing.T) {
				b := tx.Bucket([]byte(dataBucket))
				c := b.Cursor()
				gotK, gotV := c.SeekReverseSlow(tt.args.prefix)
				if !reflect.DeepEqual(gotK, tt.wantK) {
					t.Errorf("Cursor.SeekReverseSlow() gotK = %v, want %v", gotK, tt.wantK)
					t.Errorf("Cursor.SeekReverseSlow() gotK = %s, want %s", gotK, tt.wantK)

				}
				if !reflect.DeepEqual(gotV, tt.wantV) {
					t.Errorf("Cursor.SeekReverseSlow() gotV = %v, want %v", gotV, tt.wantV)
					t.Errorf("Cursor.SeekReverseSlow() gotV = %s, want %s", gotV, tt.wantV)

				}
			})
			return nil
		})
	}
}

// BenchmarkSeekReverseSlow benchmarks the c.SeekReverseSlow function.
func BenchmarkSeekReverseSlow(b *testing.B) {
	db, _ := bolt.Open(dbName, 0600, nil)
	defer db.Close()

	type args struct {
		prefix []byte
	}
	tests := []struct {
		name  string
		args  args
		wantK []byte
		wantV []byte
	}{
		struct {
			name  string
			args  args
			wantK []byte
			wantV []byte
		}{
			name: "find last with aaa prefix",
			args: args{
				prefix: []byte("aaa"),
			},
			wantK: []byte{97, 97, 97},
			wantV: []byte{54, 51, 56, 49, 57, 50, 49},
		},
		struct {
			name  string
			args  args
			wantK []byte
			wantV []byte
		}{
			name: "find last with aa prefix",
			args: args{
				prefix: []byte("aa"),
			},
			wantK: []byte{97, 97, 255},
			wantV: []byte{54, 51, 56, 50, 48, 55, 57},
		},
		struct {
			name  string
			args  args
			wantK []byte
			wantV []byte
		}{
			name: "find last with a prefix",
			args: args{
				prefix: []byte("a"),
			},
			wantK: []byte{97, 255, 255},
			wantV: []byte{54, 52, 50, 50, 53, 50, 55},
		},
		struct {
			name  string
			args  args
			wantK []byte
			wantV []byte
		}{
			name: "find last with a prefix",
			args: args{
				prefix: []byte{255},
			},
			wantK: []byte{97, 255, 255},
			wantV: []byte{54, 52, 50, 50, 53, 50, 55},
		},
		struct {
			name  string
			args  args
			wantK []byte
			wantV []byte
		}{
			name: "find last with a prefix",
			args: args{
				prefix: []byte{255, 255},
			},
			wantK: []byte{97, 255, 255},
			wantV: []byte{54, 52, 50, 50, 53, 50, 55},
		},
		struct {
			name  string
			args  args
			wantK []byte
			wantV []byte
		}{
			name: "find last with a prefix",
			args: args{
				prefix: []byte{255, 255, 255},
			},
			wantK: []byte{97, 255, 255},
			wantV: []byte{54, 52, 50, 50, 53, 50, 55},
		},
	}

	db.View(func(tx *bolt.Tx) error {
		bk := tx.Bucket([]byte(dataBucket))
		c := bk.Cursor()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {

			for _, tt := range tests {

				c.SeekReverseSlow(tt.args.prefix)
			}
		}

		return nil
	})

}

// BenchmarkSeekReverse benchmarks the c.SeekReverse function.
func BenchmarkSeekReverse(b *testing.B) {
	db, _ := bolt.Open(dbName, 0600, nil)
	defer db.Close()

	type args struct {
		prefix []byte
	}
	tests := []struct {
		name  string
		args  args
		wantK []byte
		wantV []byte
	}{
		struct {
			name  string
			args  args
			wantK []byte
			wantV []byte
		}{
			name: "find last with aaa prefix",
			args: args{
				prefix: []byte("aaa"),
			},
			wantK: []byte{97, 97, 97},
			wantV: []byte{54, 51, 56, 49, 57, 50, 49},
		},
		struct {
			name  string
			args  args
			wantK []byte
			wantV []byte
		}{
			name: "find last with aa prefix",
			args: args{
				prefix: []byte("aa"),
			},
			wantK: []byte{97, 97, 255},
			wantV: []byte{54, 51, 56, 50, 48, 55, 57},
		},
		struct {
			name  string
			args  args
			wantK []byte
			wantV []byte
		}{
			name: "find last with a prefix",
			args: args{
				prefix: []byte("a"),
			},
			wantK: []byte{97, 255, 255},
			wantV: []byte{54, 52, 50, 50, 53, 50, 55},
		},
		struct {
			name  string
			args  args
			wantK []byte
			wantV []byte
		}{
			name: "find last with a prefix",
			args: args{
				prefix: []byte{255},
			},
			wantK: []byte{97, 255, 255},
			wantV: []byte{54, 52, 50, 50, 53, 50, 55},
		},
		struct {
			name  string
			args  args
			wantK []byte
			wantV []byte
		}{
			name: "find last with a prefix",
			args: args{
				prefix: []byte{255, 255},
			},
			wantK: []byte{97, 255, 255},
			wantV: []byte{54, 52, 50, 50, 53, 50, 55},
		},
		struct {
			name  string
			args  args
			wantK []byte
			wantV []byte
		}{
			name: "find last with a prefix",
			args: args{
				prefix: []byte{255, 255, 255},
			},
			wantK: []byte{97, 255, 255},
			wantV: []byte{54, 52, 50, 50, 53, 50, 55},
		},
	}

	db.View(func(tx *bolt.Tx) error {
		bk := tx.Bucket([]byte(dataBucket))
		c := bk.Cursor()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {

			for _, tt := range tests {

				c.SeekReverse(tt.args.prefix)
			}
		}

		return nil
	})

}

// BenchmarkSeek benchmarks the c.Seek function.
func BenchmarkSeek(b *testing.B) {
	db, _ := bolt.Open(dbName, 0600, nil)
	defer db.Close()

	type args struct {
		prefix []byte
	}
	tests := []struct {
		name  string
		args  args
		wantK []byte
		wantV []byte
	}{
		struct {
			name  string
			args  args
			wantK []byte
			wantV []byte
		}{
			name: "find last with aaa prefix",
			args: args{
				prefix: []byte("aaa"),
			},
			wantK: []byte{97, 97, 97},
			wantV: []byte{54, 51, 56, 49, 57, 50, 49},
		},
		struct {
			name  string
			args  args
			wantK []byte
			wantV []byte
		}{
			name: "find last with aa prefix",
			args: args{
				prefix: []byte("aa"),
			},
			wantK: []byte{97, 97, 255},
			wantV: []byte{54, 51, 56, 50, 48, 55, 57},
		},
		struct {
			name  string
			args  args
			wantK []byte
			wantV []byte
		}{
			name: "find last with a prefix",
			args: args{
				prefix: []byte("a"),
			},
			wantK: []byte{97, 255, 255},
			wantV: []byte{54, 52, 50, 50, 53, 50, 55},
		},
		struct {
			name  string
			args  args
			wantK []byte
			wantV []byte
		}{
			name: "find last with a prefix",
			args: args{
				prefix: []byte{255},
			},
			wantK: []byte{97, 255, 255},
			wantV: []byte{54, 52, 50, 50, 53, 50, 55},
		},
		struct {
			name  string
			args  args
			wantK []byte
			wantV []byte
		}{
			name: "find last with a prefix",
			args: args{
				prefix: []byte{255, 255},
			},
			wantK: []byte{97, 255, 255},
			wantV: []byte{54, 52, 50, 50, 53, 50, 55},
		},
		struct {
			name  string
			args  args
			wantK []byte
			wantV []byte
		}{
			name: "find last with a prefix",
			args: args{
				prefix: []byte{255, 255, 255},
			},
			wantK: []byte{97, 255, 255},
			wantV: []byte{54, 52, 50, 50, 53, 50, 55},
		},
	}

	db.View(func(tx *bolt.Tx) error {
		bk := tx.Bucket([]byte(dataBucket))
		c := bk.Cursor()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {

			for _, tt := range tests {

				c.Seek(tt.args.prefix)
			}
		}

		return nil
	})

}
