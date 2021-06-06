package datastore

import (
  "fmt"
  "io"
  "io/ioutil"
  "os"
  "path/filepath"
  "strconv"
  "strings"
)

//const defaultSegment int64 = (1 << 20) * 10
const segmentPrefix = "segment"

var ErrNotFound = fmt.Errorf("record does not exist")

type Db struct {
  dirPath  string
  segments []*segment
  segSize  int64
}

func NewDb(dir string, segmentSize int64) (*Db, error) {
  db := &Db{
    dirPath:  dir,
    segments: nil,
    segSize:  segmentSize,
  }
  err := db.init()
  if err != nil && err != io.EOF {
    return nil, err
  }
  return db, nil
}

func (db *Db) init() error {
  contents, err := ioutil.ReadDir(db.dirPath)
  if err != nil {
    return err
  }
  var segments []*segment
  for _, file := range contents {
    if !file.IsDir() && strings.HasPrefix(file.Name(), segmentPrefix) {
      segment, err := initSegment(filepath.Join(db.dirPath, file.Name()))
      if err != nil {
        return err
      }

      segments = append(segments, segment)
    }
  }

  if len(segments) == 0 {
    path := filepath.Join(db.dirPath, segmentPrefix+"0")
    file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
    if err != nil {
      return err
    }

    segments = append(segments, &segment{
      filePath:  path,
      file:      file,
      outOffset: 0,
      index:     make(hashIndex),
    })
  }
  db.segments = segments
  return err
}

func (db *Db) Close() error {
  for _, seg := range db.segments {
    err := seg.close()
    if err != nil {
      return err
    }
  }
  return nil
}

func (db *Db) Get(key string) (string, error) {
  for _, seg := range db.segments {
    value, err := seg.get(key)
    if err == nil {
      return value, err
    }
  }

  return "", fmt.Errorf("not found any value at the key: %s", key)
}

func (db *Db) Put(key, value string) error {
  err := db.last().put(key, value)
  if err != nil {
    return err
  }

  if db.last().outOffset >= db.segSize {
    err := db.createSegment()
    if err != nil {
      return err
    }
  }
  return nil
}

func (db *Db) last() *segment {
  return db.segments[len(db.segments)-1]
}

func (db *Db) createSegment() error {
  last := db.last()

  name := last.file.Name()
  count, _ := strconv.Atoi(name[len(name)-1:])
  path := filepath.Join(db.dirPath, fmt.Sprintf("%s%d", segmentPrefix, count+1))

  seg, err := initSegment(path)
  if err != nil {
    return err
  }
  db.segments = append(db.segments, seg)

  if len(db.segments) > 2 {
    return db.merge()
  }

  return nil
}

func (db *Db) merge() error {
  mergees := db.segments[0 : len(db.segments)-1]
  newPath := filepath.Join(db.dirPath, segmentPrefix+"-merged")

  file, err := os.OpenFile(newPath, os.O_WRONLY|os.O_CREATE, 0o600)
  if err != nil {
    return err
  }

  mergedSeg := &segment{
    filePath:  newPath,
    file:      file,
    outOffset: 0,
    index:     make(hashIndex),
  }

  for i := len(mergees) - 1; i >= 0; i-- {
    mergee := mergees[i]
    for key := range mergee.index {
      if _, exists := mergedSeg.index[key]; exists {
        continue
      }

      value, err := mergee.get(key)
      if err != nil {
        _ = mergedSeg.close()
        _ = os.Remove(newPath)
        return err
      }

      err = mergedSeg.put(key, value)
      if err != nil {
        _ = mergedSeg.close()
        _ = os.Remove(newPath)
        return err
      }
    }
  }

  db.segments = []*segment{mergedSeg, db.last()}
  for _, segment := range mergees {
    _ = segment.close()
    _ = os.Remove(segment.filePath)
  }
  return nil
}