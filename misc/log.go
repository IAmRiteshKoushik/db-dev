// Using append only logs for persisting data
package misc

import "os"

func LogCreate(path string) (*os.File, error) {
    // Open or create if does not exist
    return os.OpenFile(path, os.O_RDWR | os.O_CREATE, 0644)
}

func LogAppend(fp *os.File, line string) error {
    buf := []byte(line)
    buf = append(buf, '\n')
    _, err := fp.Write(buf)
    if err != nil {
        return err
    }
    return fp.Sync() // fsync
}

// Problems :
// 1. It does not modify existing data
// 2. A databases uses additional "indexes" to query data efficiently. There 
//    are brute-force ways to query a bunch of records in arbitrary order.
// 3. Deleting data is a mess as logs end up growing forever.
