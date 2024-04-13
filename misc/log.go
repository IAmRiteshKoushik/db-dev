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
