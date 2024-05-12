package misc

import (
    "math/rand/v2"
	"fmt"
	"os"
)

// Approach 1
// This approach does not create directories but only files
// If the directories mentioned in the path are not created 
// already, then - no file or directory error is thrown
func SaveData1(path string, data []byte) error {
    fp, err := os.OpenFile(path, os.O_WRONLY | os.O_CREATE | os.O_TRUNC, 0664)
    if err != nil {
        return err
    }
    defer fp.Close()

    _, err = fp.Write(data)
    return err
}

// Random integer returned
func randomInt() int {
    return int(rand.Int64())
}

// Approach 2
// Better approach
// 1. Dump the content inside a temporary file
// 2. Rename the temporary file to the target file (rename is atomic be default)
func SaveData2(path string, data []byte) error {
    tmp := fmt.Sprintf("%s.tmp.%d", path, randomInt())
    fp, err := os.OpenFile(tmp, os.O_WRONLY | os.O_CREATE | os.O_EXCL, 0664)
    if err != nil {
        return err
    }
    defer fp.Close()

    _, err = fp.Write(data)
    if err != nil {
        os.Remove(tmp)
        return err
    }
    
    return os.Rename(tmp, path)
}

// Approach 3
func SaveData3(path string, data []byte) error {
    tmp := fmt.Sprintf("%s.tmp.%d", path, randomInt())
    fp, err := os.OpenFile(tmp, os.O_WRONLY | os.O_CREATE | os.O_EXCL, 0664)

    _, err = fp.Write(data)
    if err != nil {
        os.Remove(tmp)
        return err
    }
    // FSync system call is used to guarentee whether previously written data 
    // persists to the block device.
    err = fp.Sync()
    if err != nil {
        os.Remove(tmp)
        return err
    }
    return os.Rename(tmp, path)
}
