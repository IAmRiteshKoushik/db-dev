package main

import (
	"fmt"

	misc "github.com/IAmRiteshKoushik/db-dev/misc"
)

const(
    DATA_STORE_PATH = "/home/rk/mydb/"
)

func main(){
    path := DATA_STORE_PATH + "file2.txt"
    data := "Hello, World"
    // if err := misc.SaveData1(path, []byte(data)); err != nil {
    //     fmt.Println("Data not saved")
    //     fmt.Println(err)
    //     return
    // }
    // fmt.Println("Data added to file")

    if err := misc.SaveData2(path, []byte(data)); err != nil {
        fmt.Println("Data not saved")
        fmt.Println(err)
        return
    }
    fmt.Println("Data added to file")
    return
}
