package main

import (
	"encoding/csv"
	"fmt"
	"github.com/nfnt/resize"
	"image/jpeg"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"time"
)

type Config struct {
	MapFile  string
	SrcPath  string
	DestPath string
}
type Line struct {
	Src  string
	Dest string
}

const (
	MaxTaskNum = 16
)

var AppPath string
var cfg Config

func copyFileContents(src, dst string) (err error) {
	in, err := os.Open(src)
	if err != nil {
		return
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return
	}
	defer func() {
		cerr := out.Close()
		if err == nil {
			err = cerr
		}
	}()
	if _, err = io.Copy(out, in); err != nil {
		return
	}
	err = out.Sync()
	return
}

func Process(src, dest string) error {
	srcFile := filepath.Join(AppPath, "src", src)
	destFile := filepath.Join(AppPath, "dest", dest)
	if _, err := os.Stat(destFile); os.IsNotExist(err) {
		if err := os.MkdirAll(filepath.Dir(destFile), os.ModePerm); err != nil {
			return err
		}
		if err := copyFileContents(srcFile, destFile); err != nil {
			return err
		}
		if err := buildSmallJpeg(destFile[:len(destFile)-4]+"_small.jpg", destFile); err != nil {
			return err
		}
		return nil
	} else if err != nil {
		return err
	} else {
		return nil
	}
}
func buildSmallJpeg(destFile, srcFile string) error {
	src, err := os.Open(srcFile)
	if err != nil {
		return err
	}
	defer src.Close()
	srcJPG, err := jpeg.Decode(src)
	if err != nil {
		return err
	}
	destImage := resize.Thumbnail(720, 900, srcJPG, resize.Bilinear)

	destF, err := os.Create(destFile)
	if err != nil {
		return err
	}
	defer destF.Close()

	err = jpeg.Encode(destF, destImage, &jpeg.Options{90})
	if err != nil {
		return err
	}
	return nil
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	var err error
	if AppPath, err = filepath.Abs("."); err != nil {
		panic(err)
	}
	mapFile, err := os.Open(filepath.Join(AppPath, "map.csv"))
	if err != nil {
		panic(err)
	}
	defer mapFile.Close()
	csvReader := csv.NewReader(mapFile)
	title, err := csvReader.Read()
	if err != nil {
		panic(err)
	}
	var srcIdx int
	var destIdx int
	if title[0] == "SRC" {
		srcIdx = 0
		destIdx = 1
	} else {
		srcIdx = 1
		destIdx = 0
	}
	sem := make(chan int, MaxTaskNum)
	preTime := time.Now()
	i := int64(0)
	for {
		i++
		line, err := csvReader.Read()
		if err == io.EOF {
			close(sem)
			break
		} else if err != nil {
			panic(err)
		}
		sem <- 1
		go func(src, dest string) {
			if err := Process(src, dest); err != nil {
				file, err1 := os.OpenFile(filepath.Join(AppPath, "skip.txt"), os.O_RDWR|os.O_APPEND, 0666)
				if err1 != nil {
					panic(err1)
				}
				defer file.Close()
				if _, err2 := file.WriteString(fmt.Sprintf("%s error:%s\n", line[srcIdx], err)); err2 != nil {
					panic(err)
				}
			}
			<-sem
		}(line[srcIdx], line[destIdx])
		if i%100 == 0 {
			fmt.Printf("process %d (%.2f s/10000) ...\n", i, time.Now().Sub(preTime).Seconds()*100)
			preTime = time.Now()
		}
	}
	for len(sem) > 0 {
		time.Sleep(1000)
	}
}
