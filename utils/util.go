package utils

import (
	"os"
)

func PathIsExist(path string) bool {
	_, err := os.Stat(path)
	if err != nil && os.IsNotExist(err) {
		return false
	}
	return true
}

func CreateDir(dir string) {
	err := os.Mkdir(dir, os.ModePerm)
	if err != nil {
		panic(err)
	}
}

func RemoveDir(dir string) {
	err := os.RemoveAll(dir)
	if err != nil {
		panic(err)
	}
}
