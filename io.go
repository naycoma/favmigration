package main

import (
	"encoding/json"
	"os"
	"path/filepath"
)

func Create(path string, b []byte) error {
	dir := filepath.Dir(path)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		os.MkdirAll(dir, os.ModePerm)
	}
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	file.Write(b)
	return nil
}

func CreateJson(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return Create(path, b)
}
