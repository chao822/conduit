package cmd

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/sergi/go-diff/diffmatchpatch"
)

func diffCompare(t *testing.T, actual string, expected string) {
	if actual != expected {
		dmp := diffmatchpatch.New()
		diffs := dmp.DiffMain(expected, actual, true)
		patches := dmp.PatchMake(expected, diffs)
		patchText := dmp.PatchToText(patches)
		t.Fatalf("Unexpected output:\n%+v", patchText)
	}
}

/**
	Attempts to read a file and return the contents of that file as a string.
	readOptionalTestFile returns an empty string if the file name parameter being passed
	in is an empty string.
**/
func readOptionalTestFile(t *testing.T, fileName string) string {
	var fileData string

	if fileName != "" {
		file, err := os.Open(fmt.Sprintf("%s/%s", "testdata", fileName))
		if err != nil {
			t.Fatalf("Failed to open expected output file: %v", err)
		}

		goldenStdOutFile, err := ioutil.ReadAll(file)
		if err != nil {
			t.Fatalf("Failed to read expected output file: %v", err)
		}
		fileData = string(goldenStdOutFile)
	}

	return fileData
}
