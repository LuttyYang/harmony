package common

import (
	"encoding/csv"
	"log"
	"os"
	"strings"
)

var defaultFix *LocalFix

func init() {
	defaultFix = &LocalFix{
		txForceSuccess: make(map[string]bool),
	}
	defaultFix.init()
}

func GetDefaultFix() *LocalFix {
	return defaultFix
}

type LocalFix struct {
	txForceSuccess map[string]bool
}

func (f *LocalFix) init() {
	if _, err := os.Stat("rosetta_local_fix.csv"); !os.IsNotExist(err) {
		fixCsv, err := os.Open("rosetta_local_fix.csv")
		if err != nil {
			return
		}
		defer fixCsv.Close()

		reader := csv.NewReader(fixCsv)
		count := 0
		for {
			read, err := reader.Read()
			if err != nil {
				break
			}

			if len(read) != 2 {
				continue
			}

			switch read[0] {
			case "txForceSuccess":
				f.txForceSuccess[strings.ToLower(read[1])] = true
				count++
				break
			}
		}

		log.Printf("rosetta_local_fix: read %d data", count)
	}
}

func (f *LocalFix) IsForceTxSuccess(txHash string) bool {
	log.Printf("tx hash %s", txHash)
	return f.txForceSuccess[strings.ToLower(txHash)]
}
