// Translate corpus material into action metadata + document
// format for bulk operations into Elasticsearch.

package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type doc struct {
	Index   string `json:"elasticsearch_index"`
	Channel string `json:"channel"`
}

func main() {
	fname := os.Args[1]
	in, err := os.Open(fname)
	if err != nil {
		panic(err)
	}
	defer in.Close()

	fileExt := filepath.Ext(fname)
	out, err := os.OpenFile(fmt.Sprintf("%s-bulk.json", fname[0:len(fname)-len(fileExt)]), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		panic(err)
	}
	defer out.Close()

	r := bufio.NewReader(in)
	for {
		var d *doc
		data, err := r.ReadBytes('\n')
		if err == nil || err == io.EOF {
			if len(data) > 0 && data[len(data)-1] == '\n' {
				data = data[:len(data)-1]
			}
		}
		if err != nil {
			if err != io.EOF {
				fmt.Printf("%+v", err)
			}
			break
		}

		err = json.Unmarshal(data, &d)
		index := d.Index
		if d.Channel != "" {
			index = fmt.Sprintf("%s-%s", d.Index, d.Channel)
		}
		actionMetaStr := fmt.Sprintf("{\"index\":{\"_index\":\"%s\"}}\n", index)
		dataStr := fmt.Sprintf("%s\n", data)
		out.WriteString(actionMetaStr)
		out.WriteString(dataStr)
	}
}
