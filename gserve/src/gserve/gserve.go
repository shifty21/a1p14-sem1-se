package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"html/template"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

var server string = os.Getenv("server")

func createServerNode(flags int32, acl []zk.ACL, c *zk.Conn, serverPath string, server string) {
	//check for root node
	exists, stat, err := c.Exists(serverPath)
	handleError("gserve.createServerNode|Error while fetching data for server node", err)
	fmt.Printf("%+v\n", stat)
	//create root node
	if !exists {
		path, err := c.Create(serverPath, []byte("http://"+server+":9002/"), flags, acl)
		handleError("gserve.createServerNode|Error while registring gserve1 to zookeeper", err)
		fmt.Printf("gserve.createServerNode|Created the gserver node : %+v\n", path)
	} else {
		fmt.Printf("gserve.createServerNode|Server node exists %+v\n", stat)
	}
}

func registerToZookeeper() {
	fmt.Println("gserve.registerToZookeeper|Environment server " + server)
	serverPath := "/" + server
	c, _, err := zk.Connect([]string{"zookeeper"}, time.Second)
	if err != nil {
		time.Sleep(1000000000)
		fmt.Println("gserve.registerToZookeeper|Error connection to zk server try again!")
		registerToZookeeper()
	}

	for c.State() != zk.StateHasSession {
		fmt.Println("gserve.registerToZookeeper|waiting from zk server")
		time.Sleep(1000000000)
	}
	flags := int32(0)
	acl := zk.WorldACL(zk.PermAll)

	fmt.Println("gserve.registerToZookeeper|Check if " + serverPath + " is available or not")

	exists, stat, err := c.Exists(serverPath)
	handleError("gserve.registerToZookeeper|While fetching data from zk ", err)
	if !exists {
		fmt.Println("gserve.RegisterToZookeeper|Node doesnt exists " + serverPath)
		createServerNode(flags, acl, c, serverPath, server)
	} else {
		fmt.Printf("gserve.registerToZookeeper|Node exists: %+v\n", stat)
	}
	c.Close()
}

func saveDataToLibrary(encodedJSON []byte) {
	var hbaseURL = "http://hbase:8080/se2:library/fakerow"
	req, err := http.NewRequest("PUT", hbaseURL, bytes.NewBuffer(encodedJSON))
	handleError("gserve.saveDataToLibrary|Error while creating new http req", err)
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	handleError("gserve.saveDataToLibrary|Error while saving data to hbase", err)
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	handleError("gserve.saveDataToLibrary|Error while reading data from response body", err)
	fmt.Println("gserve.saveDataToLibrary|Response ", body)
}

func decodeDataFromHbase(encodedMarshalledRows []byte) []byte {
	var encodedRowsType EncRowsType
	err := json.Unmarshal(encodedMarshalledRows, &encodedRowsType)
	handleError("gserve.decodeDatFromHbase|Error while unmarshalling data", err)
	decodedRows, err := encodedRowsType.decode()
	handleError("gserve.decodeDatFromHbase|Error while decoding rows", err)
	decodedJSON, err := json.Marshal(decodedRows)
	handleError("gserve.decodeDatFromHbase|Error while marshalling data", err)
	fmt.Println("gserve.decodeDataFromHbase|decodedJSON: " + string(decodedJSON))
	return decodedJSON
}

func getLibraryData(w http.ResponseWriter, r *http.Request) {
	var hbaseURL = "http://hbase:8080/se2:library/*"
	req, err := http.NewRequest("GET", hbaseURL, nil)
	handleError("gserve.getLibraryData|Error while creating req", err)
	req.Header.Set("Accept", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Println("gserve.getLibraryData|Error while getting data from hbase ", server)
		http.Error(w, "Error while getting data from hbase "+server, http.StatusNotFound)
		return
	}
	encodedMarshalledRows, err := ioutil.ReadAll(resp.Body)
	handleError("gserve.getLibraryData|Error while reading data from response body", err)
	decodedJSON := decodeDataFromHbase(encodedMarshalledRows)
	fmt.Println("gserve.getLibraryData|decodedJSON: ", decodedJSON)
	//load data into page and send back
	// p, _ := loadPage(string(decodedJSON))
	p := loadPage(decodedJSON)
	fmt.Printf("gserve.getLibraryData|pagedata %v\n", p)
	tplFuncMap := make(template.FuncMap)
	tplFuncMap["Split"] = Split
	tplFuncMap["SplitKey"] = SplitKey
	t := template.Must(template.New("library.tmpl").Funcs(tplFuncMap).Parse(`<!DOCTYPE html>
	<html lang="en">
	<head>
	  <title>
	    {{.Title}}
	  </title>
	  <body>

	    <div name="library">
	      {{range .RowData}}
	              <h3>{{.Key}}</h3>
	                  {{range $index,$element := .Cell}}
	                    <h4>{{SplitKey $element.Column}}</h4>
	                    <div class="wrapper">
	                                <div class="wrapper">
	                                  <div class="box">{{Split $element.Column}}</div>
	                                  <div class="box">{{Split $element.Value}}</div>
	                                </div>
	                      </div>
	                  {{end}}
	      {{end}}

	    </div>
	    <footer><i>{{.Author}}</i></footer>
	  </body>
	  <style>
	  body {
	    margin: 40px;
	  }

	  .wrapper {
	    display: grid;
	    grid-template-columns: 200px 500px 500px;
	    grid-gap: 50px;
	    background-color: #fff;
	  }

	  .box {
	    border-radius: 5px;
	    padding: 20px;
	    font-size: 150%;
	  }
	  </style>
	</html>
`))
	handleError("gserve.getLibrary|unable to parse file", err)
	t.Execute(w, p)
	defer resp.Body.Close()
	return
}

//Split - splits the string on colon and gives the value
func Split(s string) string {
	arr := strings.Split(s, ":")
	return arr[1]
}

//SplitKey - splits the string on colon and gives the value
func SplitKey(s string) string {
	arr := strings.Split(s, ":")
	return strings.Title(arr[0])
}

//{"Row":[{"key":"1","Cell":[{"column":"document:sample","$":"value:samplevalue","timestamp":1543701034821},
//{"column":"metadata:meta","$":"value:samplemeta","timestamp":1543701034821}]}]}
func loadPage(decodedJSON []byte) *Data {
	var data Data
	fmt.Println("Value of decodedJSON : ", decodedJSON)
	json.Unmarshal(decodedJSON, &data)
	fmt.Println("Value of data ", data.RowData[0].Key)
	data.Title = "SE2 Library"
	data.Author = "Proudly Served by " + server

	return &data
}

//Cell multiple cells in a cell
type Cell struct {
	Column    string `json:"column"`
	Value     string `json:"$"`
	Timestamp string `json:"timestamp"`
}

//Row each row
type Row struct {
	Key  string `json:"key"`
	Cell []Cell `json:"Cell"`
}

//Data multiple rows
type Data struct {
	Title   string
	Author  string
	RowData []Row `json:"Row"`
}

func encodeDataForHbase(unencodedJSON []byte) []byte {
	var unencodedRows RowsType
	err := json.Unmarshal(unencodedJSON, &unencodedRows)
	handleError("gserve.getLibraryData|Error while unmarshalling data", err)
	fmt.Println("gserve.encodeDataForHbase|UnencodedJSON", string(unencodedJSON))
	encodedRows := unencodedRows.encode()
	encodedJSON, err := json.Marshal(encodedRows)
	handleError("gserve.getLibraryData|Error while marshalling data", err)
	fmt.Println("gserve.getLibraryData|Unencoded:", string(unencodedJSON))
	fmt.Println("gserve.getLibraryData|Encoded:", string(encodedJSON))
	return encodedJSON
}

func handleError(source string, err error) {
	if err != nil {
		fmt.Println("Error from : ", source)
		panic(err)
	}
}

func requestHandler(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/library" {
		http.Error(w, "Redirecting to home page!", http.StatusNotFound)
		return
	}
	switch r.Method {
	case "GET":
		getLibraryData(w, r)
	case "PUT":
		unencodedJSON, err := ioutil.ReadAll(r.Body)
		handleError("gserve.requestHandler|Error while reading response data", err)
		if len(unencodedJSON) > 0 {
			encodedJSON := encodeDataForHbase(unencodedJSON)
			saveDataToLibrary(encodedJSON)
		} else {
			http.Error(w, "Bad Request to server "+server, http.StatusBadRequest)
		}

	default:
		http.Error(w, "Sorry, only GET and PUT methods are supported.", http.StatusMethodNotAllowed)
	}
}

func main() {
	registerToZookeeper()
	http.HandleFunc("/library", requestHandler)
	log.Fatal(http.ListenAndServe(":9002", nil))

}
