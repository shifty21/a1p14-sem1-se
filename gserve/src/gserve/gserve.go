package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"html/template"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

func createRootNode(flags int32, acl []zk.ACL, c *zk.Conn) {
	//check for root node
	exists, stat, err := c.Exists("/01")
	handleError("gserve.createRootNode|Error while checking for rootnode", err)
	//create root node
	if !exists {
		path, err := c.Create("/01", []byte("root"), flags, acl)
		handleError("gserve.createRootNode|Error while creating root node!", err)
		fmt.Printf("gserve.createRootNode|Root node created at %+v \n", path)
	} else {
		fmt.Printf("gserve.createRootNode|Root node exists: %+v %+v \n", exists, stat)
	}
}

func createServerNode(flags int32, acl []zk.ACL, c *zk.Conn) {
	//check for root node
	exists, stat, err := c.Exists("/01/gserve1")
	handleError("gserve.createServerNode|Error while fetching data for server node", err)
	fmt.Printf("%+v\n", stat)
	//create root node
	if !exists {
		path, err := c.Create("/01/gserve1", []byte("http://localhost:8081/"), flags, acl)
		handleError("gserve.createServerNode|Error while registring gserve1 to zookeeper", err)
		fmt.Printf("gserve.createServerNode|Created the gserver node : %+v\n", path)
	} else {
		fmt.Printf("gserve.createServerNode|Server node exists %+v\n", stat)
	}
}

func registerToZookeeper() {
	fmt.Printf("gserve.registerToZookeeper|Register the gserver node address to zk\n")

	c, _, err := zk.Connect([]string{"127.0.0.1"}, time.Second)
	handleError("gserve.registerToZookeeper|While connecting to zk", err)

	for c.State() != zk.StateHasSession {
		fmt.Println("gserve.registerToZookeeper|waiting from zk server")
		time.Sleep(100000000)
	}
	flags := int32(0)
	acl := zk.WorldACL(zk.PermAll)

	fmt.Printf("gserve.registerToZookeeper|Check if node is available or not")

	exists, stat, err := c.Exists("/01/gserve1")
	handleError("gserve.registerToZookeeper|While fetching data from zk ", err)
	fmt.Printf("gserve.registerToZookeeper|Node exists: %+v %+v\n", exists, stat)
	if exists != true {
		createRootNode(flags, acl, c)
		createServerNode(flags, acl, c)
	} else {
		fmt.Printf("gserve.registerToZookeeper|node exists: %+v\n", exists)
	}
}

func saveDataToLibrary(encodedJSON []byte) {
	var hbaseURL = "http://localhost:8080/se2:library/fakerow"
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

func getLibraryData(w http.ResponseWriter) {
	var hbaseURL = "http://localhost:8080/se2:library/*"
	req, err := http.NewRequest("GET", hbaseURL, nil)
	handleError("gserve.getLibraryData|Error while creating req", err)
	req.Header.Set("Accept", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	handleError("gserve.getLibraryData|Error while getting data from hbase", err)
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
	t := template.Must(template.New("library.tmpl").Funcs(tplFuncMap).ParseFiles("library.tmpl"))
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
	data.Author = "Proudly Served by Gserve1"
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
		getLibraryData(w)
	case "PUT":
		unencodedJSON, err := ioutil.ReadAll(r.Body)
		handleError("gserve.requestHandler|Error while reading response data", err)
		encodedJSON := encodeDataForHbase(unencodedJSON)
		saveDataToLibrary(encodedJSON)

	default:
		http.Error(w, "Sorry, only GET and PUT methods are supported.", http.StatusMethodNotAllowed)
	}
}

func main() {
	registerToZookeeper()
	// time.Sleep(time.Second * 10)
	fmt.Println("gserve.main|Registered to zookeeper")
	http.HandleFunc("/library", requestHandler)
	log.Fatal(http.ListenAndServe(":8081", nil))

}
