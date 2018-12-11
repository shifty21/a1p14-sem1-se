package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"html/template"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

var server string = os.Getenv("server")

func createServerNode(flags int32, acl []zk.ACL, c *zk.Conn, serverPath string, server string) {
	exists, stat, err := c.Exists("/grproxy")
	handleError("gserve.createServerNode|Error while fetching data for server node", err)
	if !exists {
		time.Sleep(1000000000)
		createServerNode(flags, acl, c, serverPath, server)
	} else {
		fmt.Printf("gserve.createServerNode|Root node exists %+v\n", stat)
	}
	exists, stat, err = c.Exists(serverPath)
	handleError("gserve.createServerNode|Error while fetching data for server node", err)
	fmt.Printf("gserve.createServerNode| stat %v \n", stat)
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
	serverPath := "/grproxy/" + server
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
	flags := int32(zk.FlagEphemeral)
	acl := zk.WorldACL(zk.PermAll)

	createServerNode(flags, acl, c, serverPath, server)
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

func getHbaseDataURL(client *http.Client) *url.URL {
	var hbaseURL = "http://hbase:8080/se2:library/scanner"
	//get scanner url from hbase
	body := strings.NewReader("<Scanner batch=\"10\"/>")
	scannerReq, err := http.NewRequest(http.MethodPut, hbaseURL, body)
	handleError("gserve.getHbaseDataURL|Error while creating req", err)
	scannerReq.Header.Set("Content-Type", "text/xml")
	scannerReq.Header.Set("Accept", "text/plain")
	scannerReq.Header.Set("Accept-Encoding", "identity")

	resp, err := client.Do(scannerReq)
	if err != nil {
		fmt.Println("gserve.getHbaseDataURL|Error while getting data from hbase ", server)
	}
	fmt.Printf("gserve.getHbaseDataURL|body, %v \n", resp)
	dataURL, _ := resp.Location()
	defer scannerReq.Body.Close()
	return dataURL

}

func loadPage(decodedJSON []byte) *EncRowsType {

	var data EncRowsType
	fmt.Println("gserve.loadPage|Value of decodedJSON : ", decodedJSON)
	json.Unmarshal(decodedJSON, &data)
	fmt.Printf("gserve.loadPage|Value of data %v \n", data)

	return &data
}

func decodeDataFromHbase(encodedMarshalledRows []byte) []byte {
	var encodedRowsType EncRowsType
	err := json.Unmarshal(encodedMarshalledRows, &encodedRowsType)
	handleError("gserve.decodeDataFromHbase|Error while unmarshalling data", err)

	decodedRows, err := encodedRowsType.decode()
	handleError("gserve.decodeDataFromHbase|Error while decoding rows", err)
	fmt.Printf("gserve.decodeDataFromHbase|decodedJSON: %v \n", decodedRows)

	decodedJSON, err := json.Marshal(decodedRows)

	handleError("gserve.decodeDataFromHbase|Error while marshalling data", err)
	fmt.Println("gserve.decodeDataFromHbase|decodedMarshalledJSON: " + string(decodedJSON))
	return decodedJSON
}

func getLibraryData(w http.ResponseWriter, r *http.Request) {
	client := &http.Client{}
	dataURL := getHbaseDataURL(client)
	fmt.Println("gserve.getLibraryData|locationURL, ", dataURL)
	dataReq, err := http.NewRequest(http.MethodGet, dataURL.String(), nil)
	handleError("gserve.getLibraryData|error while creating req from scanner url", err)
	dataReq.Header.Set("Accept", "application/json")

	dataResp, err := client.Do(dataReq)
	handleError("gserve.getLibraryData|error while getting data from scanner url", err)

	encodedMarshalledRows, err := ioutil.ReadAll(dataResp.Body)
	handleError("gserve.getLibraryData|Error while reading data from response body", err)
	defer dataResp.Body.Close()

	decodedJSON := decodeDataFromHbase(encodedMarshalledRows)
	fmt.Println("gserve.getLibraryData|decodedJSON: ", decodedJSON)

	p := loadPage(decodedJSON)
	fmt.Printf("gserve.getLibraryData|pagedata %v\n", p)
	tplFuncMap := make(template.FuncMap)
	tplFuncMap["Split"] = Split
	tplFuncMap["SplitKey"] = SplitKey
	t := template.Must(template.New("library.tmpl").Funcs(tplFuncMap).Parse(`<!DOCTYPE html>
	<html lang="en">
	<head>
	  <title>
	    SE2 Library
	  </title>
	  <body>

	    <div name="library">
	      {{range .Row}}
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
	    <footer><i>` + "Proudly Serverd by " + server + ` </i></footer>
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

func encodeDataForHbase(unencodedJSON []byte) []byte {
	var unencodedRows RowsType
	err := json.Unmarshal(unencodedJSON, &unencodedRows)
	handleError("gserve.encodeDataForHbase|Error while unmarshalling data", err)
	fmt.Println("gserve.encodeDataForHbase|UnencodedJSON", string(unencodedJSON))
	encodedRows := unencodedRows.encode()
	encodedJSON, err := json.Marshal(encodedRows)
	handleError("gserve.encodeDataForHbase|Error while marshalling data", err)
	fmt.Println("gserve.encodeDataForHbase|Unencoded:", string(unencodedJSON))
	fmt.Println("gserve.encodeDataForHbase|Encoded:", string(encodedJSON))
	return encodedJSON
}

func handleError(source string, err error) {
	if err != nil {
		fmt.Println("Error from : ", source)
		panic(err)
	}
}

func requestHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
	w.Header().Set("Access-Control-Allow-Headers", "Accept, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization")
	if r.Method == "OPTIONS" {
		w.WriteHeader(http.StatusOK)
		return
	}
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
	http.HandleFunc("/", requestHandler)
	log.Fatal(http.ListenAndServe(":9002", nil))

}
