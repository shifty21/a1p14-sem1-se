package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

func createRootNode(flags int32, acl []zk.ACL, c *zk.Conn) {
	//check for root node
	exists, stat, err := c.Exists("/01")
	if err != nil {
		panic("Error while checking for rootnode")
	}
	//create root node
	if !exists {
		path, err := c.Create("/01", []byte("root"), flags, acl)
		if err != nil {
			panic("Error while creating root node! Exiting program")
		}
		fmt.Printf("main|createRootNode.root node created at %+v \n", path)
	} else {
		fmt.Printf("main|createRootNode.root node exists: %+v %+v \n", exists, stat)
	}

}

func createServerNode(flags int32, acl []zk.ACL, c *zk.Conn) {
	//check for root node
	exists, stat, err := c.Exists("/01/gserve1")
	if err != nil {
		panic("Error while fetching data for server node")
	}
	fmt.Printf("%+v\n", stat)
	//create root node
	if !exists {
		path, err := c.Create("/01/gserve1", []byte("http://localhost:8081/"), flags, acl)
		if err != nil {
			panic("Error while registring gserve1 to zookeeper")
		}
		fmt.Printf("main.createServerNode.created the gserver node : %+v\n", path)
	} else {
		fmt.Printf("main.createServerNode.server node exists %+v\n", stat)
	}

}

func registerToZookeeper() {
	fmt.Printf("main|registerToZookeeper.register the gserver node address to zk\n")
	// fmt.Printf("main|connectToZookeeper.Connect to zookeeper")
	c, _, err := zk.Connect([]string{"127.0.0.1"}, time.Second)
	if err != nil {
		panic(err)
	}
	flags := int32(0)
	acl := zk.WorldACL(zk.PermAll)
	// exists, _ = checkForNodeAvailability(c)
	fmt.Printf("main|checkForNodeAvailability.check if node is available or not")
	exists, stat, err := c.Exists("/01/gserve1")
	if err != nil {
		panic("Error while fetching data for gserve1")
	}
	fmt.Printf("main|checkForNodeAvailability.node exists: %+v %+v\n", exists, stat)
	if exists != true {
		createRootNode(flags, acl, c)
		createServerNode(flags, acl, c)
	} else {
		fmt.Printf("main|registerToZookeeper.node exists: %+v\n", exists)
	}
}
func saveDataToLibrary(encodedJSON []byte) int {
	var hbaseURL = "http://localhost:8080/se2:library/fakerow"
	req, err := http.NewRequest("PUT", hbaseURL, bytes.NewBuffer(encodedJSON))
	if err != nil {
		panic("Error While posting data to Hbase")
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	fmt.Println("response Status:", resp.StatusCode)
	body, _ := ioutil.ReadAll(resp.Body)
	fmt.Println("response Body:", string(body))

	return resp.StatusCode

}

func getLibraryData(w http.ResponseWriter) {
	fmt.Println("gserve1.main|getLibraryData")
	var hbaseURL = "http://localhost:8080/se2:library/*"
	req, err := http.NewRequest("GET", hbaseURL, nil)
	if err != nil {
		panic("Error While getting data from Hbase")
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}

	fmt.Println("response Status:", resp.StatusCode)

	fmt.Printf("resp.Body : %v\n", resp.Body)
	marshalledRows, _ := ioutil.ReadAll(resp.Body)
	fmt.Println("MarshalledRow: ", marshalledRows)
	var encodedRowsType EncRowsType
	json.Unmarshal(marshalledRows, &encodedRowsType)
	fmt.Println("unmarshalled EncodedRowsType : ", encodedRowsType)
	decodedRows, _ := encodedRowsType.decode()
	fmt.Println("UnencodedRowsType: ", decodedRows)
	decodedJSON, _ := json.Marshal(decodedRows)
	w.Write(decodedJSON)
	defer resp.Body.Close()
	return
}

func handler(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/library" {
		http.Error(w, "Redirecting to home page!", http.StatusNotFound)
		return
	}
	switch r.Method {
	case "GET":
		getLibraryData(w)
	case "PUT":
		unencodedJSON, err := ioutil.ReadAll(r.Body)
		if err != nil {
			panic(err)
		}
		// convert JSON to Go objects
		var unencodedRows RowsType
		json.Unmarshal(unencodedJSON, &unencodedRows)
		fmt.Println(string(unencodedJSON))
		// encode fields in Go objects
		encodedRows := unencodedRows.encode()
		// convert encoded Go objects to JSON
		encodedJSON, _ := json.Marshal(encodedRows)

		println("unencoded:", string(unencodedJSON))
		println("encoded:", string(encodedJSON))
		saveDataToLibrary(encodedJSON)

		// w.WriteHeader(statusCode)
	default:
		http.Error(w, "Sorry, only GET and PUT methods are supported.", http.StatusMethodNotAllowed)
		fmt.Fprintf(w, "Sorry, only GET and PUT methods are supported.")
	}
}

func main() {
	registerToZookeeper()
	// time.Sleep(time.Second * 10)
	fmt.Println("Registered to zookeeper")
	http.HandleFunc("/library", handler)
	log.Fatal(http.ListenAndServe(":8081", nil))

}
