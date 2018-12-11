package main

import (
	"fmt"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

var servers []string

var nextServer = -1

func getNextServer() int {
	nextServer = nextServer + 1
	fmt.Printf("grproxy.getNextServer|current server %v \n", nextServer)
	if nextServer >= len(servers) {
		nextServer = 0
	}
	return nextServer
}

func handleError(source string, err error) {
	if err != nil {
		fmt.Println("Error from : ", source)
		panic(err)
	}
}

func getServerAddress(path string, c *zk.Conn) string {

	exists, _, err := c.Exists(path)
	handleError("grproxy.getGServers|Error while fetching data for node", err)

	if !exists {
		fmt.Println("grproxy.getGServers|No Data found for " + path + " the server sleeping for 10 secs")
		time.Sleep(10000000000)
		getServerAddress(path, c)
	}
	data, _, err := c.Get(path)
	handleError("grproxy.getServerAddress|Error while getting address for "+path, err)
	fmt.Printf("grproxy.getGServers|getting path for "+path+" : %s\n", string(data))
	return string(data)

}

func createRootNode(flags int32, acl []zk.ACL, c *zk.Conn) {
	//check for root node
	exists, stat, err := c.Exists("/grproxy")
	handleError("gserve.createRootNode|Error while checking for rootnode", err)

	//create root node
	if !exists {
		path, err := c.Create("/grproxy", []byte("root"), flags, acl)
		handleError("gserve.createRootNode|Error while creating root node!", err)

		fmt.Printf("gserve.createRootNode|root node created at %+v \n", path)
	} else {
		fmt.Printf("gserve.createRootNode|root node exists: %+v %+v \n", exists, stat)
	}

}

func getZkConnection() (flags int32, acl []zk.ACL, c *zk.Conn) {
	c, _, err := zk.Connect([]string{"zookeeper"}, time.Second)
	if err != nil {
		time.Sleep(1000000000)
		fmt.Println("gserve.getZkConnection|Error connection to zk server try again!")
		getZkConnection()
	}

	for c.State() != zk.StateHasSession {
		fmt.Println("gserve.getZkConnection|waiting from zk server")
		time.Sleep(1000000000)
	}
	flags = int32(0)
	acl = zk.WorldACL(zk.PermAll)
	fmt.Println("gserve.getZkConnection|connected to zk returning")
	return flags, acl, c
}

func getGServers(c *zk.Conn) (chan []string, chan error) {
	fmt.Println("gserve.getGServers|getting active gserve nodes from zk")
	serverChan := make(chan []string)
	errors := make(chan error)
	go func() {
		for {
			children, _, events, err := c.ChildrenW("/grproxy")
			if err != nil {
				errors <- err
				return
			}
			serverChan <- children
			evt := <-events
			if evt.Err != nil {
				errors <- evt.Err
				return
			}
		}
	}()
	fmt.Println("gserve.getGServers|got active gserve nodes from zk ")
	return serverChan, errors
}

//NewMultiHostReverseProxy handles url path and return appropriate director to handle req
func NewMultiHostReverseProxy() *httputil.ReverseProxy {
	scheme := "http"
	director := func(req *http.Request) {
		if req.URL.Path == "/library" {
			fmt.Printf("grproxy.NewMultipleHostReverseProxy|req.url %v\n", req.URL)
			current := getNextServer()
			fmt.Printf("grproxy.NewMultipleHostReverseProxy|current server %v\n", current)
			origin, _ := url.Parse(servers[current] + "library")
			req.URL.Scheme = scheme
			req.URL.Host = origin.Host
		} else {
			fmt.Println("grproxy.NewMultipleHostReverseProxy|redirecting to home Page")
			req.URL.Scheme = scheme
			req.URL.Host = "nginx"
		}

	}
	return &httputil.ReverseProxy{Director: director}
}
func main() {
	flags, acl, c := getZkConnection()
	createRootNode(flags, acl, c)
	serverChan, errors := getGServers(c)
	go func() {
		for {
			select {

			case children := <-serverChan:
				fmt.Printf("grproxy.main|children --- %+v\n", children)
				var temp []string
				for _, child := range children {
					gserveURLs, _, err := c.Get("/grproxy/" + child)
					temp = append(temp, string(gserveURLs))
					if err != nil {
						fmt.Printf("grproxy.main|from child: %+v\n", err)
					}
				}
				servers = temp
				fmt.Printf("grproxy.main| %+v \n", servers)
			case err := <-errors:
				fmt.Printf("grproxy.main| %+v \n", err)
			}
		}
	}()

	proxy := NewMultiHostReverseProxy()
	log.Fatal(http.ListenAndServe(":8080", proxy))
}
