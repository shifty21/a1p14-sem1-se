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

var servers [2]string
var next_server int = 0

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

func getGServers() [2]string {

	c, _, err := zk.Connect([]string{"zookeeper"}, time.Second)
	handleError("grproxy.getGServers|Error while connecting to zk", err)

	for c.State() != zk.StateHasSession {
		fmt.Println("gserve.registerToZookeeper|waiting from zk server")
		time.Sleep(1000000000)
	}

	servers[0] = getServerAddress("/gserve1", c)
	servers[1] = getServerAddress("/gserve2", c)
	return servers
}

func proxyHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
	w.Header().Set("Access-Control-Allow-Headers", "Accept, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization")
	if r.Method == "OPTIONS" {
		return
	}

	origin, _ := url.Parse(servers[next_server] + "library")

	director := func(req *http.Request) {
		req.Header.Add("X-Forwarded-Host", req.Host)
		req.Header.Add("X-Origin-Host", origin.Host)
		req.URL.Scheme = "http"
		req.URL.Host = origin.Host
	}

	proxy := &httputil.ReverseProxy{Director: director}
	proxy.ServeHTTP(w, r)
	next_server = 1 ^ next_server
}
func main() {

	origin, _ := url.Parse("http://nginx:80/")

	director := func(req *http.Request) {
		req.Header.Add("X-Forwarded-Host", req.Host)
		req.Header.Add("X-Origin-Host", origin.Host)
		req.URL.Scheme = "http"
		req.URL.Host = origin.Host
	}

	proxy := &httputil.ReverseProxy{Director: director}

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
		proxy.ServeHTTP(w, r)
		fmt.Printf("grproxy.main|Redirecting to homepage")
	})
	getGServers()
	http.HandleFunc("/library", proxyHandler)

	log.Fatal(http.ListenAndServe(":8080", nil))
}
