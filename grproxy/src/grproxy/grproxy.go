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

func getServerAddress(path string, c *zk.Conn) string {
	data, _, err := c.Get(path)
	if err != nil {
		panic("Error while getting address for " + path)
	}
	fmt.Printf(path+" : %s\n", string(data))
	return string(data)
}

func getGServers(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
	w.Header().Set("Access-Control-Allow-Headers", "Accept, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization")
	if r.Method == "OPTIONS" {
		return
	}
	c, _, err := zk.Connect([]string{"127.0.0.1"}, time.Second)
	if err != nil {
		panic(err)
	}
	exists, _, err := c.Exists("/01")
	if err != nil {
		panic("Error while fetching data for root node")
	}

	if exists {
		var gserve1 = getServerAddress("/01/gserve1", c)
		fmt.Printf("calling : "+" : %s\n", string(gserve1)+"library"+"with req "+r.Method)
		origin, _ := url.Parse(gserve1 + "library")

		director := func(req *http.Request) {
			req.Header.Add("X-Forwarded-Host", req.Host)
			req.Header.Add("X-Origin-Host", origin.Host)
			req.URL.Scheme = "http"
			req.URL.Host = origin.Host
		}

		proxy := &httputil.ReverseProxy{Director: director}
		proxy.ServeHTTP(w, r)
	}

}
func main() {

	origin, _ := url.Parse("http://localhost:80/")

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
		fmt.Printf("Redirecting to homepage")
	})

	http.HandleFunc("/library", getGServers)

	log.Fatal(http.ListenAndServe(":9002", nil))
}
