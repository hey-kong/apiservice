package main

import (
	"context"
	"dispatcher/utils"
	"flag"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sync"

	"github.com/boltdb/bolt"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

var clientset *kubernetes.Clientset

var boltDB *bolt.DB
var dir = "/tmp/boltdb/"
var dbName = "boltdb.db"
var nodeMap = "DeviceNode"

var edgeLabel = "node-role.kubernetes.io/edge"
var edgeNames []string
var nEdge int
var cur = 0

var mu sync.Mutex

func main() {
	var err error
	var config *rest.Config
	var kubeconfig *string

	if home := homeDir(); home != "" {
		kubeconfig = flag.String("kubeconfig", filepath.Join(home, ".kube", "config"), "")
	} else {
		kubeconfig = flag.String("kubeconfig", "", "")
	}
	flag.Parse()
	if config, err = rest.InClusterConfig(); err != nil {
		if config, err = clientcmd.BuildConfigFromFlags("", *kubeconfig); err != nil {
			panic(err.Error())
		}
	}

	clientset, err = kubernetes.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}

	loadDB()
	getEdgeNames()

	http.HandleFunc("/query", getNodeName)
	http.ListenAndServe(":6442", nil)
}

func homeDir() string {
	if h := os.Getenv("HOME"); h != "" {
		return h
	}
	return os.Getenv("USERPROFILE") //windows
}

func loadDB() {
	if ok := utils.PathIsExist(dir); !ok {
		utils.CreateDir(dir)
	}

	var err error
	boltDB, err = bolt.Open(dir+dbName, 0666, nil)
	if err != nil {
		panic(err)
	}

	err = boltDB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(nodeMap))
		if b == nil {
			_, err := tx.CreateBucket([]byte(nodeMap))
			if err != nil {
				log.Fatal(err)
			}
		}
		return nil
	})

	if err != nil {
		log.Fatal(err)
	}
}

func getEdgeNames() {
	nodes, err := clientset.CoreV1().Nodes().List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		panic(err.Error())
	}
	for _, node := range nodes.Items {
		labels := node.GetLabels()
		if _, ok := labels[edgeLabel]; ok {
			edgeNames = append(edgeNames, node.GetName())
		}
	}
	nEdge = len(edgeNames)
}

func getNodeName(w http.ResponseWriter, r *http.Request) {
	// get device id
	q := r.URL.Query()
	id := q.Get("id")
	if id == "" {
		w.Write([]byte("Device ID can not be empty"))
		return
	}

	// get node name
	var nodeName string
	for i := 0; i < nEdge; i++ {
		mu.Lock()
		nodeName = edgeNames[cur]
		cur++
		if cur == nEdge {
			cur = 0
		}
		mu.Unlock()

		nodeRel, err := clientset.CoreV1().Nodes().Get(context.TODO(), nodeName, metav1.GetOptions{})
		if err != nil {
			log.Fatal(err)
		}
		status := nodeRel.Status.Conditions[len(nodeRel.Status.Conditions)-1].Type
		if status == "Ready" {
			break
		}
	}

	// persist the key-value pair of device ID and node name
	tx, err := boltDB.Begin(true)
	if err != nil {
		log.Fatal(err)
	}
	defer tx.Rollback()

	b := tx.Bucket([]byte(nodeMap))
	if b != nil {
		if err = b.Put([]byte(id), []byte(nodeName)); err != nil {
			log.Fatal(err)
		}
	}

	if err = tx.Commit(); err != nil {
		log.Fatal(err)
	}

	// return node name
	log.Println("ok")
	w.Write([]byte(nodeName))
}
