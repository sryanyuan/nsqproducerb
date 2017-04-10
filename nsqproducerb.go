package nsqproducerb

import (
	"encoding/json"
	"fmt"
	"strings"

	"sync"

	"time"

	"errors"

	"github.com/nsqio/go-nsq"
	"github.com/sryanyuan/nsqproducer"
)

/*
NSQProducerb is an wrap for go-nsq client producer.
It polls all available nsqd instance and transpond user message to it
NOTE : not thread safe
*/

//Setting variables
var (
	maxRetryTimes = 1
)

//SetMaxRetryTimes set the retry times if producer connection is break
func SetMaxRetryTimes(tm int) {
	maxRetryTimes = tm
}

//logger interface
type logger interface {
	Output(calldepth int, s string) error
}

//INSQProducerb is an interface to export
type INSQProducerb interface {
	Stop()
	Publish(topic string, body []byte) error
	SetLogger(l logger, lvl nsq.LogLevel)
	Shutdown()
}

//Json unmarshal struct
type nsqlookupdNodesProducer struct {
	BroadcastAddress string   `json:"broadcast_address"`
	TCPPort          int      `json:"tcp_port"`
	Topics           []string `json:"topics"`
	//  internal use
	unavailable bool
	producer    *nsq.Producer
}
type nsqlookupdNodesData struct {
	Producers []nsqlookupdNodesProducer `json:"producers"`
}
type nsqlookupdNodesResult struct {
	StatusCode int                 `json:"status_code"`
	StatusText string              `json:"status_txt"`
	Data       nsqlookupdNodesData `json:"data"`
}

type nsqProducer struct {
	lookupdAddrs   []string
	cfg            *nsq.Config
	availableNodes []nsqlookupdNodesProducer
	currentNode    int
	loglvl         nsq.LogLevel
	log            logger
	nodesLock      sync.Mutex
	wg             sync.WaitGroup
	exitCh         chan struct{}
}

//Implement interface
func (n *nsqProducer) Stop() {
	n.nodesLock.Lock()
	defer n.nodesLock.Unlock()

	if n.availableNodes != nil {
		for i := range n.availableNodes {
			if nil != n.availableNodes[i].producer {
				n.availableNodes[i].producer.Stop()
				n.availableNodes[i].producer = nil
			}
		}
	}

	n.currentNode = -1
	n.availableNodes = nil
}

func (n *nsqProducer) Shutdown() {
	n.Stop()
	close(n.exitCh)
	n.wg.Wait()
	n.output(nsq.LogLevelInfo, "nsqproducerb quit...")
}

func (n *nsqProducer) Publish(topic string, body []byte) error {
	//  publish message
	err := n.pollPublish(topic, body)
	if nil == err {
		return nil
	}

	//  get the nsqd nodes from nsqlookupd again
	n.Stop()
	nodes, err := getAllAvailableNSQDFromNSQLookupds(n.lookupdAddrs)
	if nil != err {
		return err
	}

	n.nodesLock.Lock()
	n.availableNodes = nodes
	n.output(nsq.LogLevelInfo, fmt.Sprintf("get %d nodes after publish failed",
		len(nodes)))
	n.nodesLock.Unlock()
	return n.pollPublish(topic, body)
}

func (n *nsqProducer) SetLogger(l logger, lvl nsq.LogLevel) {
	n.log = l
	n.loglvl = lvl
}

//unexport methods
func (n *nsqProducer) output(lvl nsq.LogLevel, str string) {
	if nil == n.log {
		return
	}
	if lvl < n.loglvl {
		return
	}
	n.log.Output(0, str)
}

func (n *nsqProducer) pollPublish(topic string, body []byte) error {
	n.nodesLock.Lock()
	defer n.nodesLock.Unlock()
	//  create a new producer
	if nil == n.availableNodes ||
		len(n.availableNodes) == 0 {
		return fmt.Errorf("No available nsqd node : node list is empty")
	}
	//  try to connect
	var err error
	var producer *nsq.Producer
	nextIndex := n.currentNode
	if -1 == nextIndex {
		nextIndex = 0
	} else {
		nextIndex++
		if nextIndex >= len(n.availableNodes) {
			nextIndex = 0
		}
	}
	//  pooling the available node
	done := false
	prevNextIndex := nextIndex
	for {
		if !n.availableNodes[nextIndex].unavailable {
			//  already have a producer ?
			if nil == n.availableNodes[nextIndex].producer {
				//	create new producer
				producer, err = nsq.NewProducer(fmt.Sprintf("%s:%d",
					n.availableNodes[nextIndex].BroadcastAddress, n.availableNodes[nextIndex].TCPPort), n.cfg)
				if nil != n.log {
					producer.SetLogger(n.log, n.loglvl)
				}

				if nil == err {
					err = producer.Publish(topic, body)
					if nil == err {
						done = true
						n.output(nsq.LogLevelDebug, fmt.Sprintf("publish message to nsqd[%s:%d] topic[%s] success",
							n.availableNodes[nextIndex].BroadcastAddress, n.availableNodes[nextIndex].TCPPort, topic))
						n.availableNodes[nextIndex].producer = producer
					} else {
						producer.Stop()
						n.availableNodes[nextIndex].unavailable = true
					}
				} else {
					n.availableNodes[nextIndex].unavailable = true
				}
			} else {
				err = n.availableNodes[nextIndex].producer.Publish(topic, body)
				if nil == err {
					done = true
					n.output(nsq.LogLevelDebug, fmt.Sprintf("publish message to nsqd[%s:%d] topic[%s] success",
						n.availableNodes[nextIndex].BroadcastAddress, n.availableNodes[nextIndex].TCPPort, topic))
				} else {
					//  set unavailable
					n.availableNodes[nextIndex].producer.Stop()
					n.availableNodes[nextIndex].producer = nil
					n.availableNodes[nextIndex].unavailable = true
				}
			}
		} else {
			if nil != n.availableNodes[nextIndex].producer {
				n.availableNodes[nextIndex].producer.Stop()
				n.availableNodes[nextIndex].producer = nil
			}
		}

		if done {
			n.currentNode = nextIndex
			return nil
		}
		//  search for next one
		nextIndex++
		if nextIndex >= len(n.availableNodes) {
			nextIndex = 0
		}
		if nextIndex == prevNextIndex {
			break
		}
	}

	//  publish failed
	return fmt.Errorf("Can't find available nsqd to publish")
}

//Update lookupd nsq nodes info
func (n *nsqProducer) lookupdNodesLoop() {
	defer n.wg.Done()
	ticker := time.NewTicker(time.Second * 30)

	for {
		select {
		case <-ticker.C:
			{
				n.lookupdNodes()
			}
		case <-n.exitCh:
			{
				return
			}
		}
	}
}

func (n *nsqProducer) lookupdNodes() {
	nodes, err := getAllAvailableNSQDFromNSQLookupds(n.lookupdAddrs)
	if nil != err {
		return
	}

	n.nodesLock.Lock()
	defer n.nodesLock.Unlock()

	//	already have node?
	nodeDelCount := 0
	nodeAddCount := 0
	if nil != n.availableNodes &&
		len(n.availableNodes) != 0 {
		for _, oldNode := range n.availableNodes {
			//	copy producer info
			found := false
			for newNodeIndex := range nodes {
				if nodes[newNodeIndex].BroadcastAddress == oldNode.BroadcastAddress &&
					nodes[newNodeIndex].TCPPort == oldNode.TCPPort {
					nodes[newNodeIndex].producer = oldNode.producer
					found = true
					break
				}
			}
			//	not found, close the producer
			if !found {
				nodeDelCount++
				if nil != oldNode.producer {
					oldNode.producer.Stop()
				}
			}
		}
	}
	nodeAddCount = len(nodes) - (len(n.availableNodes) - nodeDelCount)
	n.availableNodes = nodes

	n.output(nsq.LogLevelInfo, fmt.Sprintf("update nsqd list success, add %d nodes, remove %d nodes, %d nodes available",
		nodeAddCount, nodeDelCount, len(nodes)))
}

//Get all available nodes from nsqlookupd
func getAllAvailableNSQDFromNSQLookupds(nsqlookupdAddrs []string) ([]nsqlookupdNodesProducer, error) {
	if nil == nsqlookupdAddrs ||
		len(nsqlookupdAddrs) == 0 {
		return nil, fmt.Errorf("nsqlookupd address must be non-empty")
	}

	var nodes []nsqlookupdNodesProducer
	var err error
	for _, v := range nsqlookupdAddrs {
		nodes, err = getAllAvailableNSQDFromNSQLookupd(v)
		if nil != err {
			//	do next search
			continue
		}
		return nodes, nil
	}
	return nil, err
}

//Get all available nodes from nsqlookupd
func getAllAvailableNSQDFromNSQLookupd(nsqlookupdAddr string) ([]nsqlookupdNodesProducer, error) {
	//  search for all available node
	body, err := nsqproducer.DoHTTPGet("http://"+nsqlookupdAddr+"/nodes", nil)
	if err != nil {
		return nil, err
	}

	//  parse json
	var nodesResult nsqlookupdNodesResult
	if err = json.Unmarshal(body, &nodesResult); nil != err {
		return nil, err
	}

	//  check result
	if nodesResult.StatusCode != 200 {
		return nil, fmt.Errorf("Get nodes from nsqd failed , status_code : %d", nodesResult.StatusCode)
	}

	//  get all nodes
	return nodesResult.Data.Producers, nil
}

/*NewNSQProducer return a NSQProducer instance
  @param1 nsqlookupAddr
*/
func NewNSQProducer(nsqlookupdAddrs []string, config *nsq.Config, l logger, loglvl nsq.LogLevel) (INSQProducerb, error) {
	instance := &nsqProducer{
		lookupdAddrs: nsqlookupdAddrs,
		cfg:          config,
		exitCh:       make(chan struct{}),
	}

	nodes, err := getAllAvailableNSQDFromNSQLookupds(nsqlookupdAddrs)
	if nil != err {
		return nil, err
	}
	instance.availableNodes = nodes
	instance.log = l
	instance.loglvl = loglvl

	//	node update loop
	instance.wg.Add(1)
	go instance.lookupdNodesLoop()

	return instance, nil
}

// NewNSQProducerByAdminAddress initialize a producer instance by getting nsqlookupd info from nsq admin
func NewNSQProducerByAdminAddress(adminAddr string, config *nsq.Config, l logger, loglvl nsq.LogLevel) (INSQProducerb, error) {
	// Get nsqlookupd info from nsq admin
	reqAddr := adminAddr
	if !strings.Contains(reqAddr, "http://") {
		reqAddr = "http://" + reqAddr
	}
	// Get content from nsq admin
	body, err := nsqproducer.DoHTTPGet(reqAddr+"/nodes", nil)
	if err != nil {
		return nil, err
	}
	if nil != err {
		return nil, err
	}
	const findStr = "var NSQLOOKUPD = ["
	content := string(body)
	firstPos := strings.Index(content, findStr)
	if -1 == firstPos {
		// Not found
		return nil, errors.New("Parse nsqlookupd from nsq admin failed")
	}
	lookupdBytes := make([]byte, 0, 1024)
	for pos := firstPos + len(findStr); ; pos++ {
		if body[pos] == ']' {
			// Reach the end
			break
		}
		if body[pos] == '\'' {
			// Ignore splitter
			continue
		}
		lookupdBytes = append(lookupdBytes, body[pos])
	}
	lookupdList := strings.Split(string(lookupdBytes), ",")
	// If element is empty, remove it
	for i, v := range lookupdList {
		if v == "" {
			lookupdList = lookupdList[0:i]
			break
		}
	}
	if len(lookupdList) <= 0 {
		return nil, errors.New("nsqlookupd unavailable")
	}

	return NewNSQProducer(lookupdList, config, l, loglvl)
}
