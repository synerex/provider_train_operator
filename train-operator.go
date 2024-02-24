package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"

	rcm "github.com/synerex/proto_recommend"
	api "github.com/synerex/synerex_api"
	pbase "github.com/synerex/synerex_proto"
	sxutil "github.com/synerex/synerex_sxutil"
	"google.golang.org/protobuf/proto"

	"log"
	"sync"
	"time"
)

var (
	nodesrv         = flag.String("nodesrv", "127.0.0.1:9990", "Node ID Server")
	local           = flag.String("local", "", "Local Synerex Server")
	mu              sync.Mutex
	version         = "0.0.0"
	role            = "TrainOperator"
	sxServerAddress string
	proposedDmIds   []uint64
	rcmClient       *sxutil.SXServiceClient
	supplies        []*api.Supply
	recommends      []*rcm.Recommend
)

func init() {
	flag.Parse()
}

func supplyRecommendDemandCallback(clt *sxutil.SXServiceClient, dm *api.Demand) {
	recommend := &rcm.Recommend{}
	if dm.Cdata != nil {
		err := proto.Unmarshal(dm.Cdata.Entity, recommend)
		if err == nil {
			log.Printf("Received Recommend Demand: Demand: %+v, Recommend: %+v", dm, recommend)
		}
	} else {
		log.Printf("Received JsonRecord Demand: Demand %+v, JSON %s", dm, dm.ArgJson)
	}
}

func supplyRecommendCallback(clt *sxutil.SXServiceClient, sp *api.Supply) {
	recommend := &rcm.Recommend{}
	if sp.Cdata != nil {
		err := proto.Unmarshal(sp.Cdata.Entity, recommend)
		if err == nil {
			log.Printf("Received Recommend Supply: Supply: %+v, Recommend: %+v", sp.SenderId, recommend)
			supplies = append(supplies, sp)
			recommends = append(recommends, recommend)
			// if recommend.RecommendName == "A" {
			// 	dmo := sxutil.DemandOpts{
			// 		Name:  role,
			// 		Cdata: sp.Cdata,
			// 		JSON:  `{ "mobility":"alternative", "direction":"North", "from": "岩倉駅" }`,
			// 	}
			// 	dmid := clt.ProposeDemand(&dmo)
			// 	proposedDmIds = append(proposedDmIds, dmid)
			// 	log.Printf("#4 ProposeDemand Sent OK! dmo: %#v, dmid: %d\n", dmo, dmid)
			// }
		}
	} else {
		flag := false
		for _, pdid := range proposedDmIds {
			if pdid == sp.TargetId {
				flag = true
				log.Printf("Received JsonRecord Supply for me: Supply %+v, JSON: %s", sp, sp.ArgJson)
				err := clt.Confirm(sxutil.IDType(sp.Id), sxutil.IDType(sp.Id))
				if err != nil {
					log.Printf("#6 Confirm Send Fail! %v\n", err)
				} else {
					log.Printf("#6 Confirmed! %+v\n", sp)
				}
			}
		}
		if !flag {
			log.Printf("Received JsonRecord Supply for others: Supply %+v, JSON: %s", sp, sp.ArgJson)
		}
	}
}

func subscribeRecommendSupply(client *sxutil.SXServiceClient) {
	ctx := context.Background() //
	for {                       // make it continuously working..
		client.SubscribeSupply(ctx, supplyRecommendCallback)
		log.Print("Error on subscribe")
		reconnectClient(client)
	}
}

func reconnectClient(client *sxutil.SXServiceClient) {
	mu.Lock()
	if client.SXClient != nil {
		client.SXClient = nil
		log.Printf("Client reset \n")
	}
	mu.Unlock()
	time.Sleep(5 * time.Second) // wait 5 seconds to reconnect
	mu.Lock()
	if client.SXClient == nil {
		newClt := sxutil.GrpcConnectServer(sxServerAddress)
		if newClt != nil {
			log.Printf("Reconnect server [%s]\n", sxServerAddress)
			client.SXClient = newClt
		}
	} else { // someone may connect!
		log.Printf("Use reconnected server [%s]\n", sxServerAddress)
	}
	mu.Unlock()
}

func suppliesHandler(w http.ResponseWriter, r *http.Request) {
	log.Printf("Called /api/v0/supplies\n")

	response, err := json.Marshal(recommends)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(response)
}

func proposeDemandHandler(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("name")
	log.Printf("Called /api/v0/propose_demand with name: %s\n", name)

	recommend := &rcm.Recommend{}
	for _, sp := range supplies {
		proto.Unmarshal(sp.Cdata.Entity, recommend)
		if recommend.RecommendName == name {
			dmo := sxutil.DemandOpts{
				Name:  role,
				Cdata: sp.Cdata,
				JSON:  `{ "mobility":"alternative", "direction":"North", "from": "岩倉駅" }`,
			}
			dmid := rcmClient.ProposeDemand(&dmo)
			proposedDmIds = append(proposedDmIds, dmid)
			supplies = nil
			recommends = nil
			log.Printf("#4 ProposeDemand Sent OK! dmo: %#v, dmid: %d\n", dmo, dmid)
			break
		}
	}

	response, err := json.Marshal(recommend)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(response)
}

func main() {
	go sxutil.HandleSigInt()
	sxutil.RegisterDeferFunction(sxutil.UnRegisterNode)
	log.Printf("%s(%s) built %s sha1 %s", role, sxutil.GitVer, sxutil.BuildTime, sxutil.Sha1Ver)

	channelTypes := []uint32{pbase.ALT_PT_SVC} //, pbase.JSON_DATA_SVC}

	var rerr error
	sxServerAddress, rerr = sxutil.RegisterNode(*nodesrv, role, channelTypes, nil)

	if rerr != nil {
		log.Fatal("Can't register node:", rerr)
	}
	if *local != "" { // quick hack for AWS local network
		sxServerAddress = *local
	}
	log.Printf("Connecting SynerexServer at [%s]", sxServerAddress)

	wg := sync.WaitGroup{} // for syncing other goroutines

	client := sxutil.GrpcConnectServer(sxServerAddress)

	if client == nil {
		log.Fatal("Can't connect Synerex Server")
	} else {
		log.Print("Connecting SynerexServer")
	}

	rcmClient = sxutil.NewSXServiceClient(client, pbase.ALT_PT_SVC, fmt.Sprintf("{Client:%s}", role))
	// envClient := sxutil.NewSXServiceClient(client, pbase.JSON_DATA_SVC, fmt.Sprintf("{Client:%s}", role))

	wg.Add(1)
	log.Print("Subscribe Supply")
	go subscribeRecommendSupply(rcmClient)
	sxutil.SimpleSubscribeDemand(rcmClient, supplyRecommendDemandCallback)
	// go subscribeJsonRecordSupply(envClient)

	http.HandleFunc("/api/v0/supplies", suppliesHandler)
	http.HandleFunc("/api/v0/propose_demand", proposeDemandHandler)
	fmt.Println("Server is running on port 8030")
	go http.ListenAndServe(":8060", nil)

	// タイマーを開始する
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	// 現在時刻を取得し、次の実行時刻まで待機する
	start := time.Now()
	adjust := start.Truncate(15 * time.Second).Add(15 * time.Second)
	time.Sleep(adjust.Sub(start))

	for {
		select {
		case t := <-ticker.C:
			// ここに実行したい処理を書く
			fmt.Println("実行時刻:", t.Format("15:04:05"))
			smo := sxutil.SupplyOpts{
				Name: role,
				JSON: fmt.Sprintf(`{ "%s": null }`, role), // ここにバス運行状況を入れる
			}
			_, nerr := rcmClient.NotifySupply(&smo)
			if nerr != nil {
				log.Printf("Send Fail! %v\n", nerr)
			} else {
				//							log.Printf("Sent OK! %#v\n", ge)
			}
		}
	}

	wg.Wait()
}
