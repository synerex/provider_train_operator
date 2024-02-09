package main

import (
	"context"
	"flag"
	"fmt"

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
)

func init() {
	flag.Parse()
}

func supplyRecommendCallback(clt *sxutil.SXServiceClient, sp *api.Supply) {
	recommend := &rcm.Recommend{}
	if sp.Cdata != nil {
		err := proto.Unmarshal(sp.Cdata.Entity, recommend)
		if err == nil {
			log.Printf("Received Recommend Supply from %d %+v", sp.SenderId, recommend)
			if recommend.RecommendName == "A" {
				dmo := sxutil.DemandOpts{
					Name:  role,
					Cdata: sp.Cdata,
				}
				id := clt.ProposeDemand(&dmo)
				log.Printf("Returned ID: %v\n", id)
			}
		}
	} else {
		log.Printf("Received JsonRecord Supply from %d %+v", sp.SenderId, sp.ArgJson)
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

func supplyJsonRecordCallback(clt *sxutil.SXServiceClient, sp *api.Supply) {
	log.Printf("Received JsonRecord Supply from %d %+v", sp.SenderId, sp.ArgJson)
}

func subscribeJsonRecordSupply(client *sxutil.SXServiceClient) {
	ctx := context.Background() //
	for {                       // make it continuously working..
		client.SubscribeSupply(ctx, supplyJsonRecordCallback)
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

	rcmClient := sxutil.NewSXServiceClient(client, pbase.ALT_PT_SVC, fmt.Sprintf("{Client:%s}", role))
	// envClient := sxutil.NewSXServiceClient(client, pbase.JSON_DATA_SVC, fmt.Sprintf("{Client:%s}", role))

	wg.Add(1)
	log.Print("Subscribe Supply")
	go subscribeRecommendSupply(rcmClient)
	// go subscribeJsonRecordSupply(envClient)

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
