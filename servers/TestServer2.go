package main

import (
	"bufio"
	"github.com/golang/protobuf/proto"
	"log"
	"math/rand"
	"net"
	"sync"
	"time"
)

func main() {

	key_value_sample := make(map[int32]int32)

	var mutex sync.Mutex

	connection, err := net.Dial("tcp", ":8081")
	defer connection.Close()
	if err != nil {

		log.Println("got the following eror : ",err)
	}

	r := bufio.NewReader(connection)
	for {

		//log.Println("what happened")
		op := (rand.Int()+rand.Int()*7)%2

		if op == 0 {
			key := int32(rand.Int() % 17)
			val := int32(rand.Int() % 423)
			//log.Println("what happened there")


			log.Println("Performing write for key: {", key, "} and value : ", val)
			old_val, ok := key_value_sample[key]

			if ok == true {
				log.Println("The key : {", key, "} is being modified from : {", old_val, "} to new value : {", val, "}")

			} else {
				log.Println("The key : {", key, "} is being set to value : {", val, "}")
			}

			writeRequest := &Request{
				RequestType: 1,
				Key:int32(key),
				Val:int32(val),
			}

			out, err := proto.Marshal(writeRequest)

			if err != nil {

				log.Println("Failed to encode writeRequest")
			}

			connection.Write(out)

		} else {
			key := rand.Int() % 15

			log.Println("Performing read for key: {", key, "}")



			readRequest := &Request{
				RequestType:int32(0),
				Key:int32(key),
			}
			
			

			out, err := proto.Marshal(readRequest)

			if err != nil {

				log.Println("Failed to encode readRequest")
			}



			connection.Write(out)
		}



		time.Sleep((30)*time.Millisecond)

		go func() {

			buffer := make([]byte, 1000)

			for {

				_, err := r.Read(buffer)

				if err == nil {
					result := &Response{}
					proto.Unmarshal(buffer, result)
					log.Println("Got Response : ", result)

					if result.Success == true {

						mutex.Lock()
						key_value_sample[result.Key] = result.Val
						mutex.Unlock()
					}

					break
				}

			}
		}()




	}



}
