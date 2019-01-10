package main

import (
	"bufio"
	"github.com/golang/protobuf/proto"
	"log"
	"net"
	"strconv"
	"sync"
	)



var mutex sync.Mutex

var localHashMap map[int32]int32

func main() {

	localHashMap = make(map[int32]int32)

	listener, err := net.Listen("tcp", ":8081")

	if err != nil {

		log.Println("There was the following error : ", err)

	}

	for {

		connection, err := listener.Accept()
		defer connection.Close()

		if err != nil {
			log.Println("There was the following error: ", err)
			continue
		}

		log.Println("Accepted the connection from  : ", connection.RemoteAddr())

		request_channel_per_connection := make(chan Request)
		defer close(request_channel_per_connection)

		go func(connection net.Conn) {

			r := bufio.NewReader(connection)
			buffer := make([]byte, 10)
			for {

				_, err := r.Read(buffer)


				if err == nil {

					request := &Request{}
					proto.Unmarshal(buffer, request)
					log.Println("\n\n\n Queueing a new request on request_channel_per_connection from client : ", connection.RemoteAddr(), ", Query : ", request)
					request_channel_per_connection <- *request
					buffer = make([]byte, 10)
				}

			}

		}(connection)

		go func(connection net.Conn) {

			for inRequest := range request_channel_per_connection {

				go func(inRequest Request) {

					//log.Println("Processing request : ", inRequest)
					response := parseAndHandleRequest(inRequest)
					out, err := proto.Marshal(&response)

					if err != nil {
						log.Println("Error while encoding response", err)
					}

					log.Println("Writing out response : ", response)
					connection.Write(out)

				}(inRequest)

			}

		}(connection)

	}

}

func handleWrite(request Request) Response {

	log.Println("Handling write query : Set Map[",request.GetKey(),"] = ", request.GetVal())

	var ok bool
	var val int32

	//log.Println("Taking lock before writing to hashmap")

	mutex.Lock()
	val, ok = localHashMap[request.Key]
	localHashMap[request.Key] = request.Val
	mutex.Unlock()

	//log.Println("Releasing lock after writing to hashmap")


	var response *Response

	if ok == false {

		response = &Response{Success:true, Msg:"New key : {" + strconv.Itoa(int(request.Key)) + "} is set with value : {" + strconv.Itoa(int(request.Val)) + "}", Key:request.Key, Val:request.Val}

	} else {

		response = &Response{Success:true, Msg:"Existing key : {" + strconv.Itoa(int(request.Key)) + "} " +
			"is modified from value : {" + strconv.Itoa(int(val)) + "} to value : {" + strconv.Itoa(int(request.Val)) + "}", Key:request.Key, Val:request.Val}
	}

	return *response
}


func handleRead(request Request) Response {

	log.Println("Handling read query : get Map[", request.GetKey(), "]")

	var ok bool
	var val int32

	//log.Println("Taking lock before reading from hashmap")

	mutex.Lock()
	val, ok = localHashMap[request.Key]
	mutex.Unlock()

	//log.Println("Releasing lock after reading from hashmap")

	var response *Response
	if ok == true {

		response = &Response{Success:true, Msg:strconv.Itoa(int(val)), Key:request.Key, Val:val}

		} else {

		response = &Response{Success:false, Msg:"Key : {" + strconv.Itoa(int(request.Key)) + "} not found on this server"}

	}

	log.Println(response.Msg)

	return *response

}

func parseAndHandleRequest(request Request) Response {

	if request.RequestType == 0 {

		return handleRead(request)

	} else {

		return handleWrite(request)
	}

}
