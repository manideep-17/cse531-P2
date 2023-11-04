import time
import sys
import json
import os

import grpc
import bank_pb2
import bank_pb2_grpc


class Customer:
    def __init__(self, id, events):
        self.id = id
        self.events = events
        self.recvMsg = list()
        self.channel = None
        self.stub = None
        self.port = 50051 + id
        self.lastProcessedId = -1
        self.clock = 1

    def appendEvents(self, events):
        self.events.extend(events)

    def createStub(self):
        self.channel = grpc.insecure_channel(f"localhost:{self.port}")
        stub = bank_pb2_grpc.BankStub(self.channel)
        return stub

    def executeEvents(self):
        if self.stub is None:
            self.stub = self.createStub()
        result = {
            "id": self.id,
            "recv": []
        }
        for i in range(self.lastProcessedId+1, len(self.events)):
            self.lastProcessedId = i
            # print(f"processing {self.events[i]['interface']} Event with Index: {i}")
            if (self.events[i]["interface"] == "deposit"):
                response = self.stub.MsgDelivery(bank_pb2.MsgDeliveryRequest(
                    id=self.id, event_id=self.events[i]["id"], interface="deposit", money=self.events[i]["money"], clock=self.clock))
                result["recv"].append({
                    "interface": self.events[i]["interface"],
                    "result": response.result,
                    "clock": self.clock,
                    "comment": f"event sent from customer {self.id}"
                })
                self.clock = response.clock
            if (self.events[i]["interface"] == "query"):
                response = self.stub.MsgDelivery(bank_pb2.MsgDeliveryRequest(
                    id=self.id, event_id=self.events[i]["id"], interface="query", clock=self.clock))
                result["recv"].append({
                    "interface": self.events[i]["interface"],
                    "balance": response.balance,
                    "clock": self.clock,
                    "comment": f"event sent from customer {self.id}"
                })
                self.clock = response.clock
            if (self.events[i]["interface"] == "withdraw"):
                response = self.stub.MsgDelivery(bank_pb2.MsgDeliveryRequest(
                    id=self.id, event_id=self.events[i]["id"], money=self.events[i]["money"], interface="withdraw", clock=self.clock))
                result["recv"].append({
                    "interface": self.events[i]["interface"],
                    "result": response.result,
                    "clock": self.clock,
                    "comment": f"event sent from customer {self.id}"
                })
                self.clock = response.clock
        return result


if __name__ == '__main__':
    file_path = f'{sys.argv[1]}'
    with open(file_path, 'r') as json_file:
        data = json.load(json_file)

    customerData = []
    customers = {}
    response = []

    for i in range(len(data)):
        if (data[i]["type"] == "customer"):
            if data[i]["id"] not in customers:
                customers[data[i]["id"]] = Customer(
                    data[i]["id"], data[i]["events"])
                response.append(customers[data[i]["id"]].executeEvents())

            else:
                customers[data[i]["id"]].appendEvents(data[i]["events"])
                response.append(customers[data[i]["id"]].executeEvents())
    print(response)
    output_path = os.path.join("output", "output-clock-sample.json")
    with open(output_path, 'w') as json_file:
        json.dump(response, json_file)
