import time
import sys
import json
import os

import grpc
import bank_pb2
import bank_pb2_grpc
import concurrent.futures


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
            "type": "customer",
            "events": []
        }
        for i in range(self.lastProcessedId+1, len(self.events)):
            self.lastProcessedId = i
            # print(f"processing {self.events[i]['interface']} Event with Index: {i}")
            if (self.events[i]["interface"] == "deposit"):
                response = self.stub.MsgDelivery(bank_pb2.MsgDeliveryRequest(
                    id=self.id, event_id=self.events[i]["customer-request-id"], interface="deposit", money=self.events[i]["money"], clock=self.clock))
                result["events"].append({
                    "customer-request-id": self.events[i]["customer-request-id"],
                    "interface": self.events[i]["interface"],
                    "logical_clock": self.clock,
                    "comment": f"event_sent from customer {self.id}"
                })
                # self.clock = response.clock
            if (self.events[i]["interface"] == "withdraw"):
                response = self.stub.MsgDelivery(bank_pb2.MsgDeliveryRequest(
                    id=self.id, event_id=self.events[i]["customer-request-id"], money=self.events[i]["money"], interface="withdraw", clock=self.clock))
                result["events"].append({
                    "customer-request-id": self.events[i]["customer-request-id"],
                    "interface": self.events[i]["interface"],
                    "logical_clock": self.clock,
                    "comment": f"event_sent from customer {self.id}"
                })
                # self.clock = response.clock
            self.clock += 1
        return result


def execute_customer(customer):
    return customer.executeEvents()


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
                    data[i]["id"], data[i]["customer-requests"])
            else:
                customers[data[i]["id"]].appendEvents(
                    data[i]["customer-requests"])

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(customers)) as executor:
        customer_futures = [executor.submit(
            execute_customer, customer) for customer in customers.values()]
        concurrent.futures.wait(customer_futures)
    customer_data = []
    for future in customer_futures:
        if future.done():
            result = future.result()
            customer_data.append(result)
        else:
            print("Task was not completed")

    # Generate 1st output file. CUSTOMER
    output_path = os.path.join("output", "output-1.json")
    with open(output_path, 'w') as json_file:
        json.dump(customer_data, json_file)

    # Generate 2nd output file. BRANCH
    branch_data = []
    for i in range(len(data)):
        if (data[i]["type"] == "branch"):
            output_path = os.path.join(
                "output", f"branch-{data[i]['id']}.json")
            with open(output_path, 'r') as branch_outputs:
                branch_data.append(json.load(branch_outputs))
            if os.path.exists(output_path):
                os.remove(output_path)

    output_path = os.path.join("output", "output-2.json")
    with open(output_path, 'w') as json_file:
        json.dump(branch_data, json_file)

    # Generate 3rd output file. ALL EVENTS
    flattened_data = []

    for branch in branch_data:
        branch_id = branch["id"]
        branch_type = branch["type"]
        for event in branch["events"]:
            flattened_event = {
                "id": branch_id,
                "type": branch_type,
                "customer-request-id": event["customer-request-id"],
                "logical_clock": event["logical_clock"],
                "interface": event["interface"],
                "comment": event["comment"]
            }
            flattened_data.append(flattened_event)

    all_events = []
    for i in range(len(customer_data)):
        id = customer_data[i]["id"]
        for j in range(len(customer_data[i]["events"])):
            all_events.append({
                "id": id,
                "customer-request-id": customer_data[i]["events"][j]["customer-request-id"],
                "type": "customer",
                "logical_clock":  customer_data[i]["events"][j]["logical_clock"],
                "interface":  customer_data[i]["events"][j]["interface"],
                "comment": customer_data[i]["events"][j]["comment"]
            })
            customer_request_id = customer_data[i]["events"][j]["customer-request-id"]
            filtered_events = filter(
                lambda event: event["customer-request-id"] == customer_request_id, flattened_data)
            sorted_events = sorted(
                filtered_events, key=lambda event: event["logical_clock"])
            for event in sorted_events:
                all_events.append(event)

    output_path = os.path.join("output", "output-3.json")
    with open(output_path, 'w') as json_file:
        json.dump(all_events, json_file)

    print("Task done, generated required files in output folder.")
