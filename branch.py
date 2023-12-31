from concurrent import futures
import sys
import json
import grpc
import bank_pb2
import bank_pb2_grpc
import os


class Branch(bank_pb2_grpc.BankServicer):
    def __init__(self, id, balance, branches):
        self.id = id
        self.balance = balance
        self.branches = branches
        self.branch_logs = {
            "id": id,
            "type": "branch",
            "events": []
        }
        self.channelList = list()
        self.stubList = list()
        self.stubListBranchMapping = list()
        self.recvMsg = list()
        self.clock = 0

    def Deposit(self, request):
        self.branch_logs["events"].append({
            "customer-request-id": request.event_id,
            "logical_clock": self.clock,
            "interface": "deposit",
            "comment": f"event_recv from customer {request.id}"
        })
        self.balance += request.money
        if len(self.channelList) == 0:
            for id in self.branches:
                if id == self.id:
                    continue
                port = 50051 + id
                channel = grpc.insecure_channel(
                    f"localhost:{port}")
                self.channelList.append(channel)
                stub = bank_pb2_grpc.BankStub(channel)
                self.stubList.append(stub)
                self.stubListBranchMapping.append(id)
        for i in range(len(self.stubList)):
            stub = self.stubList[i]
            recv_branch = self.stubListBranchMapping[i]
            self.clock += 1
            self.branch_logs["events"].append({
                "customer-request-id": request.event_id,
                "logical_clock": self.clock,
                "interface": "propogate_deposit",
                "comment": f"event_sent to branch {recv_branch}"
            })
            stub.MsgDelivery(
                bank_pb2.MsgDeliveryRequest(id=self.id, event_id=request.event_id, balance=self.balance, interface="propagatedeposit", clock=self.clock))
        return {
            "id": self.id,
            "event_id": request.event_id,
            "result": "success",
            "clock": self.clock
        }

    def Query(self, request):
        return {
            "id": self.id,
            "event_id": request.event_id,
            "balance": self.balance,
            "clock": self.clock
        }

    def Withdraw(self, request):
        self.branch_logs["events"].append({
            "customer-request-id": request.event_id,
            "logical_clock": self.clock,
            "interface": "deposit",
            "comment": f"event_recv from customer {request.id}"
        })
        status = "fail"
        if self.balance >= request.money:
            status = "success"
            self.balance -= request.money
            if len(self.channelList) == 0:
                for id in self.branches:
                    if id == self.id:
                        continue
                    port = 50051 + id
                    channel = grpc.insecure_channel(
                        f"localhost:{port}")
                    self.channelList.append(channel)
                    stub = bank_pb2_grpc.BankStub(channel)
                    self.stubList.append(stub)
                    self.stubListBranchMapping.append(id)
            for i in range(len(self.stubList)):
                stub = self.stubList[i]
                recv_branch = self.stubListBranchMapping[i]
                self.clock += 1
                self.branch_logs["events"].append({
                    "customer-request-id": request.event_id,
                    "logical_clock": self.clock,
                    "interface": "propogate_withdraw",
                    "comment": f"event_sent to branch {recv_branch}"
                })
                stub.MsgDelivery(
                    bank_pb2.MsgDeliveryRequest(id=self.id, event_id=request.event_id, balance=self.balance, interface="propagatewithdraw", clock=self.clock))
        return {
            "id": self.id,
            "event_id": request.event_id,
            "result": status
        }

    def Propagate_Deposit(self, request):
        self.branch_logs["events"].append({
            "customer-request-id": request.event_id,
            "logical_clock": self.clock,
            "interface": "propogate_deposit",
            "comment": f"event_recv from bank {request.id}"
        })
        self.balance = request.balance
        return {
            "result": "success"
        }

    def Propagate_Withdraw(self, request):
        self.branch_logs["events"].append({
            "customer-request-id": request.event_id,
            "logical_clock": self.clock,
            "interface": "propogate_withdraw",
            "comment": f"event_recv from bank {request.id}"
        })
        self.balance = request.balance
        return {
            "result": "success"
        }

    def MsgDelivery(self, request, context):
        self.recvMsg.append(request)
        self.clock = max(self.clock, request.clock) + 1
        if request.interface == "query":
            response = self.Query(request=request)
        elif request.interface == "deposit":
            response = self.Deposit(request=request)
        elif request.interface == "withdraw":
            response = self.Withdraw(request=request)
        elif request.interface == "propagatewithdraw":
            response = self.Propagate_Withdraw(request=request)
        elif request.interface == "propagatedeposit":
            response = self.Propagate_Deposit(request=request)
        id = response.get("id", None)
        event_id = response.get("event_id", None)
        balance = response.get("balance", None)
        result = response.get("result", None)
        filename = f"branch-{self.id}.json"
        output_path = os.path.join("output", filename)
        with open(output_path, 'w') as file:
            json.dump(self.branch_logs, file, indent=4)
        return bank_pb2.MsgDeliveryResponse(id=id, event_id=event_id, balance=balance, result=result, clock=self.clock)


def start_grpc_servers(branches):
    servers = []
    branchPrcoessId = []
    for i in range(len(branches)):
        branchPrcoessId.append(branches[i]["id"])

    for i in range(len(branches)):
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        branch = Branch(id=branches[i]["id"],
                        balance=branches[i]["balance"], branches=branchPrcoessId)
        bank_pb2_grpc.add_BankServicer_to_server(branch, server)
        port = 50051 + branches[i]["id"]
        server.add_insecure_port(f'[::]:{port}')
        server.start()
        print(f"Branch {branches[i]['id']} started on port: {port}")
        servers.append(server)

    for server in servers:
        server.wait_for_termination()

    try:
        while True:
            pass
    except KeyboardInterrupt:
        print("\nStopping all servers.")
        for server in servers:
            server.stop(0)


if __name__ == '__main__':
    file_path = f'{sys.argv[1]}'
    with open(file_path, 'r') as json_file:
        data = json.load(json_file)

    branches = []

    for i in range(len(data)):
        if (data[i]["type"] == "branch"):
            branches.append(data[i])

    start_grpc_servers(branches)
