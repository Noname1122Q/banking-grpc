import logging
import threading
from concurrent import futures

import grpc
import banking_pb2
import banking_pb2_grpc

ACCOUNT_PORT = 50051
CURRENCY = "INR"

DEFAULT_ACCOUNTS = {
 "alice": 100_000,
 "bob": 50_000,
 "charlie": 0,
}

class AccountServicer(banking_pb2_grpc.AccountServiceServicer):
  def __init__(self, accounts=None):
    source = DEFAULT_ACCOUNTS if accounts is None else accounts
    self._balances = dict(source)
    self._lock = threading.Lock() # gRPC runs handlers on many threads
 
  @staticmethod
  def _require_user_id(user_id, context):
    if not user_id or not user_id.strip():
      context.abort(grpc.StatusCode.INVALID_ARGUMENT,"user_id must not be empty")
      
  def GetBalance(self, request, context):
    self._require_user_id(request.user_id, context)
    
    with self._lock:
      balance = self._balances.get(request.user_id)
      
    if balance is None:
      context.abort(grpc.StatusCode.NOT_FOUND,f"User '{request.user_id}' does not exist")
      
    logging.info("GetBalance(%s) -> %d", request.user_id, balance)
    
    return banking_pb2.GetBalanceResponse(user_id=request.user_id, balance=balance, currency=CURRENCY)
  
  def UpdateBalance(self, request, context):
    self._require_user_id(request.user_id, context)
    
    if request.amount <= 0:
      context.abort(grpc.StatusCode.INVALID_ARGUMENT,"amount must be greater than zero")
      
    if request.operation not in (banking_pb2.OP_CREDIT,banking_pb2.OP_DEBIT):
      context.abort(grpc.StatusCode.INVALID_ARGUMENT,"operation must be OP_CREDIT or OP_DEBIT")
      
    with self._lock:
      current = self._balances.get(request.user_id)
      
      if current is None:
        context.abort(grpc.StatusCode.NOT_FOUND,f"User '{request.user_id}' does not exist")
        
      if request.operation == banking_pb2.OP_DEBIT:
        if current < request.amount:
          context.abort(grpc.StatusCode.FAILED_PRECONDITION,f"Insufficient funds: balance {current}, "f"requested {request.amount}")
          
        new_balance = current - request.amount
      else:
        new_balance = current + request.amount
      
      self._balances[request.user_id] = new_balance
      
    logging.info("UpdateBalance(%s, %s %d) -> %d", request.user_id,banking_pb2.OperationType.Name(request.operation),request.amount, new_balance)
    
    return banking_pb2.UpdateBalanceResponse(user_id=request.user_id, new_balance=new_balance)
  
def build_server(port=ACCOUNT_PORT, accounts=None):
 server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
 
 banking_pb2_grpc.add_AccountServiceServicer_to_server(AccountServicer(accounts), server)
 
 bound_port = server.add_insecure_port(f"[::]:{port}")
 
 return server, bound_port

def serve():
 logging.basicConfig(level=logging.INFO,format="%(asctime)s [account] %(message)s")
 
 server, port = build_server()
 
 server.start()
 
 logging.info("AccountService listening on port %d", port)
 
 try:
  server.wait_for_termination()
 except KeyboardInterrupt:
  logging.info("Shutting down...")
  server.stop(grace=2)
  
if __name__ == "__main__":
 serve()
