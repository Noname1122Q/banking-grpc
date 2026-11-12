import logging
import threading
import uuid
from concurrent import futures
import grpc
from google.protobuf.timestamp_pb2 import Timestamp
import banking_pb2
import banking_pb2_grpc

ACCOUNT_SERVICE_ADDRESS = "localhost:50051"
TRANSACTION_PORT = 50052
RPC_TIMEOUT_SECONDS = 5

class TransactionServicer(banking_pb2_grpc.TransactionServiceServicer):
  def __init__(self, account_stub):
    self._accounts = account_stub 
    self._history = [] 
    self._lock = threading.Lock()
  
  def _call_accounts(self, context, rpc, message):
    try:
      return rpc(message, timeout=RPC_TIMEOUT_SECONDS)
    except grpc.RpcError as err:
      context.abort(err.code(),err.details() or "Account service unavailable")
      
  def _record(self, txn_id, sender, receiver, amount, status, note):
    now = Timestamp()
    now.GetCurrentTime()
    
    txn = banking_pb2.Transaction(transaction_id=txn_id, from_user_id=sender, to_user_id=receiver,amount=amount, status=status, timestamp=now, note=note)
    
    with self._lock:
      self._history.append(txn)
      
    return txn
  
  def InitiateTransfer(self, request, context):
    sender = request.from_user_id.strip()
    receiver = request.to_user_id.strip()
    
    amount = request.amount
    
    if not sender or not receiver:
      context.abort(grpc.StatusCode.INVALID_ARGUMENT,"from_user_id and to_user_id are required")
      
    if sender == receiver:
      context.abort(grpc.StatusCode.INVALID_ARGUMENT,"Cannot transfer to the same account")
      
    if amount <= 0:
      context.abort(grpc.StatusCode.INVALID_ARGUMENT,"amount must be greater than zero")
      
    txn_id = str(uuid.uuid4())

    self._call_accounts(context, self._accounts.GetBalance,banking_pb2.GetBalanceRequest(user_id=receiver))
    
    try:
      debit = self._accounts.UpdateBalance(banking_pb2.UpdateBalanceRequest(user_id=sender, amount=amount,operation=banking_pb2.OP_DEBIT),timeout=RPC_TIMEOUT_SECONDS)
    except grpc.RpcError as err:
      if err.code() == grpc.StatusCode.FAILED_PRECONDITION:
        self._record(txn_id, sender, receiver, amount,banking_pb2.STATUS_FAILED,f"Failed: {err.details()}")
        
      context.abort(err.code(),err.details() or "Account service unavailable")
      
    try:
      credit = self._accounts.UpdateBalance(banking_pb2.UpdateBalanceRequest(user_id=receiver, amount=amount,operation=banking_pb2.OP_CREDIT),timeout=RPC_TIMEOUT_SECONDS)
    except grpc.RpcError as err:
      logging.error("Credit failed (%s); refunding %s", err.code(), sender)
      
      try:
        self._accounts.UpdateBalance(banking_pb2.UpdateBalanceRequest(user_id=sender, amount=amount,operation=banking_pb2.OP_CREDIT),timeout=RPC_TIMEOUT_SECONDS)
      except grpc.RpcError:
        logging.critical("REFUND FAILED for txn %s - manual ""reconciliation required", txn_id)
        
        self._record(txn_id, sender, receiver, amount,banking_pb2.STATUS_FAILED,"Failed: could not credit receiver; sender refunded")
        
        context.abort(grpc.StatusCode.INTERNAL,"Transfer failed and was rolled back")
        
    self._record(txn_id, sender, receiver, amount,banking_pb2.STATUS_SUCCESS, request.note)
    
    logging.info("Transfer %s: %s -> %s amount=%d",txn_id, sender, receiver, amount)
    
    return banking_pb2.TransferResponse(transaction_id=txn_id,status=banking_pb2.STATUS_SUCCESS,from_balance=debit.new_balance,to_balance=credit.new_balance,message="Transfer completed successfully")
  
  def GetTransactionHistory(self, request, context):
    user_id = request.user_id.strip()
    
    if not user_id:
      context.abort(grpc.StatusCode.INVALID_ARGUMENT, "user_id must not be empty")
      
    if request.limit < 0:
      context.abort(grpc.StatusCode.INVALID_ARGUMENT,"limit must be 0 (all) or a positive number")
      
    self._call_accounts(context, self._accounts.GetBalance,banking_pb2.GetBalanceRequest(user_id=user_id))
    
    with self._lock:
      mine = [t for t in self._history if user_id in (t.from_user_id, t.to_user_id)]
    
    mine.reverse() # newest first
    
    if request.limit:
      mine = mine[:request.limit]
    
    return banking_pb2.HistoryResponse(user_id=user_id, transactions=mine)

def build_server(port=TRANSACTION_PORT, account_address=ACCOUNT_SERVICE_ADDRESS):
  channel = grpc.insecure_channel(account_address)
  
  stub = banking_pb2_grpc.AccountServiceStub(channel)
  
  server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
  
  banking_pb2_grpc.add_TransactionServiceServicer_to_server(TransactionServicer(stub), server)
  
  bound_port = server.add_insecure_port(f"[::]:{port}")
  
  return server, bound_port, channel

def serve():
  logging.basicConfig(level=logging.INFO,format="%(asctime)s [transaction] %(message)s")
  
  server, port, channel = build_server()
  
  server.start()
  
  logging.info("TransactionService listening on port %d", port)
  
  try:
    server.wait_for_termination()
  except KeyboardInterrupt:
    logging.info("Shutting down...")
    server.stop(grace=2)
  finally:
    channel.close()
 
if __name__ == "__main__":
  serve()
