import argparse
import sys
from decimal import Decimal, InvalidOperation
import grpc
import banking_pb2
import banking_pb2_grpc

TIMEOUT_SECONDS = 5

def to_minor_units(text):
  try:
    value = Decimal(text)
  except InvalidOperation:
    raise ValueError(f"'{text}' is not a valid amount")
  
  if not value.is_finite():
    raise ValueError(f"'{text}' is not a valid amount")
  
  scaled = value * 100
  
  if scaled != scaled.to_integral_value():
    raise ValueError("amount can have at most 2 decimal places")
  
  return int(scaled)

def format_money(minor_units):
 sign = "-" if minor_units < 0 else ""
 
 major, cents = divmod(abs(minor_units), 100)
 
 return f"{sign}{major:,}.{cents:02d}"


class BankingClient:
  def __init__(self, account_address="localhost:50051",transaction_address="localhost:50052"):
    self._account_channel = grpc.insecure_channel(account_address)
    self._transaction_channel = grpc.insecure_channel(transaction_address)
    self.accounts = banking_pb2_grpc.AccountServiceStub(self._account_channel)
    self.transactions = banking_pb2_grpc.TransactionServiceStub(self._transaction_channel)
    
  def close(self):
    self._account_channel.close()
    self._transaction_channel.close()
    
  def get_balance(self, user_id):
    return self.accounts.GetBalance(banking_pb2.GetBalanceRequest(user_id=user_id), timeout=TIMEOUT_SECONDS)
  
  def update_balance(self, user_id, amount, operation):
    return self.accounts.UpdateBalance(banking_pb2.UpdateBalanceRequest(user_id=user_id, amount=amount, operation=operation),timeout=TIMEOUT_SECONDS)
  
  def transfer(self, from_user, to_user, amount, note=""):
    return self.transactions.InitiateTransfer(banking_pb2.TransferRequest(from_user_id=from_user, to_user_id=to_user,amount=amount, note=note),timeout=TIMEOUT_SECONDS)
  
  def history(self, user_id, limit=0):
    return self.transactions.GetTransactionHistory(banking_pb2.HistoryRequest(user_id=user_id, limit=limit),timeout=TIMEOUT_SECONDS)


def cmd_balance(client, user):
  resp = client.get_balance(user)
  
  print(f"{resp.user_id}: {resp.currency} {format_money(resp.balance)}")
  

def cmd_update(client, user, amount_text, operation_name):
  operation = (banking_pb2.OP_CREDIT if operation_name == "credit"else banking_pb2.OP_DEBIT)
  
  resp = client.update_balance(user, to_minor_units(amount_text), operation)
  
  print(f"{resp.user_id}: new balance {format_money(resp.new_balance)}")
  
def cmd_transfer(client, sender, receiver, amount_text, note=""):
  resp = client.transfer(sender, receiver, to_minor_units(amount_text), note)
  status = banking_pb2.TransactionStatus.Name(resp.status)
  
  print(f"{status}: {resp.message}")
  print(f" transaction id : {resp.transaction_id}")
  print(f" {sender} balance: {format_money(resp.from_balance)}")
  print(f" {receiver} balance: {format_money(resp.to_balance)}")
  
def cmd_history(client, user, limit=0):
  resp = client.history(user, limit)
  
  if not resp.transactions:
    print(f"No transactions for {resp.user_id}")
    return
  
  print(f"Transactions for {resp.user_id} (newest first):")
  
  for t in resp.transactions:
    when = t.timestamp.ToDatetime().strftime("%Y-%m-%d %H:%M:%S UTC")
    status = banking_pb2.TransactionStatus.Name(t.status)
    
    print(f" {when} | {t.from_user_id} -> {t.to_user_id} | "f"{format_money(t.amount)} | {status} | {t.note}")
  
def print_rpc_error(err):
  print(f"[RPC ERROR] {err.code().name}: {err.details()}")

def run_demo(client):
  def step(title, action):
    print(f"\n=== {title} ===")
    
    try:
      action()
    except grpc.RpcError as err:
      print_rpc_error(err)
    except ValueError as err:
      print(f"[CLIENT ERROR] {err}")
      
  step("1. Balance of alice", lambda: cmd_balance(client, "alice"))
  
  step("2. Credit alice 100.00",lambda: cmd_update(client, "alice", "100.00", "credit"))
  
  step("3. Debit bob 20.00",lambda: cmd_update(client, "bob", "20.00", "debit"))
  
  step("4. Transfer alice -> bob 250.75",lambda: cmd_transfer(client, "alice", "bob", "250.75", "rent share"))
  
  step("5. Transfer bob -> charlie 30.00",lambda: cmd_transfer(client, "bob", "charlie", "30.00", "coffee"))
  
  step("6. History of bob", lambda: cmd_history(client, "bob"))
  
  step("7. ERROR: insufficient funds (charlie -> alice 99999.00)",lambda: cmd_transfer(client, "charlie", "alice", "99999.00"))
  
  step("8. ERROR: unknown user balance", lambda: cmd_balance(client, "zed"))
  
  step("9. ERROR: transfer to unknown user",lambda: cmd_transfer(client, "alice", "zed", "10.00"))
  
  step("10. ERROR: negative amount",lambda: cmd_transfer(client, "alice", "bob", "-5.00"))
  
  step("11. ERROR: invalid amount text",lambda: cmd_transfer(client, "alice", "bob", "abc"))
  
  step("12. ERROR: transfer to self",lambda: cmd_transfer(client, "alice", "alice", "1.00"))
  
  step("13. History of charlie (includes failed attempt)",lambda: cmd_history(client, "charlie"))
  
    
def build_parser():
  p = argparse.ArgumentParser(description="Banking gRPC client")
  
  p.add_argument("--account-addr", default="localhost:50051")
  p.add_argument("--transaction-addr", default="localhost:50052")
  
  sub = p.add_subparsers(dest="command", required=True)
  
  s = sub.add_parser("balance"); s.add_argument("user")
  s = sub.add_parser("update")
  
  s.add_argument("user"); s.add_argument("amount")
  s.add_argument("operation", choices=["credit", "debit"])
  
  s = sub.add_parser("transfer")
  
  s.add_argument("sender"); s.add_argument("receiver"); s.add_argument("amount")
  s.add_argument("--note", default="")
  
  s = sub.add_parser("history")
  
  s.add_argument("user"); s.add_argument("--limit", type=int, default=0)
  sub.add_parser("demo")
  
  return p


def main(argv=None):
  args = build_parser().parse_args(argv)
  client = BankingClient(args.account_addr, args.transaction_addr)
  
  try:
    if args.command == "balance":
      cmd_balance(client, args.user)
    elif args.command == "update":
      cmd_update(client, args.user, args.amount, args.operation)
    elif args.command == "transfer":
      cmd_transfer(client, args.sender, args.receiver, args.amount, args.note)
    elif args.command == "history":
      cmd_history(client, args.user, args.limit)
    elif args.command == "demo":
      run_demo(client)
    
    return 0
  except grpc.RpcError as err:
    print_rpc_error(err)
    return 1
  except ValueError as err:
    print(f"[CLIENT ERROR] {err}")
    return 1
  finally:
    client.close()
  
if __name__ == "__main__":
  sys.exit(main())
