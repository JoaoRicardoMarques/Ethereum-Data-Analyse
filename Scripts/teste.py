import duckdb
from web3 import Web3

def receipts(duckPath,apiPath):
    con=duckdb.connect(database=duckPath, read_only=True)
    w3=Web3(Web3.HTTPProvider(f'{apiPath}'))

    contractAddresses=[]
    amount=con.sql("SELECT COUNT(*) FROM transactions").fetchone()[0]
    
    for i in range(amount):
        hash=con.sql(f"SELECT hash FROM transactions WHERE id = {i}").fetchone()[0]

        receipt=w3.eth.get_transaction_receipt(hash)
        
        contractAddress=receipt.get("contractAddress")
        blockNumber=receipt.get("blockNumber")
        
        contractAddresses.append((contractAddress, blockNumber))


    con.close()
    return contractAddress

def contracts(duckPath,apiPath):
    w3=Web3(Web3.HTTPProvider(f'{apiPath}'))

    contractAddresses=receipts(duckPath,apiPath)

    con=duckdb.connect(database=duckPath,read_only=False)

    for (contractAddress, blockNumber) in enumerate(contractAddresses, start=0):
        bytecode=w3.eth.get_code(contractAddress).hex()

        con.sql(f"INSERT INTO contracts SELECT nextval('contract_id'), {contractAddress}, {bytecode}, {blockNumber}")
        
    con.close()

duckPath="/home/joao/Ethereum Data Analyse/Database/database-50000-99999.db"
apiPath="https://mainnet.infura.io/v3/a1396b66469549e8af981d99a0316269"
contracts(duckPath,apiPath)