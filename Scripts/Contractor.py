import duckdb
from web3 import Web3

class contractor:
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
        return contractAddresses

    def contracts(duckPath,apiPath):
        w3=Web3(Web3.HTTPProvider(f'{apiPath}'))

        contractAddresses=contractor.receipts(duckPath,apiPath)

        con=duckdb.connect(database=duckPath,read_only=False)

        for (contractAddress, blockNumber) in contractAddresses:
            if Web3.isAddress(contractAddress):
                bytecode=w3.eth.get_code(contractAddress).hex()

                con.sql(f"""
                        INSERT INTO contracts (id,address, bytecode, block_number)
                        VALUES (nextval('contract_id'), '{contractAddress}', '{bytecode}', {blockNumber});

                """)
            else:
                print("erro")
        con.close()