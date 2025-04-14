import duckdb
from web3 import Web3

class contract:
    def create(duckPath):
        con=duckdb.connect(database=duckPath,read_only=False)     
        a="id BIGINT PRIMARY KEY"
        b="address VARCHAR"
        c="bytecode VARCHAR"
        d="block_number BIGINT"
        e="FOREIGN KEY (id) REFERENCES transactions(id)"
        con.sql(f"CREATE TABLE contracts({a},{b},{c},{d},{e})")
        con.sql(f"CREATE SEQUENCE contract_id START 0 MINVALUE 0")
        con.close()

    def drop(duckPath):
        con=duckdb.connect(database=duckPath,read_only=False)
        con.sql("DROP TABLE IF EXISTS contracts")
        con.close()

    def insert(duckPath,apiPath):
        con=duckdb.connect(database=duckPath, read_only=False)
        w3=Web3(Web3.HTTPProvider(f'{apiPath}'))

        amount=con.sql("SELECT COUNT(*) FROM transactions").fetchone()[0]

        for i in range(amount):
            hash=con.sql(f"SELECT hash FROM transactions WHERE id = {i}").fetchone()[0]

            receipt=w3.eth.get_transaction_receipt(hash)
            
            contractAddress=receipt.get("contractAddress")
            blockNumber=receipt.get("blockNumber")
            
            if Web3.isAddress(contractAddress):
                bytecode=w3.eth.get_code(contractAddress).hex()

                con.sql(f"""
                        INSERT INTO contracts (id,address, bytecode, block_number)
                        VALUES (nextval('contract_id'), '{contractAddress}', '{bytecode}', {blockNumber});

                """)
            else:
                print("erro")
            
        con.close()

    def delete(duckPath):
        con=duckdb.connect(database=duckPath,read_only=False)
        con.sql(f"DELETE FROM contracts")
        con.close()

