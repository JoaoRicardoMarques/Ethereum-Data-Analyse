import duckdb
import subprocess
import os

class contractor:
    def hash(duckPath,initialBlock):
        finalBlock=initialBlock+49999
        con=duckdb.connect(database=duckPath,read_only=False)
        con.sql(f"COPY (SELECT hash FROM transactions) TO '~/Ethereum Data Analyse/Data/Hashes/hashes-{initialBlock}-{finalBlock}.txt' (HEADER FALSE)")
        con.close()

    def receipts(initialBlock):
        finalBlock=initialBlock+49999
        a = "ethereumetl"
        b = "export_receipts_and_logs"
        c = "--transaction-hashes"
        d = os.path.expanduser(f"~/Ethereum Data Analyse/Data/Hashes/hashes-{initialBlock}-{finalBlock}.txt")
        e = "--provider-uri"
        f = "https://mainnet.infura.io/v3/a1396b66469549e8af981d99a0316269"
        g = "--receipts-output"
        i = os.path.expanduser(f"~/Ethereum Data Analyse/Data/Receipts/receipts-{initialBlock}-{finalBlock}.csv")
        subprocess.run([a, b, c, d, e, f, g, i],capture_output=False,text=True)

    def contract_address(initialBlock):
        finalBlock=initialBlock+49999
        con=duckdb.connect(database=':memory:',read_only=False)
        input=f"~/Ethereum Data Analyse/Data/Receipts/receipts-{initialBlock}-{finalBlock}.csv"
        output=f"~/Ethereum Data Analyse/Data/Contracts-Address/contract_address-{initialBlock}-{finalBlock}.txt"
        con.sql(f"COPY (SELECT contract_address FROM read_csv_auto('{input}')) TO '{output}' (HEADER FALSE)")
        con.close()

    def contracts(initialBlock):
        finalBlock=initialBlock+49999
        a = "ethereumetl"
        b = "export_contracts"
        c = "--contract-addresses"
        d = os.path.expanduser(f"~/Ethereum Data Analyse/Data/Contracts-Address/contract_address-{initialBlock}-{finalBlock}.txt")
        e = "--provider-uri"
        f = "https://mainnet.infura.io/v3/a1396b66469549e8af981d99a0316269"
        g = "--output"
        i = os.path.expanduser(f"~/Ethereum Data Analyse/Data/Contracts/contracts-{initialBlock}-{finalBlock}.csv")
        subprocess.run([a, b, c, d, e, f, g, i],capture_output=False,text=True)

    def contractor(duckPath):
        contractor.hash(duckPath)
        contractor.receipts()
        contractor.contract_address()
        contractor.contracts()

duckPath="/home/joao/Ethereum Data Analyse/Database/database-50000-99999.db"
initial = 50000
contractor.contracts(initial)