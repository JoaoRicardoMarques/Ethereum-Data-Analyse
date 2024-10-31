import duckdb
import subprocess
import os

class contractor:
    def hash(duckPath):
        con=duckdb.connect(database=duckPath,read_only=False)
        con.sql("COPY (SELECT hash FROM transactions) TO '~/Ethereum Data Analyse/Testes/hashes.txt' (HEADER FALSE)")
        con.close()

    def receipts():
        a = "ethereumetl"
        b = "export_receipts_and_logs"
        c = "--transaction-hashes"
        d = os.path.expanduser("~/Ethereum Data Analyse/Testes/hashes.txt")
        e = "--provider-uri"
        f = "https://mainnet.infura.io/v3/a1396b66469549e8af981d99a0316269"
        g = "--receipts-output"
        i = os.path.expanduser("~/Ethereum Data Analyse/Testes/receipts.csv")
        subprocess.run([a, b, c, d, e, f, g, i],capture_output=False,text=True)

    def contract_address():
        con=duckdb.connect(database=':memory:',read_only=False)
        input="~/Ethereum Data Analyse/Testes/receipts.csv"
        output="~/Ethereum Data Analyse/Testes/contract_address.txt"
        con.sql(f"COPY (SELECT contract_address FROM read_csv_auto('{input}')) TO '{output}' (HEADER FALSE)")
        con.close()

    def contracts():
        a = "ethereumetl"
        b = "export_contracts"
        c = "--contract-addresses"
        d = os.path.expanduser("~/Ethereum Data Analyse/Testes/contract_address.txt")
        e = "--provider-uri"
        f = "https://mainnet.infura.io/v3/a1396b66469549e8af981d99a0316269"
        g = "--output"
        i = os.path.expanduser("~/Ethereum Data Analyse/Testes/contracts.csv")
        subprocess.run([a, b, c, d, e, f, g, i],capture_output=False,text=True)

    def contractor(duckPath):
        contractor.hash(duckPath)
        contractor.receipts()
        contractor.contract_address()
        contractor.contracts()

