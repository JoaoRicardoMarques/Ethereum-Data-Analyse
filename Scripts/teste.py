import subprocess
import duckdb
import re

class disassembler: 
    def writer(duckPath):
        con=duckdb.connect(database=duckPath,read_only=True)
        amount=con.sql("SELECT COUNT (*) FROM contracts").fetchone()[0] 
        paths=[]
        for i in range (amount):
            arquivePath=f'/home/joao/Ethereum Data Analyse/Testes/bytecode{i}.asm'
            bytecode=con.sql(f"SELECT bytecode FROM contracts WHERE id = {i}").fetchone()[0]
            with open(arquivePath, 'w') as file:
                file.write(bytecode)
            paths.append((arquivePath,i))
        con.close()
        return paths

    def disassembler(duckPath):
        con=duckdb.connect(database=duckPath,read_only=False)
        arquivePaths=disassembler.writer(duckPath)     
        for arquivePath in arquivePaths:
            result=subprocess.run(['evm','disasm',arquivePath[0]],capture_output=True,text=True)         
            for i in range(140):             
                source=con.sql(f"SELECT name FROM opcodes WHERE id = {i}")             
                query=result.stdout.split().count(str(source))             
                if (query>0):
                    con.sql(f"INSERT INTO contract_have_opcodes VALUES ('{arquivePath[1]}','{i}','{query}')")
        con.close()

duckPath="~/Ethereum Data Analyse/Database/database-50000-99999.db"
disassembler.writer(duckPath)