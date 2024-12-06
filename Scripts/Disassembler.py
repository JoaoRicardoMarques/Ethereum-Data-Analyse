import subprocess
import duckdb
import re
import os

class disassembler:
    def writer(duckPath):
        con = duckdb.connect(database=duckPath, read_only=True)
        bytecodes = con.sql("SELECT bytecode, id FROM contracts WHERE bytecode != '0x'").fetchall()
        con.close()

        paths = []

        for (bytecode, id) in bytecodes:
            
            arquivePath = f"/home/joao/Ethereum-Data-Analyse/Assembly/bytecode{id}.asm"

            bytecode = bytecode.replace("0x","",1)

            with open(arquivePath, "w") as file:
                file.write(bytecode)

            paths.append((arquivePath, id))

        return paths

    def disassembler(duckPath):
        arquivePaths=disassembler.writer(duckPath)

        con=duckdb.connect(database=duckPath,read_only=False)

        opcodes = con.sql("SELECT name, id FROM opcodes").fetchall()

        for (arquivePath, id) in arquivePaths:
            run = subprocess.run(['evm','disasm',arquivePath],capture_output=True,text=True)
            result = run.stdout

            for (opcode, idOpcode) in opcodes:
                source = rf'\b{opcode}\b'
                query = len(re.findall(source,result))

                if query>0:
                    con.sql(f"""
                            INSERT INTO contracts_have_opcodes(id_contract,id_opcodes,Qntd)
                            VALUES ('{id}','{idOpcode}','{query}');
                    """)
            os.remove(arquivePath)
        

