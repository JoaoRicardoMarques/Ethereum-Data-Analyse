from list import opcode_list
import duckdb
import re

def disasm(id_contract, bytecode, duckPath):
    list_opcodes = opcode_list.list2()  # Obtém a lista com o id e o opcode

    con = duckdb.connect(database=duckPath, read_only=False)  # Conexão com o banco

    regex = re.compile(r'([0-9a-fA-F]{2})')
    opcodes = regex.findall(bytecode[2:])  # Remove o '0x' do bytecode

    for instruction in list_opcodes:
        i = 0
        amount = 0  # Contador

        while i < len(opcodes):
            opcode = opcodes[i].lower()

            if opcode == instruction[1]:
                amount += 1

                if opcode.startswith("6") or opcode.startswith("7"):  
                    push_size = int(opcode, 16) - 0x5f  # Calcula quantos bytes pular
                    i += push_size  # Pula os bytes do argumento do PUSHx

            i += 1  
        if amount != 0:
            con.sql(f"INSERT INTO contracts_have_opcodes(id_contract, id_opcodes, Qntd) VALUES ('{id_contract}', '{instruction[0]}', '{amount}')")
