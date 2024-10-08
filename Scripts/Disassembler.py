import subprocess
import duckdb
import sys

#escreve o input dentro de arquivos para a evm ler 
def writer(arquivo, input):
    with open(arquivo, 'w') as file:
        file.write(input)

def input_EVM(arquivo):
    subprocess.run(['evm','disasm',arquivo],capture_output=True,text=True)

def output_EVM():
    subprocess

#retorna o input de dentro da contractCall_Database
def disassembler(duckPath1,duckPath2,id):
    con = duckdb.connect(database=duckPath2,read_only=False)

    input = con.sql(f"SELECT input FROM ContractCALL WHERE id={id}")

    arquivo = 'bitecode{id}.asm'

    writer(arquivo,input)

    input_EVM(arquivo)

    output_EVM

    con.close()

if __name__ == "__main__":
    duckPath1='~/Área de Trabalho/Ethereum_Dataset/Ethereum_Database'
    duckPath2='~/Área de Trabalho/Ethereum_Dataset/ContractCall_Database'

    con=duckdb.connect(database=duckPath2,read_only=False)

    limite = con.sql(f"SELECT COUNT(*) FROM ContractCALL")

    for i in limite:
        disassembler()
    

    con.close()


