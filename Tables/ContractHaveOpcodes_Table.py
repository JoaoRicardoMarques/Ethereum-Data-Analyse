import duckdb
import sys

def contracts_have_opcodes(duckpath):
    con=duckdb.connect(database=duckpath, read_only=False)
    A='id VARCHAR PRIMARY KEY'
    B='id_opcodes INT'
    C='Qntd INT'
    D='FOREIGN KEY(hash) REFERENCES (hash)'
    E='FOREIGN KEY(id_opcodes) REFERENCES opcodes(id)'
    con.sql(f"CREATE TABLE codes_tem_opcodes({A},{B},{C},{D},{E})")
    con.close()
if __name__ == "__main__":
    duckpath='~/Área de Trabalho/Ethereum_Dataset/Ethereum_Database'
    contracts_have_opcodes(duckpath)