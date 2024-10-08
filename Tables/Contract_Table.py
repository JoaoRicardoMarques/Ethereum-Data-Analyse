import duckdb
import sys

def create_Contract(duckPath):
    con=duckdb.connect(database=duckPath,read_only=False)


def drop_Contract(duckPath):
    con=duckdb.connect(database=duckPath,read_only=False)
    con.sql("DROP TABLE contracts")
    con.close()
def insert_Contract(duckPath):
    con=duckdb.connect(database=duckPath,read_only=False)


if __name__ == "__main__":
    duckPath='~/Área de Trabalho/Ethereum_Dataset/Ethereum_Database'
   