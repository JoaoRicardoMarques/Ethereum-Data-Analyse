import duckdb
import subprocess
import os

def create_transactions(duckPath):
    con=duckdb.connect(database=duckPath,read_only=False)

    a = 'id bigint primary key'
    b = 'id_block bigint'
    c = 'foreign key (id_block) references blocks(id)'
    d = 'hash VARCHAR'
    e = 'nonce BIGINT'
    f = 'block_hash VARCHAR'
    g = 'block_number BIGINT'
    h = 'transaction_index BIGINT'
    i = 'from_address VARCHAR'
    j = 'to_address VARCHAR'
    k = 'value VARCHAR'
    l = 'gas BIGINT'
    m = 'gas_price BIGINT'
    n = 'input VARCHAR'
    o = 'block_timestamp BIGINT'
    p = 'max_fee_per_gas BIGINT'
    q = 'max_priority_fee_per_gas BIGINT'
    r = 'transaction_type BIGINT'

    con.sql(f"CREATE TABLE transactions({ça},{çb},{a},{b},{c},{d},{e},{f},{g},{h},{i},{j},{k},{l},{m},{n},{o},{çc})")
    con.sql(f"CREATE TABLE brute_transactions({a},{b},{c},{d},{e},{f},{g},{h},{i},{j},{k},{l},{m},{n},{o})")

    con.close()

def drop_transactions(duckPath):
    con=duckdb.connect(database=duckPath,read_only=False)
    con.sql("DROP TABLE transactions")
    con.sql("DROP TABLE brute_transactions")
    con.close()

def insert_transactions(duckPath, arquivePath):
    con = duckdb.connect(database=duckPath, read_only=False)

    ça = 'id'
    çb = 'id_block'
    a = 'hash'
    b = 'nonce'
    c = 'block_hash'
    d = 'block_number'
    e = 'transaction_index'
    f = 'from_address'
    g = 'to_address'
    h = 'value'
    i = 'gas'
    j = 'gas_price'
    k = 'input'
    l = 'block_timestamp'
    m = 'max_fee_per_gas'
    n = 'max_priority_fee_per_gas'
    o = 'transaction_type'
    
    con.sql(f"INSERT INTO brute_transactions({a},{b},{c},{d},{e},{f},{g},{h},{i},{j},{k},{l},{m},{n},{o}) SELECT * FROM read_csv_auto('{arquivePath}')")
    
    con.sql(f"DELETE FROM brute_transactions WHERE to_address IS NOT NULL")
    
    amount=con.sql(f"SELECT COUNT(*) FROM brute_transactions").fetchone

    sequence=f'row_number() OVER () + {amount-1} AS id'

    con.sql(f"INSERT INTO transactions ({ça},{çb},{a},{b},{c},{d},{e},{f},{g},{h},{i},{j},{k},{l},{m},{n},{o}) SELECT {sequence},{result2},{a},{b},{c},{d},{e},{f},{g},{h},{i},{j},{k},{l},{m},{n},{o} FROM brute_transactions")

    con.close()
    delete_transactions(duckPath,0)

def delete_transactions(duckPath,function):
    con=duckdb.connect(database=duckPath,read_only=False)
    if function==0:
        con.sql("DELETE FROM brute_transactions")
    else:
        print("Insira o intervalo a ser deletado:")
        interval1=int(input("> "))
        interval2=int(input("> "))
        con.sql(f"DELETE FROM transactions WHERE id>={interval1} AND id<={interval2}")

def interface_transactions():
    duckPath='~/Ethereum Data Analyse/Database/ethereum_database.db'

    print("ESTE SCRIPT TEM A FUNÇÃO DA MANIPULAÇÃO DA TABLE 'transactions' DA DATABASE")
    print()
    print("insira '1' para criar a table 'transactions'")
    print("insira '2' para excluir a table 'transactions'")
    print("insira '3' para inserir valores na table 'transactions'")
    print("insira '4' para excluir valores da table 'transactions'")
    print("insira '5' para mostrar valores da table 'transactions'")
    print()

    function=int(input("> "))
    if function == 1:
        print()
        print("CRIANDO TABLE ......")
        create_transactions(duckPath)
    if function == 2:
        print()
        print("EXCLUINDO TABLE ......")
        drop_transactions(duckPath)
    if function == 3:
        print()
        arquivePath=input("Insira o local onde os arquivos extraídos estão armazenados:")
        print()
        print("INSERINDO TUBLAS ......")
        insert_transactions(duckPath,arquivePath)
    if function == 4:
        print()
        delete_transactions(duckPath,1)
    if function == 5:
        print()
        print("Insira o intervalo a ser mostrado:")
        interval1=int(input("> "))
        interval2=int(input("> "))
      

if __name__ == "__main__":
    
    interface_transactions