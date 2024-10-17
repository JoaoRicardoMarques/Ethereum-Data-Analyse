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

    con.sql(f"CREATE TABLE transactions({a},{b},{c},{d},{e},{f},{g},{h},{i},{j},{k},{l},{m},{n},{o},{p},{q},{r})")
    con.sql(f"CREATE TABLE brute_transactions({d},{e},{f},{g},{h},{i},{j},{k},{l},{m},{n},{o},{p},{q},{r})")

    con.close()

def drop_transactions(duckPath):
    con=duckdb.connect(database=duckPath,read_only=False)
    con.sql("DROP TABLE transactions")
    con.sql("DROP TABLE brute_transactions")
    con.close()

def insert_transactions(duckPath, arquivePath):
    con = duckdb.connect(database=duckPath, read_only=False)

    a = 'id'
    b = 'id_block'
    c = 'hash'
    d = 'nonce'
    e = 'block_hash'
    f = 'block_number'
    g = 'transaction_index'
    h = 'from_address'
    i = 'to_address'
    j = 'value'
    k = 'gas'
    l = 'gas_price'
    m = 'input'
    n = 'block_timestamp'
    o = 'max_fee_per_gas'
    p = 'max_priority_fee_per_gas'
    q = 'transaction_type'

    
    A = 'brute_transactions.hash'
    B = 'brute_transactions.nonce'
    C = 'brute_transactions.block_hash'
    D = 'brute_transactions.block_number'
    E = 'brute_transactions.transaction_index'
    F = 'brute_transactions.from_address'
    G = 'brute_transactions.to_address'
    H = 'brute_transactions.value'
    I = 'brute_transactions.gas'
    J = 'brute_transactions.gas_price'
    K = 'brute_transactions.input'
    L = 'brute_transactions.block_timestamp'
    M = 'brute_transactions.max_fee_per_gas'
    N = 'brute_transactions.max_priority_fee_per_gas'
    O = 'brute_transactions.transaction_type'

    con.sql(f"INSERT INTO brute_transactions({c},{d},{e},{f},{g},{h},{i},{j},{k},{l},{m},{n},{o},{p},{q}) SELECT * FROM read_csv_auto('{arquivePath}')")

    con.sql(f"DELETE FROM brute_transactions WHERE to_address IS NOT NULL OR input = '0x'")

    amount = con.sql(f"SELECT COUNT(*) FROM transactions").fetchone()[0]

    sequence = f'row_number() OVER () + {amount-1} AS id'

    con.sql(f"INSERT INTO transactions ({a},{b},{c},{d},{e},{f},{g},{h},{i},{j},{k},{l},{m},{n},{o},{p},{q}) SELECT {sequence},blocks.id,{A},{B},{C},{D},{E},{F},{G},{H},{I},{J},{K},{L},{M},{N},{O} FROM brute_transactions INNER JOIN blocks ON blocks.id = brute_transactions.block_number")

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
        con.sql(f"DELETE FROM transactions WHERE id_block>={interval1} AND id_block<={interval2}")

def show_transactions(duckPath,interval1,interval2):
    con=duckdb.connect(database=duckPath,read_only=False)
    result=con.sql(f"SELECT id,id_block FROM transactions WHERE id_block >= {interval1} AND id_block <= {interval2}").fetchall()
    for i in result:
        print(i)
    con.close()

def interface_transactions():
    duckPath='~/Ethereum Data Analyse/Database/ETH-0-49999.db'

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
        show_transactions(duckPath,interval1,interval2)
      

if __name__ == "__main__":
    interface_transactions()
    