import duckdb
import subprocess
import os

def create_block(duckPath):
    con=duckdb.connect(database=duckPath,read_only=False)
    
    ç= 'id bigint primary key'
    a= 'number bigint'
    b= 'hash varchar'
    c= 'parent_hash varchar'
    d= 'nonce varchar'
    e= 'sha3_uncles varchar'
    f= 'logs_bloom varchar'
    g= 'transactions_root varchar'
    h= 'state_root varchar'
    i= 'receipts_root varchar'
    j= 'miner varchar'
    k= 'difficulty varchar'
    l= 'total_difficulty varchar'
    m= 'size bigint'
    n= 'extra_data varchar'
    o= 'gas_limit bigint'
    p= 'timestamp bigint'
    q= 'transaction_count bigint'
    r= 'base_fee_per_gas bigint'
    s= 'withdrawals_root varchar'
    t= 'withdrawals varchar'
    

    con.sql(f"CREATE TABLE blocks({ç},{a},{b},{c},{d},{e},{f},{g},{h},{i},{j},{k},{l},{m},{n},{o},{p},{q},{r},{s},{t})")
    con.close()

def drop_block(duckPath):
    con=duckdb.connect(database=duckPath,read_only=False)
    con.sql("DROP TABLE blocks")
    con.close()

def insert_block(duckPath,arquivePath):
    con=duckdb.connect(database=duckPath,read_only=False)
    result1=con.sql("SELECT COUNT(*) FROM blocks").fetchone()
    amount=result1[0]
    
    Ç = 'id'
    Z = f'row_number() OVER () + {amount-1} AS id'
    A = 'number'
    B = 'hash'
    C = 'parent_hash'
    D = 'nonce'
    E = 'sha3_uncles'
    F = 'logs_bloom'
    G = 'transactions_root'
    H = 'state_root'
    I = 'receipts_root'
    J = 'miner'
    K = 'difficulty'
    L = 'total_difficulty'
    M = 'size'
    N = 'extra_data'
    O = 'gas_limit'
    P = 'timestamp'
    Q = 'transaction_count'
    R = 'base_fee_per_gas'
    S = 'withdrawals_root'
    T = 'withdrawals'
    U = 'blob_gas_used'
    V = 'excess_blob_gas'
   

    con.sql(f"INSERT INTO blocks({Ç},{A},{B},{C},{D},{E},{F},{G},{H},{I},{J},{K},{L},{M},{N},{O},{P},{Q},{R},{S},{T}) SELECT {Z},{A},{B},{C},{D},{E},{F},{G},{H},{I},{J},{K},{L},{M},{N},{O},{P},{Q},{R},{S},{T} FROM read_csv_auto('{arquivePath}')")
    con.close()
    ##check_block(duckPath)

def check_block(duckPath):
    con=duckdb.connect(database=duckPath,read_only=False)
    amount=con.sql("SELECT COUNT(*) FROM blocks").fetchone()[0]
    errors=con.sql('SELECT COUNT(*) FROM blocks WHERE id != number').fetchone()[0]
    con.close()
    if errors>0:
        delete_block(duckPath,amount-10000,amount)
        print("erro")
    else:
        print('dados inseridos com sucesso')

def delete_block(duckPath,interval1,interval2):
    con=duckdb.connect(database=duckPath,read_only=False)
    con.sql(f"DELETE FROM blocks WHERE id >= {interval1} AND id <= {interval2};")
    con.close()

def show_block(duckPath,interval1,interval2):
    con=duckdb.connect(database=duckPath,read_only=False)
    result=con.sql(f"SELECT id,number FROM blocks WHERE id >= {interval1} AND id <= {interval2}").fetchall()
    for i in result:
        print(i)
    con.close()

def block_interface():


    duckPath='~/Ethereum Data Analyse/Database/ETH-450000-499999.db'

    print("ESTE SCRIPT TEM A FUNÇÃO DA MANIPULAÇÃO DA TABLE 'blocks' DA DATABASE")
    print()
    print("insira '1' para criar a table 'blocks'")
    print("insira '2' para excluir a table 'blocks'")
    print("insira '3' para inserir valores na table 'blocks'")
    print("insira '4' para excluir valores da table 'blocks'")
    print("insira '5' para mostrar valores da table 'blocks'")
    print()

    function=int(input("> "))
    if function == 1:
        print()
        print("CRIANDO TABLE ......")
        create_block(duckPath)
    if function == 2:
        print()
        print("EXCLUINDO TABLE ......")
        drop_block(duckPath)
    if function == 3:
        print()
        arquivePath=input("Insira o local onde os arquivos extraídos estão armazenados:")
        print()
        print("INSERINDO TUBLAS ......")
        insert_block(duckPath,arquivePath)
    if function == 4:
        print()
        print("Insira o intervalo a ser deletado:")
        interval1=int(input("> "))
        interval2=int(input("> "))
        delete_block(duckPath,interval1,interval2)
    if function == 5:
        print()
        print("Insira o intervalo a ser mostrado:")
        interval1=int(input("> "))
        interval2=int(input("> "))
        show_block(duckPath,interval1,interval2)
    if function == 6:
        print()
        print("Insira a quantidade de arquivos a serem inseridos:")
        amount=int(input("> "))
        initial_block=int(input("> "))
        final_block=initial_block+9999
        for i in range(0,amount):
            if 9999<initial_block<99999:
                arquivePath=f"/home/joao/ethereum-contracts-data-extract/data/csvs/blocks-00000{initial_block}-00000{final_block}.csv"
            if 99999<initial_block<999999:
                arquivePath=f"/home/joao/ethereum-contracts-data-extract/data/csvs/blocks-0000{initial_block}-0000{final_block}.csv"
            if 999999<initial_block<9999999:
                arquivePath=f"/home/joao/ethereum-contracts-data-extract/data/csvs/blocks-000{initial_block}-000{final_block}.csv"
        
            insert_block(duckPath,arquivePath)
            initial_block+=10000
            final_block+=10000
if __name__ == "__main__":
    ##duckPath='~/Ethereum Data Analyse/Database/ethereum_database.db'
    block_interface()
    