import duckdb
import subprocess

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
    j= 'minerdifficulty varchar'
    k= 'total_difficulty varchar'
    l= 'size bigint'
    m= 'extra_data varchar'
    n= 'gas_limit bigint'
    o= 'timestamp bigint'
    p= 'transaction_count bigint'
    q= 'base_fee_per_gas bigint'
    r= 'withdrawals_root varchar'
    s= 'withdrawals varchar'
    t= 'blob_gas_used bigint'
    u= 'excess_blob_gas bigint'
    v= ''

    con.sql(f"CREATE TEABLE blocks({ç},{a},{b},{c},{d},{e},{f},{g},{h},{i},{j},{k},{l},{m},{n},{o},{p},{q},{r},{s},{t},{u},{v})")
    con.close()
def drop_block(duckPath):
    con=duckdb.connect(database=duckPath,read_only=False)
    con.sql("DROP TABLE blocks")
    con.close()
def insert_block(duckPath,arquivePath):
    con=duckdb.connect(database=duckPath,read_only=False)
    result=con.sql("SELECT COUNT(*) FROM blocks").fetchone()
    amount=result[0]
    
    Ç = 'id'
    Z = f'row_number() OVER () + {amount+1} AS id'
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
if __name__ == "__main__":
    
    duckPath='~/Área de Trabalho/Ethereum_Dataset/Databases/Ethereum_Database'
    inicio=0
    fim=190
    initial=2000000
    final=2009999
    for i in range(inicio,fim):
        
        print(f'ethereum-contracts-data-extract/data/csvs/blocks-000{initial}-000{final}.csv')
        arquivePath=f'~/ethereum-contracts-data-extract/data/csvs/blocks-000{initial}-000{final}.csv'
        insert_block(duckPath,arquivePath)
        initial=initial+10000
        final=final+10000