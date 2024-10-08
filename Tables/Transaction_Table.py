import duckdb
import subprocess


def create_transactions(duckPath):
    con=duckdb.connect(database=duckPath,read_only=False)

    ça = 'id bigint primary key'
    çb = 'id_block bigint'
    çc = 'foreign key (id_block) references blocks(id)'
    a = 'hash VARCHAR'
    b = 'nonce BIGINT'
    c = 'block_hash VARCHAR'
    d = 'block_number BIGINT'
    e = 'transaction_index BIGINT'
    f = 'from_address VARCHAR'
    g = 'to_address VARCHAR'
    h = 'value VARCHAR'
    i = 'gas BIGINT'
    j = 'gas_price BIGINT'
    k = 'input VARCHAR'
    l = 'block_timestamp BIGINT'
    m = 'max_fee_per_gas BIGINT'
    n = 'max_priority_fee_per_gas BIGINT'
    o = 'transaction_type BIGINT'

    con.sql(f"CREATE TABLE IF NOT EXISTS transactions({ça},{çb},{a},{b},{c},{d},{e},{f},{g},{h},{i},{j},{k},{l},{m},{n},{o},{çc})")
    con.close()

def drop_transactions(duckPath):
    con=duckdb.connect(database=duckPath,read_only=False)
    con.sql("DROP TABLE IF EXISTS transactions")
    con.close()

def insert_transactions(duckPath, blockNumber, amount):
    con = duckdb.connect(database=duckPath, read_only=False)
    con.sql("CREATE SEQUENCE IF NOT EXISTS transaction_id START 1")

    a = 'hash VARCHAR'
    b = 'nonce BIGINT'
    c = 'block_hash VARCHAR'
    d = 'block_number BIGINT'
    e = 'transaction_index BIGINT'
    f = 'from_address VARCHAR'
    g = 'to_address VARCHAR'
    h = 'value VARCHAR'
    i = 'gas BIGINT'
    j = 'gas_price BIGINT'
    k = 'input VARCHAR'
    l = 'block_timestamp BIGINT'
    m = 'max_fee_per_gas BIGINT'
    n = 'max_priority_fee_per_gas BIGINT'
    o = 'transaction_type BIGINT'

    A = 'hash'
    B = 'nonce'
    C = 'block_hash'
    D = 'block_number'
    E = 'transaction_index'
    F = 'from_address'
    G = 'to_address'
    H = 'value'
    I = 'gas'
    J = 'gas_price'
    K = 'input'
    L = 'block_timestamp'
    M = 'max_fee_per_gas'
    N = 'max_priority_fee_per_gas'
    O = 'transaction_type'

    finalBlockNumber = blockNumber + 9999

    for y in range(amount):
        # Corrige o caminho do arquivo, substitua o '~' pelo caminho absoluto
        arquivePath = f'/home/joao/UTFPR-IC/Data_Analysis/Deployment_Transactions_Contracts/DTC-{blockNumber}-{finalBlockNumber}.csv'
        con.sql(f"CREATE TABLE temporaria({a},{b},{c},{d},{e},{f},{g},{h},{i},{j},{k},{l},{m},{n},{o})")
        con.sql(f"INSERT INTO temporaria SELECT * FROM read_csv_auto('{arquivePath}')")

        for z in range(blockNumber, finalBlockNumber):
            block = con.sql(f"SELECT id FROM blocks WHERE id={z}").fetchone()  # Extrai o valor
            if block:
                check = con.sql(f"SELECT COUNT(*) FROM temporaria WHERE block_number={block[0]}").fetchone()[0]
                if check > 0:
                    # Insere diretamente selecionando todos os campos
                    con.sql(f"""
                        INSERT INTO transactions (id, id_block, {A}, {B}, {C}, {D}, {E}, {F}, {G}, {H}, {I}, {J}, {K}, {L}, {M}, {N}, {O}) 
                        SELECT nextval('transaction_id'), {block[0]}, {A}, {B}, {C}, {D}, {E}, {F}, {G}, {H}, {I}, {J}, {K}, {L}, {M}, {N}, {O}
                        FROM temporaria WHERE block_number={block[0]}
                    """)

        con.sql("DROP TABLE temporaria")
        blockNumber += 10000
        finalBlockNumber += 10000

    con.close()
