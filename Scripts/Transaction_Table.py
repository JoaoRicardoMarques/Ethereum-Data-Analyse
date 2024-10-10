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
        show_block(duckPath,interval1,interval2)

if __name__ == "__main__":
    
    interface_transactions