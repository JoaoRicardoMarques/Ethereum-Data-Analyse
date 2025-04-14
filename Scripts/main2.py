import duckdb

duckPath="/home/joao/Ethereum-Data-Analyse/Database/blocks-3200000-3599999.db"

con=duckdb.connect(database=duckPath,read_only=False)


b = 'hash VARCHAR'
c = 'nonce BIGINT'
d = 'block_hash VARCHAR'
e = 'block_number BIGINT'
g = 'transaction_index BIGINT'
h = 'from_address VARCHAR'
i = 'to_address VARCHAR'
j = 'value VARCHAR'
k = 'gas BIGINT'
l = 'gas_price BIGINT'
m = 'input VARCHAR'
n = 'block_timestamp BIGINT'
o = 'max_fee_per_gas BIGINT'
p = 'max_priority_fee_per_gas BIGINT'
q = 'transaction_type BIGINT'

con.sql(f"CREATE TABLE transactions_of_blocks_0003590000_0003599999({b},{c},{d},{e},{g},{h},{i},{j},{k},{l},{m},{n},{o},{p},{q})")