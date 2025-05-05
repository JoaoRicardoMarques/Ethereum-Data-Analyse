import duckdb
import pandas as pd
import numpy as np
from Block_Table import blocks
from Transaction_Table import transactions
from Contract_Table import contract
from Opcodes_Table import opcodes
from ContractHaveOpcodes_Table import contracts_have_opcodes

duckPath = "/home/joao/Ethereum-Data-Analyse/Database/dataset.db"
con = duckdb.connect(database=duckPath, read_only=False)

df = pd.read_csv('/home/joao/Ethereum-Data-Analyse/Data/blocks.csv', na_values=['nan'])

#blocks.create(duckPath)

for row in df.itertuples(index=False, name=None):
    cleaned = tuple(None if isinstance(x, float) and np.isnan(x) else x for x in row)
    con.execute("""
        INSERT INTO blocks (
            number, 
            hash, 
            parent_hash, 
            nonce, 
            sha3_uncles, 
            logs_bloom,
            transactions_root, 
            state_root, 
            receipts_root, 
            miner, difficulty,
            total_difficulty, 
            size, 
            extra_data, 
            gas_limit, 
            gas_used, 
            timestamp,
            transaction_count, 
            base_fee_per_gas, 
            withdrawals_root, 
            withdrawals
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, cleaned)

df = pd.read_csv('/home/joao/Ethereum-Data-Analyse/Data/transactions.csv', na_values=['nan'])

#transactions.create(duckPath)

id = con.sql("SELECT COUNT(*) FROM transactions").fetchone()[0]

for row in df.itertuples(index=False, name=None):
    cleaned = tuple(None if isinstance(x, float) and np.isnan(x) else x for x in row)
    con.execute(f"""
        INSERT INTO transactions (
            id,
            hash,
            nonce,
            block_hash,
            block_number, 
            transaction_index,
            from_address,
            to_address,
            value,
            gas,
            gas_price, 
            input,
            block_timestamp, 
            max_fee_per_gas,
            max_priority_fee_per_gas, 
            transaction_type 
        ) VALUES ({id},?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, cleaned)
    id=id+1

df = pd.read_csv('/home/joao/Ethereum-Data-Analyse/Data/contracts.csv', na_values=['nan'])

#contract.create(duckPath)

id = con.sql("SELECT COUNT(*) FROM contracts").fetchone()[0]

for row in df.itertuples(index=False, name=None):
    cleaned = tuple(None if isinstance(x, float) and np.isnan(x) else x for x in row)
    con.execute(f"""
        INSERT INTO contracts (
            id,
            address,
            bytecode,
            block_number
        ) VALUES ({id},?, ?, ?)
    """, cleaned)
    id=id+1



