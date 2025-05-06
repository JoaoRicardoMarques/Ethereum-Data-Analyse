import duckdb
from ContractHaveOpcodes_Table import contracts_have_opcodes
from Block_Table import blocks
from Transaction_Table import transactions
from Contract_Table import contract
from Opcodes_Table import opcodes
duckPath = "~/Ethereum-Data-Analyse/Database/teste2.db"

blocks.create(duckPath)
transactions.create(duckPath)
contract.create(duckPath)
opcodes.create(duckPath)
contracts_have_opcodes.create(duckPath)
opcodes.insert(duckPath)

con=duckdb.connect(database=duckPath,read_only=False)
blockPath="~/Ethereum-Data-Analyse/Database/blocks.csv"
transactionPath="~/Ethereum-Data-Analyse/Database/transactions.csv"
contractPath="~/Ethereum-Data-Analyse/Database/contracts.csv"
con.sql(f"INSERT INTO blocks SELECT * FROM read_csv_auto('{blockPath}')")
con.sql(f"INSERT INTO transactions SELECT * FROM read_csv_auto('{transactionPath}')")
con.sql(f"INSERT INTO contracts SELECT * FROM read_csv_auto('{contractPath}')")
con.close()

contracts_have_opcodes.insert(duckPath)