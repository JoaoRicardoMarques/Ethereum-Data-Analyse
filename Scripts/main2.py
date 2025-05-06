import duckdb
from ContractHaveOpcodes_Table import contracts_have_opcodes
from Block_Table import blocks
from Transaction_Table import transactions
from Contract_Table import contract
from Opcodes_Table import opcodes
duckPath = "~/Ethereum-Data-Analyse/Database/teste.db"

contracts_have_opcodes.insert(duckPath)
