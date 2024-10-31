from Block_Table import blocks
from Transaction_Table import transactions
from Contract_Table import contract
from Opcodes_Table import opcodes
from ContractHaveOpcodes_Table import contracts_have_opcodes

duckPath = "/home/joao/Ethereum Data Analyse/Database/Teste.db"
blockPath = "/home/joao/ethereum-contracts-data-extract/data/csvs/blocks-0000050000-0000059999.csv"
transactionPath = "/home/joao/ethereum-contracts-data-extract/data/csvs/transactions-of-blocks-0000050000-0000059999.csv"

contracts_have_opcodes.drop(duckPath)
opcodes.drop(duckPath)
contract.drop(duckPath)
transactions.drop(duckPath)
blocks.drop(duckPath)

blocks.create(duckPath)
transactions.create(duckPath)
contract.create(duckPath)
opcodes.create(duckPath)
contracts_have_opcodes.create(duckPath)

blocks.insert(duckPath,blockPath)
transactions.insert(duckPath,transactionPath)
opcodes.insert(duckPath)
