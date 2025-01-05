from Block_Table import blocks
from Transaction_Table import transactions
from Contract_Table import contract
from Opcodes_Table import opcodes
from ContractHaveOpcodes_Table import contracts_have_opcodes

database = input("nome da database:")

initial = int(input("bloco inicial:"))
final = initial + 9999

duckPath = f'/home/joao/Ethereum-Data-Analyse/Database/{database}.db'
apiPath="https://mainnet.infura.io/v3/a1396b66469549e8af981d99a0316269"

contract.insert(duckPath,apiPath)
opcodes.insert(duckPath)
contracts_have_opcodes.insert(duckPath)