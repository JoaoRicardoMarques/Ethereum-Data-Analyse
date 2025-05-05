COPY
	(SELECT 
		number,
		hash,
		parent_hash,
		nonce,
		sha3_uncles, 
		logs_bloom ,
		transactions_root, 
		state_root,
		receipts_root, 
		miner,
		difficulty, 
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
	FROM 
		blocks
	ORDER BY 
		number)
TO
	"/home/joao/Ethereum-Data-Analyse/Data/blocks.csv"
	(HEADER, DELIMITER ",")
;
COPY
	(SELECT 
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
	FROM 
		transactions
	ORDER BY
		id)
TO
	"/home/joao/Ethereum-Data-Analyse/Data/transactions.csv"
	(HEADER, DELIMITER ",")
;
COPY
	(SELECT 
		address,
		bytecode,
		block_number
	FROM 
		contracts
	ORDER BY 
		id)
TO
	"/home/joao/Ethereum-Data-Analyse/Data/contracts.csv"
	(HEADER, DELIMITER ",")
;
