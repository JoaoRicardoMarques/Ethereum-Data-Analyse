class opcode_list:
    def list1():
        instructions = [
        ("0", "STOP"),
        ("1", "ADD"),
        ("2", "MUL"),
        ("3", "SUB"),
        ("4", "DIV"),
        ("5", "SDIV"),
        ("6", "MOD"),
        ("7", "SMOD"),
        ("8", "ADDMOD"),
        ("9", "MULMOD"),
        ("10", "EXP"),
        ("11", "SIGNEXTEND"),
        ("12", "LT"),
        ("13", "GT"),
        ("14", "SLT"),
        ("15", "SGT"),
        ("16", "EQ"),
        ("17", "ISZERO"),
        ("18", "AND"),
        ("19", "OR"),
        ("20", "XOR"),
        ("21", "NOT"),
        ("22", "BYTE"),
        ("23", "SHL"),
        ("24", "SHR"),
        ("25", "SAR"),
        ("26", "SHA3"),
        ("27", "ADDRESS"),
        ("28", "BALANCE"),
        ("29", "ORIGIN"),
        ("30", "CALLER"),
        ("31", "CALLVALUE"),
        ("32", "CALLDATALOAD"),
        ("33", "CALLDATASIZE"),
        ("34", "CALLDATACOPY"),
        ("35", "CODESIZE"),
        ("36", "CODECOPY"),
        ("37", "GASPRICE"),
        ("38", "EXTCODESIZE"),
        ("39", "EXTCODECOPY"),
        ("40", "RETURNDATASIZE"),
        ("41", "RETURNDATACOPY"),
        ("42", "EXTCODEHASH"),
        ("43", "BLOCKHASH"),
        ("44", "COINBASE"),
        ("45", "TIMESTAMP"),
        ("46", "NUMBER"),
        ("47", "DIFFICULTY"),
        ("48", "GASLIMIT"),
        ("49", "CHAINID"),
        ("50", "SELFBALANCE"),
        ("51", "BASEFEE"),
        ("52", "POP"),
        ("53", "MLOAD"),
        ("54", "MSTORE"),
        ("55", "MSTORE8"),
        ("56", "SLOAD"),
        ("57", "SSTORE"),
        ("58", "JUMP"),
        ("59", "JUMPI"),
        ("60", "PC"),
        ("61", "MSIZE"),
        ("62", "GAS"),
        ("63", "JUMPDEST"),
        ("64", "PUSH1"),
        ("65", "PUSH2"),
        ("66", "PUSH3"),
        ("67", "PUSH4"),
        ("68", "PUSH5"),
        ("69", "PUSH6"),
        ("70", "PUSH7"),
        ("71", "PUSH8"),
        ("72", "PUSH9"),
        ("73", "PUSH10"),
        ("74", "PUSH11"),
        ("75", "PUSH12"),
        ("76", "PUSH13"),
        ("77", "PUSH14"),
        ("78", "PUSH15"),
        ("79", "PUSH16"),
        ("80", "PUSH17"),
        ("81", "PUSH18"),
        ("82", "PUSH19"),
        ("83", "PUSH20"),
        ("84", "PUSH21"),
        ("85", "PUSH22"),
        ("86", "PUSH23"),
        ("87", "PUSH24"),
        ("88", "PUSH25"),
        ("89", "PUSH26"),
        ("90", "PUSH27"),
        ("91", "PUSH28"),
        ("92", "PUSH29"),
        ("93", "PUSH30"),
        ("94", "PUSH31"),
        ("95", "PUSH32"),
        ("96", "DUP1"),
        ("97", "DUP2"),
        ("98", "DUP3"),
        ("99", "DUP4"),
        ("100", "DUP5"),
        ("101", "DUP6"),
        ("102", "DUP7"),
        ("103", "DUP8"),
        ("104", "DUP9"),
        ("105", "DUP10"),
        ("106", "DUP11"),
        ("107", "DUP12"),
        ("108", "DUP13"),
        ("109", "DUP14"),
        ("110", "DUP15"),
        ("111", "DUP16"),
        ("112", "SWAP1"),
        ("113", "SWAP2"),
        ("114", "SWAP3"),
        ("115", "SWAP4"),
        ("116", "SWAP5"),
        ("117", "SWAP6"),
        ("118", "SWAP7"),
        ("119", "SWAP8"),
        ("120", "SWAP9"),
        ("121", "SWAP10"),
        ("122", "SWAP11"),
        ("123", "SWAP12"),
        ("124", "SWAP13"),
        ("125", "SWAP14"),
        ("126", "SWAP15"),
        ("127", "SWAP16"),
        ("128", "LOG0"),
        ("129", "LOG1"),
        ("130", "LOG2"),
        ("131", "LOG3"),
        ("132", "LOG4"),
        ("133", "CREATE"),
        ("134", "CALL"),
        ("135", "CALLCODE"),
        ("136", "RETURN"),
        ("137", "DELEGATECALL"),
        ("138", "CREATE2"),
        ("139", "STATICCALL"),
        ("140", "REVERT"),
        ]
        return instructions
    
    def list2():
        intructions = [
                ("0", "00"),
                ("1", "01"),
                ("2", "02"),
                ("3", "03"),
                ("4", "04"),
                ("5", "05"),
                ("6", "06"),
                ("7", "07"),
                ("8", "08"),
                ("9", "09"),
                ("10", "0A"),
                ("11", "0B"),
                ("12", "10"),
                ("13", "11"),
                ("14", "12"),
                ("15", "13"),
                ("16", "14"),
                ("17", "15"),
                ("18", "16"),
                ("19", "17"),
                ("20", "18"),
                ("21", "19"),
                ("22", "1A"),
                ("23", "1B"),
                ("24", "1C"),
                ("25", "1D"),
                ("26", "20"),
                ("27", "30"),
                ("28", "31"),
                ("29", "32"),
                ("30", "33"),
                ("31", "34"),
                ("32", "35"),
                ("33", "36"),
                ("34", "37"),
                ("35", "38"),
                ("36", "39"),
                ("37", "3A"),
                ("38", "3B"),
                ("39", "3C"),
                ("40", "3D"),
                ("41", "3E"),
                ("42", "3F"),
                ("43", "40"),
                ("44", "41"),
                ("45", "42"),
                ("46", "43"),
                ("47", "44"),
                ("48", "45"),
                ("49", "46"),
                ("50", "47"),
                ("51", "48"),
                ("52", "50"),
                ("53", "51"),
                ("54", "52"),
                ("55", "53"),
                ("56", "54"),
                ("57", "55"),
                ("58", "56"),
                ("59", "57"),
                ("60", "58"),
                ("61", "59"),
                ("62", "5A"),
                ("63", "5B"),
                ("64", "60"),
                ("65", "61"),
                ("66", "62"),
                ("67", "63"),
                ("68", "64"),
                ("69", "65"),
                ("70", "66"),
                ("71", "67"),
                ("72", "68"),
                ("73", "69"),
                ("74", "6A"),
                ("75", "6B"),
                ("76", "6C"),
                ("77", "6D"),
                ("78", "6E"),
                ("79", "6F"),
                ("80", "70"),
                ("81", "71"),
                ("82", "72"),
                ("83", "73"),
                ("84", "74"),
                ("85", "75"),
                ("86", "76"),
                ("87", "77"),
                ("88", "78"),
                ("89", "79"),
                ("90", "7A"),
                ("91", "7B"),
                ("92", "7C"),
                ("93", "7D"),
                ("94", "7E"),
                ("95", "7F"),
                ("96", "80"),
                ("97", "81"),
                ("98", "82"),
                ("99", "83"),
                ("100", "84"),
                ("101", "85"),
                ("102", "86"),
                ("103", "87"),
                ("104", "88"),
                ("105", "89"),
                ("106", "8A"),
                ("107", "8B"),
                ("108", "8C"),
                ("109", "8D"),
                ("110", "8E"),
                ("111", "8F"),
                ("112", "90"),
                ("113", "91"),
                ("114", "92"),
                ("115", "93"),
                ("116", "94"),
                ("117", "95"),
                ("118", "96"),
                ("119", "97"),
                ("120", "98"),
                ("121", "99"),
                ("122", "9A"),
                ("123", "9B"),
                ("124", "9C"),
                ("125", "9D"),
                ("126", "9E"),
                ("127", "9F"),
                ("128", "A0"),
                ("129", "A1"),
                ("130", "A2"),
                ("131", "A3"),
                ("132", "A4"),
                ("133", "F0"),
                ("134", "F1"),
                ("135", "F2"),
                ("136", "F3"),
                ("137", "F4"),
                ("138", "F5"),
                ("139", "FA"),
                ("140", "FD"),  
        ]
        return intructions