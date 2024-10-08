import subprocess

def contar_palavra_no_output(comando, palavra):
    # Executa o comando no terminal e captura o output
    resultado = subprocess.run(comando, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    
    # Verifica se houve algum erro ao executar o comando
    if resultado.returncode != 0:
        print(f"Erro ao executar o comando: {resultado.stderr}")
        return 0
    
    # Conta a quantidade de vezes que a palavra aparece no output
    contagem = resultado.stdout.lower().count(palavra.lower())
    
    return contagem

# Exemplo de uso
comando = "evm disasm teste.asm"  # Substitua pelo comando que deseja executar
palavra = "ADD"  # Substitua pela palavra que deseja contar
contagem = contar_palavra_no_output(comando, palavra)

print(f"A palavra '{palavra}' aparece {contagem} vezes no output.")
