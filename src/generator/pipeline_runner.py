import time
import subprocess
import sys

INTERVALO = 60
LOTE_SIZE = 1

def rodar(script):
    result = subprocess.run([sys.executable, script], capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Erro em {script}:\n{result.stderr}")
    else:
        print(result.stdout.strip())

lote = 1
print(f"Pipeline streaming — 1 NF-e por minuto\n")

while True:
    print(f"{'='*40}")
    print(f"Lote #{lote} — {time.strftime('%H:%M:%S')}")
    print(f"{'='*40}")
    rodar("src/generator/nfe_generator.py")
    rodar("src/processor/shadow_calculator.py")
    rodar("src/storage/s3_writer.py")
    print(f"\nPróxima NF-e em {INTERVALO}s...\n")
    lote += 1
    time.sleep(INTERVALO)
