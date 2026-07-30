import sys
import subprocess
import datetime

def generate_months(start_str, end_str):
    start = datetime.datetime.strptime(start_str, "%Y-%m")
    end = datetime.datetime.strptime(end_str, "%Y-%m")
    
    current = start
    months = []
    while current <= end:
        months.append(current.strftime("%Y-%m"))
        # Add a month
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)
    return months

def main():
    if len(sys.argv) < 3:
        print("Uso: python3 run_range_updates.py YYYY-MM YYYY-MM")
        print("Exemplo: python3 run_range_updates.py 2023-05 2026-07")
        sys.exit(1)
        
    start_month = sys.argv[1]
    end_month = sys.argv[2]
    
    try:
        months = generate_months(start_month, end_month)
    except ValueError as e:
        print("Erro no formato dos meses. Use YYYY-MM.")
        sys.exit(1)
        
    print(f"Período selecionado: {start_month} até {end_month}")
    print(f"Meses a serem processados ({len(months)}): {', '.join(months)}")
    
    python_interpreter = sys.executable
    script_path = "code/ETL_incremental_dados_RFB.py"
    
    for i, month in enumerate(months, 1):
        print("\n" + "="*60)
        print(f"PROCESSANDO MÊS ({i}/{len(months)}): {month}")
        print("="*60 + "\n")
        
        try:
            # Invoca o script incremental passando o mês como argumento com python unbuffered
            result = subprocess.run(
                [python_interpreter, "-u", script_path, month],
                check=True
            )
            print(f"\nMês {month} finalizado com sucesso!")
        except subprocess.CalledProcessError as e:
            print(f"\n[ERRO] Ocorreu uma falha ao processar o mês {month}: {e}")
            print("Interrompendo a execução em lote para evitar inconsistências.")
            sys.exit(1)
            
    print("\nExecução em lote concluída com sucesso para todo o período!")

if __name__ == "__main__":
    main()
