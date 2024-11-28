import cx_Oracle
import sys
import json
from datetime import datetime

def fetch_data(romaneio):
    # Configurações da conexão com o banco de dados
    dsn = cx_Oracle.makedsn("192.168.0.42", 1521, service_name="POTIGUAR")
    username = "integra"
    password = "integra"

    # SQL para buscar os dados
    sql = "SELECT * FROM EPORTAL.AUDITITEM WHERE ROMANEIO = :romaneio"

    try:
        # Conexão com o banco de dados
        with cx_Oracle.connect(username, password, dsn) as connection:
            with connection.cursor() as cursor:
                # Executar a consulta
                cursor.execute(sql, romaneio=romaneio)
                
                # Obter os nomes das colunas
                columns = [col[0] for col in cursor.description]
                
                # Obter os dados como lista de dicionários
                rows = [
                    {columns[i]: (row[i].strftime("%Y-%m-%d %H:%M:%S") if isinstance(row[i], datetime) else row[i])
                     for i in range(len(row))}
                    for row in cursor.fetchall()
                ]
                
                # Formatar como JSON
                formatted_json = json.dumps(rows, indent=4, ensure_ascii=False)
                
                print("\nDados encontrados:")
                print(formatted_json)
    except cx_Oracle.DatabaseError as e:
        print("Erro ao acessar o banco de dados:", e)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Por favor, informe o romaneio como argumento.")
        print("Uso: python programa.py <romaneio>")
    else:
        romaneio = sys.argv[1]
        fetch_data(romaneio)
