import cx_Oracle
import sys
import time
import threading

def execute_insert(romaneio):
    # Configurações da conexão com o banco de dados
    dsn = cx_Oracle.makedsn("192.168.0.42", 1521, service_name="POTIGUAR")
    username = "integra"
    password = "integra"

    # SQL com o placeholder para o romaneio
    sql = """
        INSERT INTO EPORTAL.AUDITITEM (ROMANEIO,COD_PRODUTO,DESCRICAO,EAN_PRODUTO,QTD_ITENS,QTD_CONFERIDA,CONFERIDO,QTD_AUDITADA,AUDITADO,ID,EMB,CREATED_AT,UPDATED_AT, VEICULO)
        SELECT ROMANEIO, COD_PRODUTO, DESCRICAO, NULL, QTD_ITENS, NULL, 'N',NULL,'N', SQ_EPORTAL_AUDT_ITEM.NEXTVAL, EMB, SYSDATE, SYSDATE, VEICULO FROM (
        (SELECT ROMANEIO, COD_PRODUTO, VEICULO, DESCRICAO, SUM(QTDE) AS QTD_ITENS, EMB FROM (
        SELECT *
        FROM (
        SELECT
            Z.ROMANEIO AS ROMANEIO
            ,P.PRODUTO  AS COD_PRODUTO
            ,z.CARRIER AS VEICULO
            ,PR.T076_DESCRICAO AS DESCRICAO
            ,CASE WHEN X.QTDNAOENTREGUE IS NULL THEN (P.QUANTIDADE/P.MINIMO_MULTIPLO_EMB)
                WHEN X.QTDNAOENTREGUE = p.QUANTIDADE AND X.ENTREGUE = 'N' THEN (P.QUANTIDADE/P.MINIMO_MULTIPLO_EMB)	
                ELSE X.QTDNAOENTREGUE
                END AS QTDE
            ,CASE WHEN X.EMB_QTD_NAO_ENT IS NOT NULL THEN X.EMB_QTD_NAO_ENT
                WHEN X.EMB_QTD_NAO_ENT IS NULL AND P.EMB_QTD_TOTAL = 'M2' THEN 'CX'
                WHEN X.QTDNAOENTREGUE = p.QUANTIDADE AND P.EMB_QTD_TOTAL = 'M2' THEN 'CX'
                ELSE P.EMB_QTD_TOTAL
                END AS EMB
        FROM EPORTAL.PACKING_LIST Z
            INNER JOIN EPORTAL.PACKING_LIST_X_PRODUCTS P
                ON P.ROMANEIO = Z.ROMANEIO
                AND P.DAV = Z.DAV
                AND P.STORE = Z.STORE
                AND P.PREINVOICE = Z.PREINVOICE
                AND (P.ENTREGUE IS NULL OR P.ENTREGUE = 'N')
            LEFT JOIN DBAMDATA.T076_PRODUTO PR
                ON PR.T076_PRODUTO_IU = P.PRODUTO
            LEFT JOIN DBAWMS.TW501_PRODUTO_EMBALAGEM N
                ON N.TW501_PRODUTO_IE = P.PRODUTO
                AND N.TW501_BLOQUEADA = 'N'
            LEFT JOIN (
                        SELECT * FROM (
                        SELECT *
                        FROM EPORTAL.PACKING_LIST_X_PRODUCTS C
                        WHERE 1=1
                        ORDER BY C.UPDATED_AT DESC
                        )
                    ) X ON X.STORE = Z.STORE
                AND X.DAV = Z.DAV
                AND X.PREINVOICE = Z.PREINVOICE
                AND X.ROMANEIO <> Z.ROMANEIO
                AND X.PRODUTO = P.PRODUTO
                AND ROWNUM <= 1
        WHERE Z.D_E_L_E_T_ = 0
        AND Z.ROMANEIO = :romaneio
        )
        WHERE QTDE > 0
        GROUP BY ROMANEIO, COD_PRODUTO, VEICULO, DESCRICAO, EMB, QTDE)
        GROUP BY ROMANEIO, COD_PRODUTO, VEICULO, DESCRICAO, EMB))
    """

    try:
        # Conexão com o banco de dados
        with cx_Oracle.connect(username, password, dsn) as connection:
            with connection.cursor() as cursor:
                # Executar a consulta com o romaneio fornecido
                cursor.execute(sql, romaneio=romaneio)
                connection.commit()
                print("\nDados inseridos com sucesso!")
    except cx_Oracle.DatabaseError as e:
        print("\nErro ao acessar o banco de dados:", e)

def start_timer():
    start_time = time.time()
    while True:
        elapsed_time = int(time.time() - start_time)
        sys.stdout.write(f"\rScript rodando... Tempo decorrido: {elapsed_time} segundos")
        sys.stdout.flush()
        time.sleep(1)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Por favor, informe o romaneio como argumento.")
        print("Uso: python programa.py <romaneio>")
    else:
        romaneio = sys.argv[1]

        # Iniciar o contador em uma thread separada
        timer_thread = threading.Thread(target=start_timer, daemon=True)
        timer_thread.start()

        # Executar o script principal
        execute_insert(romaneio)
