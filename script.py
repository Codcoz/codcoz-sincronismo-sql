# Importando bibliotecas necessárias
import os
from dotenv import load_dotenv
import psycopg2
import pandas as pd

# Carregando variáveis de ambiente
load_dotenv()

# Conectando com o banco do origem e de destino
conn_origem = psycopg2.connect(os.getenv("DB_URL_1"))
conn_destino = psycopg2.connect(os.getenv("DB_URL_2"))

cursor_destino = conn_destino.cursor()

#-----------Sincronismo Empresas-----------

# Função para executar o MERGE statement com base em campos que usaremos do primeiro
def upsert_empresa(cnpj, nome, email):
    cursor_destino.execute("""
    MERGE INTO empresa AS t
    USING (VALUES (%s, %s, %s)) AS s(cnpj, nome, email)
    ON t.cnpj = s.cnpj
    WHEN MATCHED THEN
        UPDATE SET nome = s.nome, email = s.email
    WHEN NOT MATCHED THEN
        INSERT (cnpj, nome, email)
        VALUES (s.cnpj, s.nome, s.email);
    """, (cnpj, nome, email))

# Função para executar o sincronismo das empresas
def sincronizar_empresas():

    # Selecionando todas as empresas do banco do primeiro
    sql_query_empresas = """
        SELECT id
             , nome
             , cnpj
             , email
          FROM empresa;
    """
    df_empresas = pd.read_sql_query(sql_query_empresas, conn_origem)

    # Rodando a função de upsert pra cada empresa do primeiro
    for empresa in df_empresas.itertuples(index=False):
        upsert_empresa(empresa.cnpj, empresa.nome, empresa.email)

    # Commitando as alterações
    conn_destino.commit()

# Função para mapear ids entre os bancos, usa como parâmetro o CNPJ da empresa
def get_empresa(cnpj_empresa):
    cursor_destino.execute("SELECT id FROM empresa WHERE cnpj = %s;", (cnpj_empresa, ))
    row = cursor_destino.fetchone()

    return row[0]

#-----------Sincronismo Funcionários-----------

# Função para normalizar o campo 'funcao' da tabela funcionario do primeiro, que se tornará 'funcao_id' na tabela funcionario do segundo
def normalize_funcao(funcao, empresa_id):
    # Tenta selecionar a função baseado em seu nome e empresa_id
    cursor_destino.execute("SELECT id FROM funcao WHERE nome = %s AND empresa_id = %s;", (funcao, empresa_id))
    row = cursor_destino.fetchone()
    
    # Se não existir, insere essa função no banco
    if row:
        return row[0]
    else:
        cursor_destino.execute("INSERT INTO funcao (nome, empresa_id) VALUES (%s, %s) RETURNING id;", (funcao, empresa_id))
        return cursor_destino.fetchone()[0]

# Função para executar o MERGE statement com base em campos que usaremos do primeiro
def upsert_funcionario(nome, sobrenome, funcao, status, email, cnpj_empresa):
    # Mapeando os ids necessários
    empresa_id = get_empresa(cnpj_empresa)
    funcao_id = normalize_funcao(funcao, empresa_id)

    cursor_destino.execute("""
    MERGE INTO funcionario AS t
    USING (VALUES (%s, %s, %s, %s, %s, %s)) AS s(nome, sobrenome, funcao_id, status, email, empresa_id)
    ON t.nome = s.nome AND t.sobrenome = s.sobrenome
    WHEN MATCHED THEN
        UPDATE SET email = s.email, funcao_id = s.funcao_id, status = UPPER(s.status), empresa_id = s.empresa_id
    WHEN NOT MATCHED THEN
        INSERT (nome, sobrenome, funcao_id, status, email, empresa_id)
        VALUES (s.nome, s.sobrenome, s.funcao_id, UPPER(s.status), s.email, s.empresa_id);
    """, (nome, sobrenome, funcao_id, status, email, empresa_id))

# Função para executar o sincronismo dos funcionários
def sincronizar_funcionarios():
    # Selecionando todos os funcionários do banco do primeiro
    sql_query_funcionarios = """
        SELECT f.id
             , f.nome
             , f.sobrenome
             , f.funcao
             , f.status
             , f.email
             , e.cnpj as cnpj_empresa
          FROM funcionario f
          JOIN empresa e ON e.id = f.id_empresa;
    """
    df_funcionarios = pd.read_sql_query(sql_query_funcionarios, conn_origem)

    # Rodando a função de upsert pra cada funcionário do primeiro
    for funcionario in df_funcionarios.itertuples(index=False):
        upsert_funcionario(funcionario.nome, funcionario.sobrenome, funcionario.funcao, funcionario.status, funcionario.email, funcionario.cnpj_empresa)

    # Commitando as alterações
    conn_destino.commit()

#-----------Sincronismo Pedidos/Notas Fiscais, Produtos e Itens do Pedido-----------

def upsert_pedido(cnpj_empresa, cod_nota_fiscal, data_compra):
    # Mapeando o id da empresa
    empresa_id = get_empresa(cnpj_empresa)

    cursor_destino.execute("""
        MERGE INTO pedido AS t
        USING (VALUES (%s, %s, %s)) AS s(empresa_id, cod_nota_fiscal, data_compra)
        ON t.cod_nota_fiscal = s.cod_nota_fiscal
        WHEN MATCHED THEN
            UPDATE SET empresa_id = s.empresa_id, data_compra = s.data_compra
        WHEN NOT MATCHED THEN
            INSERT (empresa_id, cod_nota_fiscal, data_compra)
            VALUES (s.empresa_id, s.cod_nota_fiscal, s.data_compra);
    """, (empresa_id, cod_nota_fiscal, data_compra))

    # Selecionando o id do pedido que acabou de ser alterado/inserido
    cursor_destino.execute("SELECT id FROM pedido WHERE cod_nota_fiscal = %s;", (cod_nota_fiscal, ))
    
    return cursor_destino.fetchone()[0], empresa_id

# Função para normalizar o campo 'unidade_medida' da tabela produto do primeiro, que se tornará 'unidade_medida_id' na tabela produto do segundo
def normalize_unidade_medida(unidade_medida):
    # Tenta selecionar a unidade de medida baseado em sua sigla
    cursor_destino.execute("SELECT id FROM unidade_medida WHERE sigla = %s;", (unidade_medida, ))
    row = cursor_destino.fetchone()

    # Se não existir, insere essa unidade de medida no banco
    if row:
        return row[0]
    else:
        cursor_destino.execute("INSERT INTO unidade_medida (sigla) VALUES (%s) RETURNING id;", (unidade_medida, ))
        return cursor_destino.fetchone()[0]    

# Função para normalizar o campo 'unidade_medida' da tabela produto do primeiro, que se tornará 'unidade_medida_id' na tabela produto do segundo
def normalize_produto(nome_produto, quantidade, unidade_medida, codigo_ean, empresa_id):
    # Tenta selecionar o produto baseado em seu nome e empresa
    cursor_destino.execute("SELECT id FROM produto WHERE codigo_ean = %s AND empresa_id = %s;", (codigo_ean, empresa_id))
    row = cursor_destino.fetchone()
    
    # Se não existir, insere esse produto no banco
    if row:
        return row[0]
    else:
        # Mapeia a unidade de medida
        unidade_medida_id = normalize_unidade_medida(unidade_medida)

        cursor_destino.execute(
            "INSERT INTO produto (nome, quantidade, unidade_medida_id, empresa_id, codigo_ean) VALUES (%s, %s, %s, %s, %s) RETURNING id;",
            (nome_produto, quantidade, unidade_medida_id, empresa_id, codigo_ean)
        )
        return cursor_destino.fetchone()[0]

def inserir_item_pedido(pedido_id, produto_nome, quantidade, unidade_medida, codigo_ean, empresa_id):
    # Mapeia o produto
    produto_id = normalize_produto(produto_nome, quantidade, unidade_medida, codigo_ean, empresa_id)

    cursor_destino.execute("""
        MERGE INTO item_pedido AS t
        USING (VALUES (%s, %s, %s)) AS s(pedido_id, produto_id, quantidade)
        ON t.pedido_id = s.pedido_id AND t.produto_id = s.produto_id
        WHEN MATCHED THEN
            UPDATE SET quantidade = s.quantidade
        WHEN NOT MATCHED THEN
            INSERT (pedido_id, produto_id, quantidade)
            VALUES (s.pedido_id, s.produto_id, s.quantidade);
    """, (pedido_id, produto_id, quantidade))

def sincronizar_notas_e_itens():
    # Selecionando todos as notas fiscais/pedidos do banco do primeiro
    sql_query_nota_fiscal = """
        SELECT nf.id
             , nf.numero_nota  as cod_nota_fiscal  
             , nf.data_emissao as data_compra
             , e.cnpj          as cnpj_empresa 
          FROM nota_fiscal_xml nf
          JOIN empresa e ON e.id = nf.id_empresa;
    """
    df_notas = pd.read_sql_query(sql_query_nota_fiscal, conn_origem)

    # Rodando a função de upsert pra cada nota_fiscal/pedido do primeiro
    for nota in df_notas.itertuples(index=False):
        pedido_id, empresa_id = upsert_pedido(
            nota.cnpj_empresa,
            nota.cod_nota_fiscal,
            nota.data_compra,
        )

        # Selecionando produtos da nota fiscal usando id como parâmetro 
        sql_query_produto = f"SELECT nome, quantidade, unidade_medida, codigo_ean FROM produto WHERE id_nota_fiscal = {nota.id};"
        df_produtos = pd.read_sql_query(sql_query_produto, conn_origem)

    # Rodando a função de upsert pra cada produto/item_pedido do primeiro
        for prod in df_produtos.itertuples(index=False):
            inserir_item_pedido(pedido_id, prod.nome, prod.quantidade, prod.unidade_medida, prod.codigo_ean, empresa_id)

    # Commitando as alterações
    conn_destino.commit()

# Executando o sincronismo respeitando a ordem de relacionamento entre as tabelas escolhidas
sincronizar_empresas()
sincronizar_funcionarios()
sincronizar_notas_e_itens()