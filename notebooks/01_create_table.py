# Databricks notebook source
# MAGIC %md
# MAGIC # NFCom Export PoC — Step 1: Create Delta Table & Load Mock Data
# MAGIC
# MAGIC Creates the `nfcom_data` Delta table with **Liquid Clustering** on
# MAGIC `(EMPRESA, UF, ANO, MES)` and populates it with realistic mock NFCom XML
# MAGIC records conforming to the NF-Com 1.00 schema (NT 2025.001 RTC 1.14).

# COMMAND ----------
# DBTITLE 1, Parameters

CATALOG = "ericos_catalog"
SCHEMA  = "godata"
TABLE   = "nfcom_data"
VOLUME  = "xml_exports"

# COMMAND ----------
# DBTITLE 1, Create Catalog / Schema / Volume

spark.sql(f"USE CATALOG `{CATALOG}`")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`.`{VOLUME}`")
print(f"Schema  : {CATALOG}.{SCHEMA}")
print(f"Volume  : /Volumes/{CATALOG}/{SCHEMA}/{VOLUME}")

# COMMAND ----------
# DBTITLE 1, Create Delta Table with Liquid Clustering
# Liquid Clustering on (EMPRESA, UF, ANO, MES) is the best choice here because:
#   • The filter predicates always include these columns
#   • Liquid Clustering is adaptive — no manual OPTIMIZE with ZORDER needed
#   • It handles skew better than static partitioning for variable-cardinality columns
#   • For 1B records the automatic clustering keeps data co-located for fast scans

spark.sql(f"""
CREATE TABLE IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`.`{TABLE}` (
  ID            STRING  NOT NULL COMMENT 'UUID surrogate key',
  EMPRESA       STRING  NOT NULL COMMENT 'CNPJ of the telecommunications company',
  UF            STRING  NOT NULL COMMENT 'Brazilian state abbreviation (e.g. SP, RJ)',
  ANO           INT     NOT NULL COMMENT 'Reference year',
  MES           INT     NOT NULL COMMENT 'Reference month (1–12)',
  DIA           INT     NOT NULL COMMENT 'Reference day (1–31)',
  CHAVE_ACESSO  STRING           COMMENT '44-digit NFCom access key',
  NFCOM         STRING           COMMENT 'Full NFCom XML payload'
)
CLUSTER BY (EMPRESA, UF, ANO, MES)
COMMENT 'NFCom — Nota Fiscal Fatura de Comunicação — PoC table'
""")

print(f"Table ready: {CATALOG}.{SCHEMA}.{TABLE}")

# COMMAND ----------
# DBTITLE 1, Reference Data

import random, uuid, math

# Telecommunications companies (emitentes)
EMPRESAS = [
    {"cnpj": "00394460007202", "nome": "VIVO S.A."},
    {"cnpj": "02558157000162", "nome": "CLARO S.A."},
    {"cnpj": "04206050000080", "nome": "TIM S.A."},
    {"cnpj": "33000118000179", "nome": "OI S.A."},
    {"cnpj": "76535764000143", "nome": "SERCOMTEL S.A."},
]

# IBGE state codes
UFS_IBGE = {
    "SP": "35", "RJ": "33", "MG": "31", "RS": "43", "PR": "41",
    "SC": "42", "BA": "29", "GO": "52", "DF": "53", "CE": "23",
    "PE": "26", "ES": "32", "MT": "51", "MS": "50", "PA": "15",
    "MA": "21", "AM": "13", "PI": "22", "RN": "24", "AL": "27",
}

# Sample municipality per UF (cod_ibge, name)
MUNICIPIOS = {
    "SP": ("3550308", "Sao Paulo"),
    "RJ": ("3304557", "Rio de Janeiro"),
    "MG": ("3106200", "Belo Horizonte"),
    "RS": ("4314902", "Porto Alegre"),
    "PR": ("4106902", "Curitiba"),
    "SC": ("4205407", "Florianopolis"),
    "BA": ("2927408", "Salvador"),
    "GO": ("5208707", "Goiania"),
    "DF": ("5300108", "Brasilia"),
    "CE": ("2304400", "Fortaleza"),
    "PE": ("2611606", "Recife"),
    "ES": ("3205309", "Vitoria"),
    "MT": ("5103403", "Cuiaba"),
    "MS": ("5002704", "Campo Grande"),
    "PA": ("1501402", "Belem"),
    "MA": ("2111300", "Sao Luis"),
    "AM": ("1302603", "Manaus"),
    "PI": ("2211001", "Teresina"),
    "RN": ("2408102", "Natal"),
    "AL": ("2704302", "Maceio"),
}

PLANOS = [
    ("PLAN001", "Plano Fibra 100MB",  89.90),
    ("PLAN002", "Plano Fibra 300MB", 119.90),
    ("PLAN003", "Plano Fibra 500MB", 149.90),
    ("PLAN004", "Plano Movel 15GB",   59.90),
    ("PLAN005", "Plano Movel 30GB",   79.90),
    ("PLAN006", "Plano Corporativo", 299.90),
]

# COMMAND ----------
# DBTITLE 1, XML Generator

def gerar_nfcom_xml(empresa: dict, uf: str, ano: int, mes: int, dia: int, seq: int) -> tuple:
    """
    Returns (chave_acesso, xml_string) for one NFCom record.
    The XML conforms to http://www.portalfiscal.inf.br/nfcom schema v1.00.
    """
    cnpj       = empresa["cnpj"]
    nome_emit  = empresa["nome"]
    cuf        = UFS_IBGE.get(uf, "35")
    cod_mun, nome_mun = MUNICIPIOS.get(uf, ("3550308", "Sao Paulo"))

    serie  = random.randint(1, 3)
    nnf    = seq
    cnf    = random.randint(1000000, 9999999)
    cdv    = random.randint(0, 9)
    n_cont = random.randint(1000000, 9999999)

    # Date/time
    hora   = f"{random.randint(0,23):02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}"
    data_r = f"{ano}-{mes:02d}-{dia:02d}"
    dh_emi = f"{data_r}T{hora}-03:00"

    # Pricing
    plano_cod, plano_nome, v_item = random.choice(PLANOS)
    v_item   = round(v_item + random.uniform(-10, 10), 2)
    v_bc     = round(v_item * 0.9200, 2)
    p_icms   = round(random.uniform(7.0, 12.0), 2)
    v_icms   = round(v_bc * p_icms / 100, 2)

    # Build 44-digit access key (simplified — not running modulo-11 for PoC)
    aamm     = f"{str(ano)[2:]}{mes:02d}"
    raw_key  = f"{cuf}{aamm}{cnpj}{62:02d}{serie:03d}{nnf:09d}0{cnf:07d}{cdv}"
    chave    = raw_key[:44].ljust(44, "0")

    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<NFCom xmlns="http://www.portalfiscal.inf.br/nfcom">
  <infNFCom Id="NFCom{chave}">
    <ide>
      <cUF>{cuf}</cUF>
      <tpAmb>1</tpAmb>
      <mod>62</mod>
      <serie>{serie}</serie>
      <nNF>{nnf}</nNF>
      <cNF>{cnf:07d}</cNF>
      <cDV>{cdv}</cDV>
      <dhEmi>{dh_emi}</dhEmi>
      <tpEmis>1</tpEmis>
      <nSiteAutoriz>0</nSiteAutoriz>
      <cMunFG>{cod_mun}</cMunFG>
      <finNFCom>0</finNFCom>
      <tpFat>0</tpFat>
      <verProc>1.0.0</verProc>
    </ide>
    <emit>
      <CNPJ>{cnpj}</CNPJ>
      <xNome>{nome_emit}</xNome>
      <enderEmit>
        <xLgr>Av. das Telecomunicacoes</xLgr>
        <nro>1000</nro>
        <xBairro>Centro</xBairro>
        <cMun>{cod_mun}</cMun>
        <xMun>{nome_mun}</xMun>
        <CEP>01310100</CEP>
        <UF>{uf}</UF>
        <fone>1130000000</fone>
      </enderEmit>
      <IE>{random.randint(100000000, 999999999)}</IE>
    </emit>
    <dest>
      <CPF>{random.randint(10000000000, 99999999999):011d}</CPF>
      <xNome>CLIENTE {seq:07d}</xNome>
      <enderDest>
        <xLgr>Rua das Flores</xLgr>
        <nro>{random.randint(1, 9999)}</nro>
        <xBairro>Jardim</xBairro>
        <cMun>{cod_mun}</cMun>
        <xMun>{nome_mun}</xMun>
        <CEP>{random.randint(10000000, 99999999):08d}</CEP>
        <UF>{uf}</UF>
      </enderDest>
    </dest>
    <detPlano>
      <assinante>
        <nContrato>{n_cont}</nContrato>
        <dContratoIni>2020-01-01</dContratoIni>
        <dContratoPer>{data_r}</dContratoPer>
        <tpServUtil>1</tpServUtil>
        <CNPJ_Oper>{cnpj}</CNPJ_Oper>
      </assinante>
    </detPlano>
    <det nItem="1">
      <prod>
        <cProd>{plano_cod}</cProd>
        <xProd>{plano_nome}</xProd>
        <cClass>03020104</cClass>
        <CFOP>5307</CFOP>
        <uMed>1</uMed>
        <qFaturada>1.0000</qFaturada>
        <vItem>{v_item:.2f}</vItem>
      </prod>
      <imposto>
        <ICMS>
          <ICMS00>
            <CST>00</CST>
            <vBC>{v_bc:.2f}</vBC>
            <pICMS>{p_icms:.2f}</pICMS>
            <vICMS>{v_icms:.2f}</vICMS>
          </ICMS00>
        </ICMS>
        <PIS>
          <PISAliq>
            <CST>01</CST>
            <vBC>{v_bc:.2f}</vBC>
            <pPIS>0.65</pPIS>
            <vPIS>{round(v_bc * 0.0065, 2):.2f}</vPIS>
          </PISAliq>
        </PIS>
        <COFINS>
          <COFINSAliq>
            <CST>01</CST>
            <vBC>{v_bc:.2f}</vBC>
            <pCOFINS>3.00</pCOFINS>
            <vCOFINS>{round(v_bc * 0.03, 2):.2f}</vCOFINS>
          </COFINSAliq>
        </COFINS>
      </imposto>
    </det>
    <total>
      <vServ>{v_item:.2f}</vServ>
      <vBC>{v_bc:.2f}</vBC>
      <vICMS>{v_icms:.2f}</vICMS>
      <vFCP>0.00</vFCP>
      <vPIS>{round(v_bc * 0.0065, 2):.2f}</vPIS>
      <vCOFINS>{round(v_bc * 0.03, 2):.2f}</vCOFINS>
      <vRetTrib>0.00</vRetTrib>
      <vNF>{v_item:.2f}</vNF>
      <vDescIncond>0.00</vDescIncond>
    </total>
    <infAdic>
      <infCpl>NF-Com — Periodo de Referencia {mes:02d}/{ano}</infCpl>
    </infAdic>
  </infNFCom>
</NFCom>"""

    return chave, xml.strip()

# COMMAND ----------
# DBTITLE 1, Generate Mock Records

# Coverage: 5 companies × 10 UFs × 2 years × 12 months × 5 days × 20 rec/day = 12 000
# Adjust the multipliers below to scale the dataset.
UFS_SAMPLE   = list(UFS_IBGE.keys())          # all 20 UFs
ANOS         = [2024, 2025]
DIAS_PER_MES = [1, 5, 10, 15, 20]             # 5 days per month
RECS_PER_DAY = 20                             # records per (empresa, uf, ano, mes, dia)

records = []
seq = 1

for emp in EMPRESAS:
    for uf in UFS_SAMPLE:
        for ano in ANOS:
            for mes in range(1, 13):
                for dia in DIAS_PER_MES:
                    for _ in range(RECS_PER_DAY):
                        chave, xml = gerar_nfcom_xml(emp, uf, ano, mes, dia, seq)
                        records.append({
                            "ID":           str(uuid.uuid4()),
                            "EMPRESA":      emp["cnpj"],
                            "UF":           uf,
                            "ANO":          ano,
                            "MES":          mes,
                            "DIA":          dia,
                            "CHAVE_ACESSO": chave,
                            "NFCOM":        xml,
                        })
                        seq += 1

print(f"Records generated : {len(records):,}")

# COMMAND ----------
# DBTITLE 1, Write to Delta Table

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema_spark = StructType([
    StructField("ID",           StringType(),  False),
    StructField("EMPRESA",      StringType(),  False),
    StructField("UF",           StringType(),  False),
    StructField("ANO",          IntegerType(), False),
    StructField("MES",          IntegerType(), False),
    StructField("DIA",          IntegerType(), False),
    StructField("CHAVE_ACESSO", StringType(),  True),
    StructField("NFCOM",        StringType(),  True),
])

df = spark.createDataFrame(records, schema=schema_spark)

(df.write
   .mode("overwrite")
   .option("overwriteSchema", "true")
   .saveAsTable(f"`{CATALOG}`.`{SCHEMA}`.`{TABLE}`"))

count = spark.table(f"`{CATALOG}`.`{SCHEMA}`.`{TABLE}`").count()
print(f"Table {CATALOG}.{SCHEMA}.{TABLE} — rows: {count:,}")

# COMMAND ----------
# DBTITLE 1, Optimize (trigger first Liquid Clustering pass)

spark.sql(f"OPTIMIZE `{CATALOG}`.`{SCHEMA}`.`{TABLE}`")
print("OPTIMIZE completed — Liquid Clustering applied.")

# COMMAND ----------
# DBTITLE 1, Verify Sample

display(
    spark.sql(f"""
        SELECT EMPRESA, UF, ANO, MES, DIA, CHAVE_ACESSO,
               LEFT(NFCOM, 200) AS NFCOM_PREVIEW
        FROM `{CATALOG}`.`{SCHEMA}`.`{TABLE}`
        LIMIT 5
    """)
)
