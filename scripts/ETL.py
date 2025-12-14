""""
import pandas as pd
from datetime import datetime
import sys
import os

# Importer les connexions existantes
from db_connect_source1 import get_source1_connection
from db_connect_source2 import get_source2_files
from db_connect_BI import get_bi_connection
"""


# etl_extraction.py

import pandas as pd
import pyodbc
import os
from db_connect_source1 import get_source1_connection  # Connexion SQL Server
from db_connect_source2 import get_source2_files       # Chemins des fichiers Excel
from db_connect_BI import get_bi_connection
import numpy as np

# ------------------------------
# 1️⃣ EXTRACTION DE LA SOURCE 1 : SQL SERVER
# ------------------------------

def extract_sql_server_data():
   
    print("🚀 Extraction des données depuis SQL Server...")

    # Connexion à SQL Server
    conn = get_source1_connection()
    if not conn:
        raise Exception("❌ Impossible de se connecter à SQL Server")

    # Initialisation dictionnaire pour stocker les DataFrames
    sql_data = {}

    try:
        # --- Orders ---
        sql_orders_query = "SELECT * FROM Orders"
        df_orders = pd.read_sql(sql_orders_query, conn)
        print(f"✅ Orders (SQL Server) : {df_orders.shape[0]} lignes, {df_orders.shape[1]} colonnes")
        sql_data['orders'] = df_orders

        # --- Customers ---
        sql_customers_query = "SELECT * FROM Customers"
        df_customers = pd.read_sql(sql_customers_query, conn)
        print(f"✅ Customers (SQL Server) : {df_customers.shape[0]} lignes, {df_customers.shape[1]} colonnes")
        sql_data['customers'] = df_customers

        # --- Employees ---
        sql_employees_query = "SELECT * FROM Employees"
        df_employees = pd.read_sql(sql_employees_query, conn)
        print(f"✅ Employees (SQL Server) : {df_employees.shape[0]} lignes, {df_employees.shape[1]} colonnes")
        sql_data['employees'] = df_employees



        # --- Region ---
        sql_region_query = "SELECT * FROM Region"
        df_region = pd.read_sql(sql_region_query, conn)
        print(f"✅ Region : {df_region.shape[0]} lignes")
        sql_data['region'] = df_region

# --- Territories ---
        sql_territory_query = "SELECT * FROM Territories"
        df_territory = pd.read_sql(sql_territory_query, conn)
        print(f"✅ Territories : {df_territory.shape[0]} lignes")
        sql_data['territories'] = df_territory

# --- EmployeeTerritories ---
        sql_emp_terr_query = "SELECT * FROM EmployeeTerritories"
        df_emp_terr = pd.read_sql(sql_emp_terr_query, conn)
        print(f"✅ EmployeeTerritories : {df_emp_terr.shape[0]} lignes")
        sql_data['employee_territories'] = df_emp_terr


    finally:
        conn.close()  # Toujours fermer la connexion
        print("🔒 Connexion SQL Server fermée")

    return sql_data

# ------------------------------
# 2️⃣ EXTRACTION DE LA SOURCE 2 : EXCEL
# ------------------------------

def extract_excel_data():
    """
    Récupère les données des fichiers Excel : orders.xlsx, customers.xlsx, employees.xlsx
    Retourne un dictionnaire de DataFrames
    """
    print("\n🚀 Extraction des données depuis Excel...")

    # Obtenir les chemins des fichiers
    files = get_source2_files()
    excel_data = {}

    for key, path in files.items():
        if os.path.exists(path):
            try:
                df = pd.read_excel(path)
                excel_data[key] = df
                print(f"✅ {key.capitalize()} (Excel) : {df.shape[0]} lignes, {df.shape[1]} colonnes")
            except Exception as e:
                print(f"❌ Erreur lecture {key} : {e}")
        else:
            print(f"❌ Fichier non trouvé : {path}")

    return excel_data

# ------------------------------
# 3️⃣ FONCTION PRINCIPALE D'EXTRACTION
# ------------------------------

def main_extraction():
    """
    Extrait toutes les données depuis les sources SQL et Excel
    Retourne deux dictionnaires de DataFrames
    """
    # Extraction SQL Server
    sql_data = extract_sql_server_data()

    # Extraction Excel
    excel_data = extract_excel_data()

    # Petit check : afficher quelques lignes pour vérifier

    """
    for key, df in sql_data.items():
        print(f"\n📌 Preview SQL Server {key}:")
        print(df.head(3))

    for key, df in excel_data.items():
        print(f"\n📌 Preview Excel {key}:")
        print(df.head(3))

   """

    # ------------------------------
# Harmonisation des colonnes Excel
# ------------------------------
    for key, df in excel_data.items():
     if key == 'orders':
        df.rename(columns={
            'Order ID': 'OrderID',
            'Customer ID': 'CustomerID',
            'Employee ID': 'EmployeeID',
            'Order Date': 'OrderDate'
        }, inplace=True)
     elif key == 'customers':
        df.rename(columns={
            'ID': 'CustomerID',
            'Company': 'CompanyName',
            'Last Name': 'ContactName'
        }, inplace=True)
     elif key == 'employees':
        df.rename(columns={
            'ID': 'EmployeeID'
        }, inplace=True)



    key_columns_map = {
    'orders': 'OrderID',
    'customers': 'CustomerID',
    'employees': 'EmployeeID'
    }

   
    # Vérification
    verify_data_consistency(sql_data, excel_data, key_columns_map)


    return sql_data, excel_data





    # ------------------------------
# 4️⃣ FONCTION DE VERIFICATION SQL vs EXCEL
# ------------------------------

def verify_data_consistency(sql_data, excel_data, key_columns_map):
    """
    Compare les DataFrames SQL et Excel pour chaque table.
    Affiche les lignes manquantes dans chaque source.
    
    key_columns_map : dictionnaire { 'table_name' : 'clé_unique' }
    """
    print("\n🔍 Vérification de la correspondance SQL <-> Excel...")

    for table, key_col in key_columns_map.items():
        df_sql = sql_data.get(table)
        df_excel = excel_data.get(table)

        if df_sql is None or df_excel is None:
            print(f"⚠️ Données manquantes pour la table {table}")
            continue

        # Identifier les clés uniques présentes dans SQL mais pas dans Excel
        missing_in_excel = df_sql[~df_sql[key_col].isin(df_excel[key_col])]
        # Identifier les clés uniques présentes dans Excel mais pas dans SQL
        missing_in_sql = df_excel[~df_excel[key_col].isin(df_sql[key_col])]

        print(f"\n📌 Table '{table}' (clé: '{key_col}')")
        print(f"   - Lignes présentes en SQL mais absentes dans Excel : {len(missing_in_excel)}")
        if not missing_in_excel.empty:
            print(missing_in_excel.head())

        print(f"   - Lignes présentes en Excel mais absentes dans SQL : {len(missing_in_sql)}")
        if not missing_in_sql.empty:
            print(missing_in_sql.head())





# ---------- UTIL — harmonisation des colonnes Excel ----------
def harmonize_excel_columns(excel_data):
    """
    Renomme les colonnes Excel pour correspondre aux noms SQL (utiles pour la suite).
    Modifie les DataFrames en place et les retourne.
    """
    for key, df in excel_data.items():
        # Lower-level safety: copy to avoid accidental external mutation if needed
        # (ici on modifie en place pour simplicité)
        if key == 'orders':
            # Reprise des noms vus dans ton extraction
            rename_map = {
                'Order ID': 'OrderID',
                'Customer ID': 'CustomerID',
                'Employee ID': 'EmployeeID',
                'Order Date': 'OrderDate',
                'Shipped Date': 'ShippedDate',
                'Required Date': 'RequiredDate',
                # d'autres colonnes possibles selon le fichier Excel
            }
            df.rename(columns=rename_map, inplace=True)
        elif key == 'customers':
            rename_map = {
                'ID': 'CustomerID',
                'Company': 'CompanyName',
                'Last Name': 'LastName',
                'First Name': 'FirstName',
                'Country/Region': 'CountryRegion'
            }
            df.rename(columns=rename_map, inplace=True)
        elif key == 'employees':
            rename_map = {
                'ID': 'EmployeeID',
                'First Name': 'FirstName',
                'Last Name': 'LastName',
                # selon les colonnes du fichier Excel
            }
            df.rename(columns=rename_map, inplace=True)

    print("🔧 Harmonisation des colonnes Excel terminée.")
    return excel_data


# ---------- BUILD DimDate ----------
def build_dim_date(sql_orders, excel_orders, min_override=None, max_override=None):
    """
    Construit DimDate à partir des colonnes de date présentes dans les ordres (SQL + Excel).
    Retourne DataFrame dim_date avec DateKey (YYYYMMDD int) et colonnes de découpage.
    Si min_override/max_override fournis, on les utilise (utile pour tests).
    """
    print("📅 Construction de DimDate...")

    # Récupérer toutes les colonnes date possibles (OrderDate, ShippedDate, RequiredDate)
    date_cols_sql = []
    for c in ['OrderDate', 'ShippedDate', 'RequiredDate']:
        if c in sql_orders.columns:
            date_cols_sql.append(sql_orders[c])

    date_cols_xls = []
    for c in ['OrderDate', 'ShippedDate', 'RequiredDate']:
        if c in excel_orders.columns:
            date_cols_xls.append(excel_orders[c])

    all_dates = pd.concat(date_cols_sql + date_cols_xls, ignore_index=True) if (date_cols_sql + date_cols_xls) else pd.Series(dtype='datetime64[ns]')

    # Convertir en datetime proprement
    all_dates = pd.to_datetime(all_dates, errors='coerce')
    all_dates = all_dates.dropna()

    if all_dates.empty:
        # Aucun date trouvé ; lever une erreur ou créer une période par défaut
        raise ValueError("Aucune date trouvée dans les sources d'orders pour générer DimDate.")

    start_date = pd.to_datetime(min_override) if min_override is not None else all_dates.min()
    end_date = pd.to_datetime(max_override) if max_override is not None else all_dates.max()

    print(f"   ▶ plage: {start_date.date()} → {end_date.date()}")

    # Générer calendrier complet
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')

    dim_date = pd.DataFrame({
        'DateKey': date_range.strftime('%Y%m%d').astype(int),
        'DateValue': date_range,
        'Year': date_range.year,
        'Quarter': date_range.quarter,
        'Month': date_range.month,
        'MonthName': date_range.strftime('%B'),
        'Day': date_range.day,
        'DayOfWeek': (date_range.dayofweek + 1),  # 1=Monday .. 7=Sunday
    })

    # IsWeekend: Saturday (6) and Sunday (7) -> 1
    dim_date['IsWeekend'] = dim_date['DayOfWeek'].isin([6,7]).astype(int)

    print(f"   ▶ DimDate : {len(dim_date)} lignes générées.")
    return dim_date


# ---------- BUILD DimCustomer ----------
def build_dim_customer(sql_customers, excel_customers):
    """
    Construit DimCustomer en unifiant SQL et Excel.
    - CustomerCode : natural key unifiée (SQL garde son CustomerID string, Excel reçoit préfixe EX_)
    - Sépare Company / LastName / FirstName si possible
    Retourne DataFrame dim_customer avec une clé surrogate CustomerID_local (commence à 1).
    """
    print("👥 Construction de DimCustomer...")

    # Préparer copies
    df_sql = sql_customers.copy()
    df_xls = excel_customers.copy()

    # Normaliser les codes :
    # SQL : CustomerID est souvent une string comme 'ALFKI'
    if 'CustomerID' in df_sql.columns:
        df_sql['CustomerCode'] = df_sql['CustomerID'].astype(str)
    else:
        # fallback si nom différent
        df_sql['CustomerCode'] = df_sql.index.astype(str).apply(lambda x: f"SQL_{x}")

    # Excel : numeric ID -> on préfixe pour éviter conflit (EX_)
    if 'CustomerID' in df_xls.columns:
        df_xls['CustomerCode'] = 'EX_' + df_xls['CustomerID'].astype(str)
    else:
        df_xls['CustomerCode'] = df_xls.index.astype(str).apply(lambda x: f"EX_{x}")

    # Construire colonnes utiles
    # From SQL: CompanyName, ContactName (ou LastName/FirstName)
    def split_contact(name):
        if pd.isna(name):
            return (None, None)
        parts = str(name).strip().split()
        if len(parts) == 1:
            return (parts[0], None)
        # heuristique: last token as last name? we'll take first as first name, rest as last name
        first = parts[0]
        last = " ".join(parts[1:])
        return (last, first)

    rows = []

    # SQL rows
    for _, r in df_sql.iterrows():
        company = r.get('CompanyName') or r.get('Company') or None
        contact = r.get('ContactName') or None
        last, first = split_contact(contact)
        rows.append({
            'CustomerCode': r['CustomerCode'],
            'Company': company,
            'LastName': last,
            'FirstName': first,
            'City': r.get('City'),
            'StateProvince': r.get('Region') or r.get('StateProvince'),
            'CountryRegion': r.get('Country')
        })

    # Excel rows
    for _, r in df_xls.iterrows():
        company = r.get('CompanyName') or r.get('Company') or None
        last = r.get('LastName') if 'LastName' in r.index else None
        first = r.get('FirstName') if 'FirstName' in r.index else None
        rows.append({
            'CustomerCode': r['CustomerCode'],
            'Company': company,
            'LastName': last,
            'FirstName': first,
            'City': r.get('City'),
            'StateProvince': r.get('StateProvince'),
            'CountryRegion': r.get('CountryRegion') or r.get('Country/Region') or r.get('Country')
        })

    df_customers_all = pd.DataFrame(rows)

    # Dé-duplication par CustomerCode (garder la première apparition)
    df_customers_all.drop_duplicates(subset=['CustomerCode'], inplace=True)

    # Réindexer pour créer une clé surrogate locale (CustomerKeyLocal) — utile pour mapping avant insertion
    df_customers_all = df_customers_all.reset_index(drop=True)
    df_customers_all['CustomerID_local'] = df_customers_all.index + 1  # commence à 1

    # Réordonner colonnes pour insertion
    dim_customer = df_customers_all[['CustomerID_local', 'CustomerCode', 'Company', 'LastName', 'FirstName', 'City', 'StateProvince', 'CountryRegion']]

    print(f"   ▶ DimCustomer : {len(dim_customer)} lignes (après unification).")
    return dim_customer


# ---------- BUILD DimEmployee ----------
def build_dim_employee(sql_employees, excel_employees):
    print("👤 Construction de DimEmployee...")

    df_sql = sql_employees.copy()
    df_xls = excel_employees.copy()

    rows = []

    # ========== SQL EMPLOYEES ==========
    if 'EmployeeID' in df_sql.columns:
        for _, r in df_sql.iterrows():
            emp_orig = int(r['EmployeeID'])     # 👈 ID original Northwind SQL
            code = f"EMP_{emp_orig}"

            rows.append({
                'EmployeeID_orig': emp_orig,    # 👈 Sauvegarde ID original
                'EmployeeCode': code,
                'LastName': r.get('LastName'),
                'FirstName': r.get('FirstName'),
                'JobTitle': r.get('Title') or r.get('JobTitle'),
                'City': r.get('City'),
                'CountryRegion': r.get('Country')
            })

    # ========== EXCEL EMPLOYEES ==========
    if 'EmployeeID' in df_xls.columns:
        for _, r in df_xls.iterrows():
            emp_orig = int(r['EmployeeID'])     # même clé naturelle
            code = f"EMP_{emp_orig}"

            rows.append({
                'EmployeeID_orig': emp_orig,
                'EmployeeCode': code,
                'LastName': r.get('LastName'),
                'FirstName': r.get('FirstName'),
                'JobTitle': r.get('Title') or r.get('JobTitle'),
                'City': r.get('City'),
                'CountryRegion': r.get('CountryRegion') or r.get('Country')
            })

    # ========== DÉDOUBLONNAGE & SURROGATE ==========
    df_emps_all = pd.DataFrame(rows)
    df_emps_all.drop_duplicates(subset=['EmployeeCode'], inplace=True)
    df_emps_all = df_emps_all.reset_index(drop=True)
    df_emps_all['EmployeeID_local'] = df_emps_all.index + 1

    dim_employee = df_emps_all[
        ['EmployeeID_local', 'EmployeeID_orig', 'EmployeeCode',
         'LastName', 'FirstName', 'JobTitle', 'City', 'CountryRegion']
    ]

    print(f"   ▶ DimEmployee : {len(dim_employee)} lignes (après unification).")
    return dim_employee

# ---------- BUILD DimOrder ----------
def build_dim_order(sql_orders, excel_orders):
    """
    Construire DimOrder en unifiant SQL et Excel:
    - OrderID (natural key) : on garde tel quel (ton DW attend int)
    - CustomerCode, EmployeeCode (natural keys)
    - OrderDate, ShippedDate, StatusID
    """
    print("📦 Construction de DimOrder (préparation)...")

    rows = []

    # SQL orders
    for _, r in sql_orders.iterrows():
        orderid = r.get('OrderID')
        # SQL CustomerID is typically a code like 'ALFKI'
        cust_code = r.get('CustomerID')
        empid = r.get('EmployeeID')
        emp_code = f"EMP_{int(empid)}" if pd.notna(empid) else None
        rows.append({
            'OrderID': int(orderid),
            'CustomerCode': cust_code,
            'EmployeeCode': emp_code,
            'OrderDate': pd.to_datetime(r.get('OrderDate'), errors='coerce'),
            'ShippedDate': pd.to_datetime(r.get('ShippedDate'), errors='coerce') if 'ShippedDate' in r.index else pd.NaT,
            'StatusID': r.get('StatusID') if 'StatusID' in r.index else None
        })

    # Excel orders
    for _, r in excel_orders.iterrows():
        # Excel OrderID likely numeric small; it's safe as int
        # Map CustomerID -> EX_<id> as earlier for customers
        orderid = r.get('OrderID')
        cust_x = r.get('CustomerID')
        cust_code = f"EX_{int(cust_x)}" if pd.notna(cust_x) else None
        empid = r.get('EmployeeID')
        emp_code = f"EMP_{int(empid)}" if pd.notna(empid) else None
        rows.append({
            'OrderID': int(orderid),
            'CustomerCode': cust_code,
            'EmployeeCode': emp_code,
            'OrderDate': pd.to_datetime(r.get('OrderDate'), errors='coerce'),
            'ShippedDate': pd.to_datetime(r.get('ShippedDate') if 'ShippedDate' in r.index else r.get('Shipped Date'), errors='coerce'),
            'StatusID': r.get('Status ID') if 'Status ID' in r.index else r.get('StatusID')
        })

    df_orders_all = pd.DataFrame(rows)

    # Deduplicate by OrderID if necessary (keep first)
    df_orders_all.drop_duplicates(subset=['OrderID'], inplace=True)

    dim_order = df_orders_all[['OrderID', 'CustomerCode', 'EmployeeCode', 'OrderDate', 'ShippedDate', 'StatusID']]

    print(f"   ▶ DimOrder (préparé) : {len(dim_order)} lignes (après union).")
    return dim_order






def build_dim_region(df_region_sql):
    """
    Build DimRegion with:
    - RegionCode (natural key used for loading)
    - RegionName
    """
    print("🌍 Construction de DimRegion...")

    df = df_region_sql.copy()

    # Harmoniser noms (Northwind SQL a "RegionID" et "RegionDescription")
    if 'RegionDescription' in df.columns:
        df['RegionName'] = df['RegionDescription'].str.strip()
    else:
        df['RegionName'] = df['RegionName']

    # Créer un code naturel propre (exemple : REG_1)
    df['RegionCode'] = df['RegionID'].apply(lambda x: f"REG_{int(x)}")

    dim_region = df[['RegionCode', 'RegionName']].drop_duplicates()

    print(f"   ▶ DimRegion : {len(dim_region)} lignes.")

    return dim_region




def build_dim_territory(df_territory):
    dim_territory = df_territory.copy()
    dim_territory.rename(columns={
        "TerritoryDescription": "TerritoryName",
        "RegionID": "RegionID"
    }, inplace=True)

    dim_territory["TerritoryName"] = dim_territory["TerritoryName"].str.strip()
    dim_territory["TerritoryCode"] = df_territory["TerritoryID"]  # utiliser l'ID d'origine comme code

    # Ne pas garder TerritoryID
    dim_territory = dim_territory[["TerritoryCode", "TerritoryName", "RegionID"]]

    return dim_territory


def build_fact_table(dim_order, dim_customer, dim_employee, dim_date, sql_data):
    """
    Construit la table de faits avec RegionID et TerritoryID.
    """
    df = dim_order.copy()

    # Merge Customer
    df = df.merge(dim_customer[['CustomerID_local', 'CustomerCode']],
                  on='CustomerCode', how='left')

    # Merge Employee
    df = df.merge(dim_employee[['EmployeeID_local', 'EmployeeCode', 'EmployeeID_orig']],
                  on='EmployeeCode', how='left')

    # KPI livraison
    df['OrdersDelivered'] = df['ShippedDate'].notna().astype(int)
    df['OrdersNotDelivered'] = (~df['ShippedDate'].notna()).astype(int)

    # DateKey
    df['DateKey'] = df['OrderDate'].dt.strftime('%Y%m%d').astype('Int64')

    # Merge Territory via EmployeeID_orig
    df_empTerr = sql_data['employee_territories'].copy()
    df_empTerr['EmployeeID'] = df_empTerr['EmployeeID'].astype('Int64')
    df_empTerr['TerritoryID'] = df_empTerr['TerritoryID'].astype('Int64')

    df = df.merge(
        df_empTerr[['EmployeeID', 'TerritoryID']],
        left_on='EmployeeID_orig',
        right_on='EmployeeID',
        how='left'
    )

    # Merge Region via TerritoryID
    dfTerr = sql_data['territories'].copy()
    dfTerr['TerritoryID'] = dfTerr['TerritoryID'].astype('Int64')
    dfTerr['RegionID'] = dfTerr['RegionID'].astype('Int64')

    df = df.merge(
        dfTerr[['TerritoryID', 'RegionID']],
        on='TerritoryID',
        how='left'
    )

    # Table de faits finale
    df_fact = pd.DataFrame({
        'OrderID': df['OrderID'].astype(int),
        'CustomerID': df['CustomerID_local'].astype('Int64'),
        'EmployeeID': df['EmployeeID_local'].astype('Int64'),
        'OrdersDelivered': df['OrdersDelivered'].astype(int),
        'OrdersNotDelivered': df['OrdersNotDelivered'].astype(int),
        'RegionID': df['RegionID'].astype('Int64'),
        'TerritoryID': df['TerritoryID'].astype('Int64'),
        'DateKey': df['DateKey'].astype('Int64')
    })

    print(f"▶ Tabledefait construite : {len(df_fact)} lignes")
    print(f"▶ Region null : {df_fact['RegionID'].isna().sum()}, Territory null : {df_fact['TerritoryID'].isna().sum()}")

    return df_fact







def transform_pipeline(sql_data, excel_data, date_min_override=None, date_max_override=None):
    """
    Orchestrateur complet de transformation.
    Prépare les dimensions et la table de faits.
    """
    print("===== START TRANSFORM PIPELINE =====")

    excel_data = harmonize_excel_columns(excel_data)

    # ------ Dimensions
    dim_customer = build_dim_customer(sql_data['customers'], excel_data['customers'])
    dim_employee = build_dim_employee(sql_data['employees'], excel_data['employees'])
    dim_order = build_dim_order(sql_data['orders'], excel_data['orders'])
    dim_date = build_dim_date(sql_data['orders'], excel_data['orders'],
                              min_override=date_min_override,
                              max_override=date_max_override)
    dim_region = build_dim_region(sql_data['region'])
    dim_territory = build_dim_territory(sql_data['territories'])

    # ------ Fact table
    df_fact = build_fact_table(dim_order, dim_customer, dim_employee, dim_date, sql_data)

    # ------ Retourner les dims + facts
    dims = {
        'dim_date': dim_date,
        'dim_customer': dim_customer,
        'dim_employee': dim_employee,
        'dim_order': dim_order,
        'dim_region': dim_region,
        'dim_territory': dim_territory
    }

    print("===== END TRANSFORM PIPELINE =====")
    return dims, df_fact







# ---------- TESTS ET CONTRÔLES ----------
def run_transformation_tests(dims, df_fact, sql_data, excel_data):
    """
    Exécute une série de tests/contrôles pour valider la transformation.
    Imprime des messages et lève AssertionError si quelque chose d'essentiel est faux.
    """
    print("\n--- RUNNING TRANSFORMATION CHECKS ---")

    dim_date = dims['dim_date']
    dim_customer = dims['dim_customer']
    dim_employee = dims['dim_employee']
    dim_order = dims['dim_order']

    # Test 1: dim_date couvre la plage réelle des OrderDate non-null
    all_order_dates = pd.concat([
        pd.to_datetime(sql_data['orders'].get('OrderDate', pd.Series(dtype='datetime64[ns]')), errors='coerce'),
        pd.to_datetime(excel_data['orders'].get('OrderDate', pd.Series(dtype='datetime64[ns]')), errors='coerce')
    ]).dropna()
    if not all_order_dates.empty:
        min_date = all_order_dates.min().date()
        max_date = all_order_dates.max().date()
        assert dim_date['DateValue'].min().date() <= min_date, "DimDate start > min(order dates)"
        assert dim_date['DateValue'].max().date() >= max_date, "DimDate end < max(order dates)"
        print("✔ DimDate couvre la plage des OrderDate trouvées.")

    # Test 2: unicité des CustomerCode et EmployeeCode
    assert dim_customer['CustomerCode'].is_unique, "CustomerCode doit être unique dans dim_customer"
    assert dim_employee['EmployeeCode'].is_unique, "EmployeeCode doit être unique dans dim_employee"
    print("✔ Unicité : CustomerCode et EmployeeCode OK.")

    # Test 3: Fact lines count matches dim_order (après dédup)
    #assert len(df_fact) == len(dim_order), "Nombre de lignes fact != nombre d'ordres préparés"
    #print("✔ Nombre de lignes dans la table de faits cohérent avec dim_order.")


    # Vérification correcte : 1 OrderID dans dim_order doit exister dans df_fact
    n_unique_fact_orders = df_fact['OrderID'].nunique()
    assert n_unique_fact_orders == len(dim_order), (
    f"Mismatch : orders uniques dans fact={n_unique_fact_orders} vs dim_order={len(dim_order)}"
    )
    print("✔ Concordance : tous les OrderID de dim_order sont dans la fact table.")


    # Test 4: pas de doublons d'OrderID dans dim_order
    assert dim_order['OrderID'].is_unique, "OrderID doit être unique dans dim_order"
    print("✔ OrderID unique dans dim_order.")

    # Test 5: KPI coherence (OrdersDelivered + OrdersNotDelivered == 1)
    sum_kpi = (df_fact['OrdersDelivered'] + df_fact['OrdersNotDelivered']).unique()
    assert all(x == 1 for x in sum_kpi), "Pour chaque ligne, OrdersDelivered + OrdersNotDelivered doit être 1"
    print("✔ KPI consistency OK (OrdersDelivered + OrdersNotDelivered == 1).")

    # Test 6: mapping check - nombre de CustomerID non résolus (NULL) doit être raisonnable
    missing_cust = df_fact['CustomerID'].isna().sum()
    missing_emp = df_fact['EmployeeID'].isna().sum()
    print(f"ℹ CustomerID non mappés dans fact: {missing_cust}")
    print(f"ℹ EmployeeID non mappés dans fact: {missing_emp}")
    # Pas d'assertion ici — dépend des sources ; on avertit simplement

    # Test 7: affichage des top KPI par client / employé / mois/année pour vérification rapide
    # Par client (CustomerCode -> count)
    print("\n📈 Exemple d'agrégats (vérification visuelle) :")
    sample = dim_order.merge(dim_customer[['CustomerID_local','CustomerCode']], on='CustomerCode', how='left')
    sample['Delivered'] = sample['ShippedDate'].notna().astype(int)
    by_cust = sample.groupby('CustomerCode')['Delivered'].agg(['sum','count']).sort_values('sum', ascending=False).head(10)
    print("Top 10 clients par commandes livrées (sum,count):\n", by_cust)

    # Par employé
    sample_emp = dim_order.merge(dim_employee[['EmployeeID_local','EmployeeCode']], on='EmployeeCode', how='left')
    sample_emp['Delivered'] = sample_emp['ShippedDate'].notna().astype(int)
    by_emp = sample_emp.groupby('EmployeeCode')['Delivered'].agg(['sum','count']).sort_values('sum', ascending=False).head(10)
    print("\nTop 10 employés par commandes livrées (sum,count):\n", by_emp)

    # Par mois/année (MM/YYYY)
    sample_date = dim_order.copy()
    sample_date['MonthYear'] = sample_date['OrderDate'].dt.strftime('%m/%Y')
    sample_date['Delivered'] = sample_date['ShippedDate'].notna().astype(int)
    by_month = sample_date.groupby('MonthYear')['Delivered'].agg(['sum','count']).sort_index()
    print("\nExtrait par mois (MM/YYYY) — quelques lignes :\n", by_month.head(6))

    print("\n--- TRANSFORMATION CHECKS COMPLETED ---")










# ------------------------------
# 1️⃣ Chargement complet dans le Data Warehouse
# ------------------------------

def load_dimension(cursor, table_name, df, natural_key, id_col=None):
    """
    Charge une dimension dans la base de données.
    Évite les doublons et les problèmes de types.
    """
    print(f"🔹 Chargement de {table_name}...")

    # 1️⃣ Colonnes existantes dans la table
    cursor.execute(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='{table_name}'")
    valid_columns = [row[0] for row in cursor.fetchall()]

    df_to_insert = df[[col for col in df.columns if col in valid_columns]].copy()

    # 2️⃣ Supprimer les lignes avec clé naturelle manquante
    df_to_insert = df_to_insert.dropna(subset=[natural_key])

    # 3️⃣ Conversion spéciale
    if table_name == 'DimOrder' and 'OrderID' in df_to_insert.columns:
        df_to_insert['OrderID'] = df_to_insert['OrderID'].astype('Int64')
        if 'StatusID' in df_to_insert.columns:
            df_to_insert['StatusID'] = df_to_insert['StatusID'].astype('Int64')
    elif id_col == 'DateKey' and 'DateKey' in df_to_insert.columns:
        df_to_insert['DateKey'] = df_to_insert['DateKey'].astype('Int64')

    # 4️⃣ Insertion avec gestion doublons et types
    inserted_count = 0
    for _, row in df_to_insert.iterrows():
        cursor.execute(f"SELECT {id_col} FROM {table_name} WHERE {natural_key} = ?", row[natural_key])
        if cursor.fetchone():
            continue

        values = [
            None if pd.isna(val) else
            int(val) if isinstance(val, (np.integer, pd.Int64Dtype().type)) else
            float(val) if isinstance(val, (np.floating, float)) else
            val.to_pydatetime() if isinstance(val, pd.Timestamp) else
            val
            for val in row
        ]

        columns = ", ".join(df_to_insert.columns)
        placeholders = ", ".join(["?"] * len(df_to_insert.columns))
        cursor.execute(f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})", values)
        inserted_count += 1

    print(f"✅ {inserted_count} lignes insérées dans {table_name}.")




def load_fact(cursor, df_fact):
    """
    Charge la table de faits Tabledefait de manière robuste.
    """
    print("🔹 Chargement de Tabledefait...")

    inserted_count = 0

    for _, row in df_fact.iterrows():
        def safe(val):
            return None if pd.isna(val) else int(val)

        order_id = safe(row['OrderID'])
        customer_id = safe(row['CustomerID'])
        employee_id = safe(row['EmployeeID'])
        orders_delivered = safe(row['OrdersDelivered'])
        orders_not_delivered = safe(row['OrdersNotDelivered'])
        region_id = safe(row.get('RegionID'))
        territory_id = safe(row.get('TerritoryID'))
        date_key = safe(row['DateKey'])

        # Doublon
        cursor.execute("""
            SELECT FactID FROM Tabledefait
            WHERE OrderID=? AND CustomerID=? AND EmployeeID=? AND DateKey=?
        """, order_id, customer_id, employee_id, date_key)
        if cursor.fetchone():
            continue

        cursor.execute("""
            INSERT INTO Tabledefait
            (OrderID, CustomerID, EmployeeID, OrdersDelivered, OrdersNotDelivered, RegionID, TerritoryID, DateKey)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, order_id, customer_id, employee_id, orders_delivered, orders_not_delivered, region_id, territory_id, date_key)

        inserted_count += 1

    print(f"✅ {inserted_count} lignes insérées dans Tabledefait.")










def load_all(dims, df_fact):
    """
    Chargement complet des dimensions + table de faits
    avec transaction et rollback.
    Corrige le mapping RegionID pour DimTerritory.
    """
    print("⏱ Vérification avant insertion :")
    for table_name, df in dims.items():
        print(f"{table_name} : {len(df)} lignes à charger")
    print(f"Table de faits : {len(df_fact)} lignes à charger")

    conn = get_bi_connection()
    if not conn:
        raise Exception("Connexion DW impossible")

    cursor = conn.cursor()

    try:
        conn.autocommit = False   # START TRANSACTION

        # ---------------------------------------------
        # 1️⃣ CHARGEMENT DES DIMENSIONS (ordre correct)
        # ---------------------------------------------
        load_dimension(cursor, "DimDate",       dims['dim_date'],       natural_key='DateKey',      id_col='DateKey')
        load_dimension(cursor, "DimRegion",     dims['dim_region'],     natural_key='RegionCode',   id_col='RegionID')

        # ----- CORRECTION REGIONID POUR DIMTERRITORY -----
        # Récupérer le mapping RegionCode -> RegionID réel
        cursor.execute("SELECT RegionCode, RegionID FROM DimRegion")
        region_map = {row[0]: row[1] for row in cursor.fetchall()}

        # Appliquer le mapping dans dim_territory
        def map_region_id(old_region_id):
            # old_region_id correspond à l'ancien RegionID (original de Northwind)
            code = f"REG_{int(old_region_id)}"
            return region_map.get(code, None)

        dims['dim_territory']['RegionID'] = dims['dim_territory']['RegionID'].apply(map_region_id)

        # Charger DimTerritory après correction
        load_dimension(cursor, "DimTerritory",  dims['dim_territory'],  natural_key='TerritoryCode', id_col='TerritoryID')

        # Charger les autres dimensions
        load_dimension(cursor, "DimCustomer",   dims['dim_customer'],   natural_key='CustomerCode', id_col='CustomerID')
        load_dimension(cursor, "DimEmployee",   dims['dim_employee'],   natural_key='EmployeeCode', id_col='EmployeeID')
        load_dimension(cursor, "DimOrder",      dims['dim_order'],      natural_key='OrderID',      id_col='OrderID')

        # ---------------------------------------------
        # 2️⃣ RÉCUPÉRATION MAPPING DES IDS SQL SERVER
        # ---------------------------------------------
        cursor.execute("SELECT CustomerCode, CustomerID FROM DimCustomer")
        customer_map = {row[0]: row[1] for row in cursor.fetchall()}

        cursor.execute("SELECT EmployeeCode, EmployeeID FROM DimEmployee")
        employee_map = {row[0]: row[1] for row in cursor.fetchall()}

        cursor.execute("SELECT OrderID, CustomerCode, EmployeeCode, OrderDate FROM DimOrder")
        order_map = {
            row[0]: {
                "CustomerCode": row[1],
                "EmployeeCode": row[2],
                "OrderDate":    row[3]  # string ou datetime
            }
            for row in cursor.fetchall()
        }

        # ---------------------------------------------
        # 3️⃣ REMPLISSAGE DES IDS DANS LA FACT TABLE
        # ---------------------------------------------
        df_fact_updated = df_fact.copy()

        df_fact_updated['CustomerID'] = df_fact_updated['OrderID'].apply(
            lambda oid: customer_map.get(order_map[oid]["CustomerCode"])
        )

        df_fact_updated['EmployeeID'] = df_fact_updated['OrderID'].apply(
            lambda oid: employee_map.get(order_map[oid]["EmployeeCode"])
        )

        # Correction pour DateKey (string ou datetime)
        def compute_datekey(order_date):
            if isinstance(order_date, str):
                return int(order_date.replace("-", ""))
            elif hasattr(order_date, "strftime"):
                return int(order_date.strftime("%Y%m%d"))
            else:
                return None

        df_fact_updated['DateKey'] = df_fact_updated['OrderID'].apply(
            lambda oid: compute_datekey(order_map[oid]["OrderDate"])
        )

        # ---------------------------------------------
        # 4️⃣ INSERTION DE LA TABLE DE FAITS
        # ---------------------------------------------
        load_fact(cursor, df_fact_updated)

        conn.commit()
        print("\n✅ Chargement terminé avec succès !")

    except Exception as e:
        conn.rollback()
        print("❌ Erreur chargement, rollback :", e)
        import traceback
        traceback.print_exc()

    finally:
        cursor.close()
        conn.close()


# ------------------------------
# EXECUTION
# ------------------------------

if __name__ == "__main__":
    RUN_TESTS = True      # active les tests après transformation

    print("\n===== ETL: START =====\n")

    try:
        # --------------------------------------------------------------------
        # 1) EXTRACTION
        # --------------------------------------------------------------------
        sql_data, excel_data = main_extraction()
        print("\n✅ Extraction terminée avec succès !")

        # --------------------------------------------------------------------
        # 2) TRANSFORMATION
        # --------------------------------------------------------------------
        print("\n🔁 Lancement de la transformation...")
        dims, df_fact = transform_pipeline(sql_data, excel_data)
        print("\n✅ Transformation terminée avec succès !")

        # --------------------------------------------------------------------
        # 3) TESTS (optionnels mais recommandés)
        # --------------------------------------------------------------------
        if RUN_TESTS:
            print("\n🔍 Exécution des tests de validation...")
            run_transformation_tests(dims, df_fact, sql_data, excel_data)
            print("\n✅ Tous les tests ont réussi !")

        print("\n===== ETL: END =====\n")

    except Exception as e:
        print("\n❌ ERREUR FATALE dans le processus ETL :", str(e))
        print("---------------------------------------------------------")
        import traceback
        traceback.print_exc()
        print("---------------------------------------------------------")

    

    print("\n🔍 Contrôle avant chargement :")

    print("dim_date:", dims['dim_date'].shape)
    print("dim_customer:", dims['dim_customer'].shape)
    print("dim_employee:", dims['dim_employee'].shape)
    print("dim_order:", dims['dim_order'].shape)
    print("Tabledefait:", df_fact.shape)



   



    # 🔁 Chargement dans le DW
    load_all(dims, df_fact)


















