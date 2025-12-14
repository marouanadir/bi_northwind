import pyodbc

def get_source1_connection():
    """
    Connexion au Data Warehouse (DW) Northwind_BI1
    Retourne un objet connexion si succès, sinon None.
    """

    server = "localhost"
    database = "Northwind"   
    username = "sa"        
    password = "maroua"    

    conn_str = (
        f"DRIVER={{SQL Server}};"
        f"SERVER=.;"
        f"DATABASE={database};"
        f"UID=sa;"
        f"PWD=maroua;"
        f"Trusted_Connection=no;"
    )
    
    try:
        conn = pyodbc.connect(conn_str)
        return conn
    except Exception as e:
        print(f"❌ Erreur de connexion au Data Warehouse: {e}")
        return None


def test_bi_connection():
    """
    Teste la connexion au Data Warehouse.
    Affiche l'état et essaye de lire une table si possible.
    """

    print("🚀 TEST CONNEXION DATA WAREHOUSE")
    print("=" * 50)

    conn = get_source1_connection()

    if conn:
        print("✅ Connexion réussie à Northwind_BI1")

        try:
            cursor = conn.cursor()

            # Vérifier que les tables du DW existent
            cursor.execute("""
                SELECT TABLE_NAME 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_SCHEMA = 'dbo'
            """)

            tables = cursor.fetchall()

            if not tables:
                print("⚠️  Aucun tableau trouvé dans le DW !")
            else:
                print("📌 Tables trouvées dans le Data Warehouse :")
                for t in tables:
                    print(f"   - {t.TABLE_NAME}")

            conn.close()
            return True

        except Exception as e:
            print(f"❌ Erreur lors de la lecture des tables: {e}")
            return False

    else:
        print("❌ Impossible de se connecter au Data Warehouse")
        return False


if __name__ == "__main__":
    test_bi_connection()
