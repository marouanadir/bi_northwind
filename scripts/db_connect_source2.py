# db_connect_source2.py
import os
import pandas as pd
import sys
sys.path.append(os.path.dirname(__file__))


DATA_PATH = "../data/raw"

def get_source2_files():
    """Retourne les chemins complets des fichiers Excel Source 2."""
    
    files = {
        "orders": os.path.join(DATA_PATH, "orders.xlsx"),
        "customers": os.path.join(DATA_PATH, "customers.xlsx"),
        "employees": os.path.join(DATA_PATH, "employees.xlsx")
    }
    
    return files







def test_source2():
    
    
    # SOURCE 2: Fichiers Excel
    print("\n🔍 SOURCE 2 - FICHIERS EXCEL")
    print("-" * 40)
    
    source2_ok = False
    excel_files = ['orders.xlsx', 'customers.xlsx', 'employees.xlsx']
    all_excel_ok = True
    
    for file in excel_files:
        file_path = f"../data/raw/{file}"
        if os.path.exists(file_path):
            try:
                df = pd.read_excel(file_path)
                print(f"✅ {file}: {df.shape[0]} lignes")
            except Exception as e:
                print(f"❌ {file}: Erreur - {e}")
                all_excel_ok = False
        else:
            print(f"❌ {file}: Fichier non trouvé")
            all_excel_ok = False
    
    source2_ok = all_excel_ok
    
    # RÉSULTAT FINAL
    print("\n" + "=" * 60)
    print("📊 RAPPORT FINAL:")
    print(f"   Source 2 (Fichiers Excel): {'✅ PRÊTE' if source2_ok else '❌ PROBLEME'}")
    
    if  source2_ok:
        print("\n🎯 Connexion reusie à la Source 2! ")
        return True
    else:
        print("\n⚠️  Problèmes détectés - corrigez avant de continuer")
        return False

if __name__ == "__main__":
    test_source2()