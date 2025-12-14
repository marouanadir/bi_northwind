pandas
numpy
matplotlib
pyodbc
sqlalchemy
jupyter
openpyxl
plotly
streamlit






# 📊 Projet BI – Northwind (ETL & Dashboard Streamlit)

## 🎯 Objectif du projet
Ce projet consiste à concevoir une solution **Business Intelligence complète** basée sur la base **Northwind**, en utilisant :
- Deux sources de données hétérogènes
- Un processus **ETL en Python**
- Une base de données décisionnelle (schéma en étoile)
- Un **tableau de bord interactif avec Streamlit**

Le projet est réalisé **en monôme (individuel)**.

---

## 🗂️ Sources de données

### 🔹 Source 1 – SQL Server
- Base relationnelle créée à l’aide de **scripts SQL**
- Contient les tables opérationnelles Northwind
- Connexion gérée par le fichier :




👉 Ce fichier assure la connexion à la **base SQL Server** créée à partir des scripts fournis.

---

### 🔹 Source 2 – Fichiers Excel
- Tables exportées depuis **Microsoft Access (Northwind)**
- 3 fichiers Excel :
  - `orders.xlsx`
  - `customers.xlsx`
  - `employees.xlsx`

- Connexion et lecture assurées par :


👉 Cette source est utilisée pour contourner les limitations et problèmes d’Access.

---

## 🏗️ Architecture BI

- Modèle **multidimensionnel en étoile**
- Une **table de faits** :
  - Nombre de commandes livrées
  - Nombre de commandes non livrées
- Dimensions :
  - Client
  - Employé
  - Date
  - Région
  - Territoire

Toutes les relations passent **uniquement par la table de faits**.

---

## 🔄 ETL (Extract – Transform – Load)

Le processus ETL est implémenté dans le fichier :





### Étapes de l’ETL :
1. **Extraction**
   - Données depuis SQL Server (source 1)
   - Données depuis les fichiers Excel (source 2)

2. **Transformation**
   - Nettoyage des données
   - Harmonisation des clés (CustomerID, EmployeeID, OrderID)
   - Gestion des valeurs manquantes
   - Calcul des indicateurs (livré / non livré)

3. **Chargement**
   - Insertion des données dans la base décisionnelle SQL Server
   - Remplissage des dimensions puis de la table de faits

---

## 📊 Tableau de bord (Streamlit)

Le tableau de bord est développé avec **Streamlit** dans le fichier :




### Fonctionnalités :
- KPIs globaux :
  - Total commandes
  - Commandes livrées / non livrées
  - Taux de livraison
- Filtres dynamiques :
  - Année
  - Employé
  - Région
  - Territoire
- Visualisations :
  - Statistiques par année
  - Graphiques mensuels
  - Heatmap (Jour × Mois)
  - Statistiques par région, territoire et pays
  - Top clients et employés
- Tableau détaillé des commandes

---

## ▶️ Exécution du projet

### 1️⃣ Installer les dépendances
```bash
pip install pandas pyodbc streamlit plotly numpy




Lancer le Dashboard Streamlit
streamlit run dashboard.py


Le tableau de bord s’ouvre automatiquement à l’adresse :

http://localhost:8501