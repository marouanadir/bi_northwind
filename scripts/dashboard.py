# dashboard.py
# =====================================================================
# 📊 NORTHWIND BI DASHBOARD — VERSION COMPLÈTE & PROFESSIONNELLE
# =====================================================================

import streamlit as st
import pandas as pd
import pyodbc
import plotly.express as px
import numpy as np
import io
import calendar
from typing import Dict

# =====================================================================
# CONFIGURATION GLOBALE
# =====================================================================

st.set_page_config(
    page_title="Northwind BI - Dashboard Complet",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Couleurs
COLOR_MAIN = "#2B8CBE"
COLOR_ACCENT = "#F39C12"
COLOR_GREEN = "#27AE60"
COLOR_RED = "#C0392B"
COLOR_CARD = "#2A2A2A"

# CSS
st.markdown(f"""
    <style>
        .kpi-card {{
            background-color: {COLOR_CARD};
            padding: 16px;
            border-radius: 10px;
            text-align: center;
            color: #fff;
        }}
        .kpi-title {{ font-size: 13px; color: #D0D0D0; }}
        .kpi-number {{ font-size: 26px; font-weight: 700; color: white; }}
        .small-muted {{ color: #B0B0B0; font-size: 12px; }}
    </style>
""", unsafe_allow_html=True)

# =====================================================================
# CONNEXION
# =====================================================================

def get_connection(server: str = ".", database: str = "Northwind_BI3",
                   uid: str = "sa", pwd: str = "maroua") -> pyodbc.Connection:
    conn_str = (
        f"DRIVER={{SQL Server}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={uid};"
        f"PWD={pwd};"
        f"Trusted_Connection=no;"
    )
    return pyodbc.connect(conn_str, timeout=5)

# =====================================================================
# CHARGEMENT (CACHÉ)
# =====================================================================

@st.cache_data(ttl=600)
def load_dw_data(params: Dict) -> pd.DataFrame:
    """
    Charge et normalise les données du Data Warehouse.
    params: dict(server, database, uid, pwd)
    """
    conn = None
    try:
        conn = get_connection(server=params.get("server", "."),
                              database=params.get("database", "Northwind_BI3"),
                              uid=params.get("uid", "sa"),
                              pwd=params.get("pwd", "maroua"))
        query = """
        SELECT f.*,
               d.DateValue, d.[Year], d.[Month], d.MonthName, d.DayOfWeek, d.IsWeekend,
               c.Company, c.City AS CustomerCity, c.CountryRegion,
               e.FirstName AS EmpFirst, e.LastName AS EmpLast,
               ter.TerritoryName, reg.RegionName
        FROM Tabledefait f
        LEFT JOIN DimDate d ON f.DateKey = d.DateKey
        LEFT JOIN DimCustomer c ON f.CustomerID = c.CustomerID
        LEFT JOIN DimEmployee e ON f.EmployeeID = e.EmployeeID
        LEFT JOIN DimTerritory ter ON f.TerritoryID = ter.TerritoryID
        LEFT JOIN DimRegion reg ON ter.RegionID = reg.RegionID
        """
        df = pd.read_sql(query, conn)
    finally:
        if conn:
            conn.close()

    # Normalisation
    df["DateValue"] = pd.to_datetime(df.get("DateValue"), errors="coerce")
    df["EmpFirst"] = df.get("EmpFirst", "").fillna("").astype(str)
    df["EmpLast"] = df.get("EmpLast", "").fillna("").astype(str)
    df["Employee"] = (df["EmpFirst"] + " " + df["EmpLast"]).str.strip().replace("", np.nan)

    # KPI flags robustes
    df["OrdersDelivered"] = df.get("OrdersDelivered", 0).fillna(0).astype(int)
    df["OrdersNotDelivered"] = df.get("OrdersNotDelivered", 0).fillna(0).astype(int)
    df["DeliveredFlag"] = df["OrdersDelivered"].clip(0,1).astype(int)
    df["NotDeliveredFlag"] = df["OrdersNotDelivered"].clip(0,1).astype(int)

    # Country normalization
    if "CountryRegion" in df.columns:
        df["CountryRegion"] = df["CountryRegion"].astype(str).replace("nan", np.nan)

    # Adds: Year, MonthName if not present
    if "MonthName" not in df.columns and "DateValue" in df.columns:
        df["MonthName"] = df["DateValue"].dt.strftime("%b").fillna("Unknown")
    if "Year" not in df.columns and "DateValue" in df.columns:
        df["Year"] = df["DateValue"].dt.year

    return df

# =====================================================================
# UTILITAIRES
# =====================================================================

def summarize_data_quality(df: pd.DataFrame) -> pd.DataFrame:
    total = len(df)
    rows = []
    for col in df.columns:
        nulls = int(df[col].isna().sum())
        pct = round(100 * nulls / max(1, total), 2)
        rows.append({"column": col, "missing": nulls, "missing_pct": pct})
    return pd.DataFrame(rows).sort_values("missing", ascending=False)

def df_to_csv_bytes(df: pd.DataFrame) -> bytes:
    towrite = io.BytesIO()
    df.to_csv(towrite, index=False, encoding="utf-8-sig")
    return towrite.getvalue()

# =====================================================================
# SIDEBAR
# =====================================================================

st.sidebar.title("🔧 Connexion & Filtres")

server = st.sidebar.text_input("SQL Server (server)", value=".")
database = st.sidebar.text_input("Database", value="Northwind_BI3")
uid = st.sidebar.text_input("SQL UID", value="sa")
pwd = st.sidebar.text_input("SQL PWD", value="maroua", type="password")

connection_params = {"server": server, "database": database, "uid": uid, "pwd": pwd}

if st.sidebar.button("🔄 Recharger / Tester connexion"):
    load_dw_data.clear()
    st.experimental_rerun()

st.sidebar.markdown("---")
st.sidebar.markdown("Filtres globaux (utilisés dans Overview)") 
# Filters initial placeholders (will be populated after load)
st.sidebar.markdown("NB: Ajuste les filtres dans l'onglet Overview.")

# =====================================================================
# CHARGEMENT AVEC SPINNER
# =====================================================================

with st.spinner("🔄 Chargement des données depuis le DW..."):
    try:
        df = load_dw_data(connection_params)
        st.success("✅ Données chargées depuis le DW.")
    except Exception as e:
        st.error(f"❌ Erreur lors du chargement: {e}")
        st.stop()

# =====================================================================
# GLOBAL FILTERS (OVERVIEW)
# =====================================================================

# Prepare filters' options
years = sorted(df["Year"].dropna().unique().tolist()) if "Year" in df.columns else []
years_str = ["Toutes les années"] + [str(int(y)) for y in years]

employees = sorted(df["Employee"].dropna().unique().tolist()) if "Employee" in df.columns else []
regions = sorted(df["RegionName"].dropna().unique().tolist()) if "RegionName" in df.columns else []
territories = sorted(df["TerritoryName"].dropna().unique().tolist()) if "TerritoryName" in df.columns else []
countries = sorted(df["CountryRegion"].dropna().unique().tolist()) if "CountryRegion" in df.columns else []

# Use placeholders in overview via expander (we'll create local controls per tab for clarity)

# =====================================================================
# NAVIGATION PAR TABS
# =====================================================================

tabs = st.tabs(["Overview", "Dates & Trends", "Heatmap", "Regions & Map", "Top / Details", "Data Quality"])
tab_overview, tab_dates, tab_heatmap, tab_regions, tab_top, tab_quality = tabs

# ------------------------------
# TAB: OVERVIEW
# ------------------------------
with tab_overview:
    st.title("📊 Northwind BI — Overview")
    st.markdown("Utilisez les filtres rapides ci-dessous pour explorer les données.")

    # Quick filters inline
    with st.expander("Filtres rapides"):
        colf1, colf2, colf3, colf4 = st.columns(4)
        selected_year_str = colf1.selectbox("Année", years_str, index=0)
        selected_year = None if selected_year_str == "Toutes les années" else int(selected_year_str)

        selected_employee = colf2.multiselect("Employés", employees, default=[])
        selected_region = colf3.multiselect("Régions", regions, default=[])
        selected_territory = colf4.multiselect("Territoires", territories, default=[])

    # Apply filters
    df_filtered = df.copy()
    if selected_year:
        df_filtered = df_filtered[df_filtered["Year"] == selected_year]
    if selected_employee:
        df_filtered = df_filtered[df_filtered["Employee"].isin(selected_employee)]
    if selected_region:
        df_filtered = df_filtered[df_filtered["RegionName"].isin(selected_region)]
    if selected_territory:
        df_filtered = df_filtered[df_filtered["TerritoryName"].isin(selected_territory)]

    # KPIs
    total_orders = len(df_filtered)
    delivered = int(df_filtered["DeliveredFlag"].sum())
    not_delivered = int(df_filtered["NotDeliveredFlag"].sum())
    pct_delivered = round((delivered / max(1, delivered + not_delivered)) * 100, 1)
    pct_not_delivered = round(100 - pct_delivered, 1)

    k1, k2, k3, k4 = st.columns(4)
    with k1:
        st.markdown(f"<div class='kpi-card'><div class='kpi-title'>Total Commandes</div><div class='kpi-number'>{total_orders}</div></div>", unsafe_allow_html=True)
    with k2:
        st.markdown(f"<div class='kpi-card'><div class='kpi-title'>Livrées</div><div class='kpi-number' style='color:{COLOR_GREEN}'>{delivered}</div></div>", unsafe_allow_html=True)
    with k3:
        st.markdown(f"<div class='kpi-card'><div class='kpi-title'>Non livrées</div><div class='kpi-number' style='color:{COLOR_RED}'>{not_delivered}</div></div>", unsafe_allow_html=True)
    with k4:
        st.markdown(f"<div class='kpi-card'><div class='kpi-title'>Taux de Livraison</div><div class='kpi-number'>{pct_delivered}%</div></div>", unsafe_allow_html=True)

    st.markdown("---")

    # Trends small
    colA, colB = st.columns([2,1.1])
    with colA:
        st.subheader("Commandes livrées - Vue mensuelle")
        if not df_filtered.empty and df_filtered["DateValue"].notna().any():
            df_month = df_filtered.dropna(subset=["DateValue"])
            df_month["Month"] = df_month["DateValue"].dt.to_period("M")
            monthly = df_month.groupby("Month")[["DeliveredFlag"]].sum().reset_index()
            monthly["Date"] = monthly["Month"].dt.to_timestamp()
            fig_trend = px.line(monthly, x="Date", y="DeliveredFlag", markers=True, title="Livraisons - évolution mensuelle")
            st.plotly_chart(fig_trend, use_container_width=True)
        else:
            st.info("Aucune donnée de date pour afficher la tendance.")

    with colB:
        st.subheader("Livrées vs Non livrées")
        fig = px.pie(names=["Livrées", "Non livrées"], values=[delivered, not_delivered], color_discrete_map={"Livrées": COLOR_GREEN, "Non livrées": COLOR_RED})
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")
    st.subheader("Dernières commandes")
    st.dataframe(
        df_filtered.sort_values("DateValue", ascending=False).head(200)[[
            "OrderID", "DateValue", "Company", "Employee", "DeliveredFlag", "NotDeliveredFlag",
            "RegionName", "TerritoryName", "CountryRegion"
        ]], use_container_width=True
    )

# ------------------------------
# TAB: DATES & TRENDS (détails)
# ------------------------------
with tab_dates:
    st.header("📅 Dates & Tendances détaillées")

    if df_filtered.empty:
        st.info("Aucun enregistrement après filtrage.")
    else:
        col1, col2 = st.columns([2, 1.2])
        with col1:
            st.subheader("Histogramme : Livrées par mois")
            df_month = df_filtered.dropna(subset=["DateValue"])
            if not df_month.empty:
                # Keep month order chronological
                df_month["MonthOrder"] = df_month["DateValue"].dt.month
                df_month["MonthNameFull"] = df_month["DateValue"].dt.strftime("%b %Y")
                monthly = df_month.groupby(["MonthOrder","MonthNameFull"])[["DeliveredFlag"]].sum().reset_index().sort_values("MonthOrder")
                fig = px.bar(monthly, x="MonthNameFull", y="DeliveredFlag", title="Commandes livrées par mois")
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Pas de dates valides pour histogramme.")

        with col2:
            st.subheader("Weekend vs Weekdays")
            weekend_count = df_filtered[df_filtered["IsWeekend"] == 1]["DeliveredFlag"].sum()
            weekday_count = df_filtered[df_filtered["IsWeekend"] == 0]["DeliveredFlag"].sum()
            fig_pie = px.pie(names=["Weekends", "Semaine"], values=[weekend_count, weekday_count], title="Livraisons weekend / semaine", color_discrete_sequence=[COLOR_ACCENT, COLOR_MAIN])
            st.plotly_chart(fig_pie, use_container_width=True)

    st.markdown("---")
    st.subheader("Distribution Livrées / Non livrées par mois")
    if not df_filtered.empty:
        df_m = df_filtered.dropna(subset=["DateValue"])
        df_m["Month"] = df_m["DateValue"].dt.to_period("M").dt.to_timestamp()
        monthly_both = df_m.groupby("Month")[["DeliveredFlag","NotDeliveredFlag"]].sum().reset_index()
        fig_both = px.bar(monthly_both, x="Month", y=["DeliveredFlag","NotDeliveredFlag"], title="Livrées / Non livrées par mois")
        st.plotly_chart(fig_both, use_container_width=True)

# ------------------------------
# TAB: HEATMAP Jour × Mois
# ------------------------------
with tab_heatmap:
    st.header("🔥 Heatmap : Activité par Jour × Mois")

    if df_filtered.empty or df_filtered["DateValue"].notna().sum() == 0:
        st.info("Données de date insuffisantes pour la heatmap.")
    else:
        df_heat = df_filtered.dropna(subset=["DateValue"]).copy()
        # Weekday ordering Mon..Sun
        df_heat["WeekdayNum"] = df_heat["DateValue"].dt.weekday  # Mon=0
        df_heat["WeekdayName"] = df_heat["DateValue"].dt.day_name()
        df_heat["MonthName"] = df_heat["DateValue"].dt.strftime("%b")
        # Pivot table: index weekday, columns month, values sum of DeliveredFlag
        pivot = df_heat.pivot_table(index="WeekdayNum", columns="MonthName", values="DeliveredFlag", aggfunc="sum", fill_value=0)
        # Reindex rows to Monday-Sunday
        weekdays_order = [calendar.day_name[i] for i in range(7)]
        pivot.index = [calendar.day_name[i] for i in pivot.index]
        pivot = pivot.reindex(weekdays_order).fillna(0)

        # Ensure months order by calendar short names present in data
        month_order = list(dict.fromkeys(df_heat["DateValue"].dt.strftime("%b").tolist()))
        # plot heatmap
        try:
            fig_heat = px.imshow(pivot.values, x=pivot.columns, y=pivot.index, labels=dict(x="Mois", y="Jour", color="Livrées"), aspect="auto", title="Heatmap : Livrées par Jour × Mois")
            st.plotly_chart(fig_heat, use_container_width=True)
        except Exception:
            st.warning("Impossible d'afficher la heatmap graphiquement.")

# ------------------------------
# TAB: REGIONS & MAP
# ------------------------------
with tab_regions:
    st.header("🌍 Régions & Territoires")
    colA, colB = st.columns(2)

    with colA:
        st.subheader("Livraisons par Région")
        df_reg = df_filtered.dropna(subset=["RegionName"])
        if not df_reg.empty:
            reg = df_reg.groupby("RegionName")[["DeliveredFlag"]].sum().reset_index().sort_values("DeliveredFlag", ascending=False)
            fig_reg = px.bar(reg, x="RegionName", y="DeliveredFlag", title="Livraisons par Région", color_discrete_sequence=[COLOR_MAIN])
            st.plotly_chart(fig_reg, use_container_width=True)
        else:
            st.info("Pas de données Région disponibles.")

    with colB:
        st.subheader("Livraisons par Territoire")
        df_ter = df_filtered.dropna(subset=["TerritoryName"])
        if not df_ter.empty:
            ter = df_ter.groupby("TerritoryName")[["DeliveredFlag"]].sum().reset_index().sort_values("DeliveredFlag", ascending=False)
            fig_ter = px.bar(ter, x="TerritoryName", y="DeliveredFlag", title="Livraisons par Territoire", color_discrete_sequence=[COLOR_ACCENT])
            st.plotly_chart(fig_ter, use_container_width=True)
        else:
            st.info("Pas de données Territoire disponibles.")

    st.markdown("---")
    st.subheader("Carte — Livraisons par Pays")
    df_country = df_filtered.dropna(subset=["CountryRegion"])
    if df_country.empty:
        st.info("Aucune donnée Pays disponible pour la carte.")
    else:
        country_bar = df_country.groupby("CountryRegion")[["DeliveredFlag"]].sum().reset_index().sort_values("DeliveredFlag", ascending=False)
        try:
            fig_map = px.choropleth(country_bar, locations="CountryRegion", locationmode="country names",
                                    color="DeliveredFlag", title="Livraisons par Pays")
            st.plotly_chart(fig_map, use_container_width=True)
        except Exception:
            st.warning("Impossible de tracer la carte (noms pays non standard). Affichage en bar chart.")
            fig_bar = px.bar(country_bar, x="CountryRegion", y="DeliveredFlag", title="Livraisons par Pays")
            st.plotly_chart(fig_bar, use_container_width=True)

    # Country table with stats
    st.markdown("---")
    st.subheader("📋 Statistiques par Pays")
    if not df_country.empty:
        df_country_table = df_country.groupby("CountryRegion").agg(
            TotalOrders=("OrderID", "count"),
            Delivered=("DeliveredFlag", "sum"),
            NotDelivered=("NotDeliveredFlag", "sum"),
            DeliveryRate=("DeliveredFlag", lambda x: round((x.sum() / max(1, len(x))) * 100, 2))
        ).reset_index().sort_values("Delivered", ascending=False)
        st.dataframe(df_country_table, use_container_width=True)
    else:
        st.info("Aucune donnée Pays pour le tableau.")

# ------------------------------
# TAB: TOP / DETAILS
# ------------------------------
with tab_top:
    st.header("🏆 Top & Détails")

    # Top Clients
    st.subheader("Top 10 Clients (par livraisons)")
    df_clients = df_filtered.dropna(subset=["Company"])
    if not df_clients.empty:
        top_clients = df_clients.groupby("Company")[["DeliveredFlag"]].sum().reset_index().sort_values("DeliveredFlag", ascending=False).head(10)
        fig_clients = px.bar(top_clients, x="Company", y="DeliveredFlag", title="Top 10 Clients", color_discrete_sequence=[COLOR_MAIN])
        st.plotly_chart(fig_clients, use_container_width=True)
    else:
        st.info("Aucune donnée client.")

    # Top Employees
    st.subheader("Top Employés (par livraisons)")
    df_emp = df_filtered.dropna(subset=["Employee"])
    if not df_emp.empty:
        top_emp = df_emp.groupby("Employee")[["DeliveredFlag"]].sum().reset_index().sort_values("DeliveredFlag", ascending=False).head(10)
        fig_emp = px.bar(top_emp, x="Employee", y="DeliveredFlag", title="Top Employés", color_discrete_sequence=[COLOR_ACCENT])
        st.plotly_chart(fig_emp, use_container_width=True)
    else:
        st.info("Aucune donnée employé.")

    st.markdown("---")
    st.subheader("Livrées vs Non livrées — Comparaisons")
    col1, col2 = st.columns(2)
    with col1:
        fig_bar = px.bar(x=["Livrées", "Non Livrées"], y=[delivered, not_delivered], color=["Livrées", "Non Livrées"],
                         color_discrete_map={"Livrées": COLOR_GREEN, "Non Livrées": COLOR_RED}, title="Nombre de commandes")
        st.plotly_chart(fig_bar, use_container_width=True)
    with col2:
        fig_pie2 = px.pie(names=["Livrées", "Non Livrées"], values=[delivered, not_delivered],
                          color_discrete_map={"Livrées": COLOR_GREEN, "Non Livrées": COLOR_RED}, title="Proportion Livrées / Non Livrées")
        st.plotly_chart(fig_pie2, use_container_width=True)

    st.markdown("---")
    st.subheader("Table détaillée des commandes (filtré)")
    cols_to_show = ["OrderID", "DateValue", "Company", "Employee", "DeliveredFlag", "NotDeliveredFlag", "RegionName", "TerritoryName", "CountryRegion"]
    available_cols = [c for c in cols_to_show if c in df_filtered.columns]
    st.dataframe(df_filtered[available_cols].sort_values("DateValue", ascending=False).reset_index(drop=True), use_container_width=True)

    st.markdown("---")
    st.subheader("Télécharger le dataset filtré")
    csv_bytes = df_to_csv_bytes(df_filtered)
    st.download_button("⬇️ Télécharger CSV (filtré)", data=csv_bytes, file_name="northwind_filtered.csv", mime="text/csv")

# ------------------------------
# TAB: DATA QUALITY
# ------------------------------
with tab_quality:
    st.header("🧾 Data Quality & Diagnostics")
    st.write("Résumé rapide des valeurs manquantes et exemples d'incohérences.")

    dq_summary = summarize_data_quality(df)
    st.dataframe(dq_summary, use_container_width=True)

    st.markdown("---")
    st.subheader("Exemples : lignes avec client manquant")
    # Choose columns that likely exist
    check_cols = [c for c in ["OrderID", "DateValue", "CustomerID", "Company", "CustomerCity"] if c in df.columns]
    missing_customers = df[df["Company"].isna() | df.get("CustomerCity", pd.Series([np.nan]*len(df))).isna()][check_cols].head(200)
    if not missing_customers.empty:
        st.warning(f"Extrait de lignes avec client manquant ({len(missing_customers)} exemples affichés).")
        st.dataframe(missing_customers, use_container_width=True)
    else:
        st.success("Aucun client manquant détecté dans l'extrait.")

    st.markdown("---")
    st.write("✅ Recommandations :")
    st.write("- Charger les dimensions avant la table de faits dans l'ETL.")
    st.write("- Normaliser les natural keys (strip, upper, remove non-printable chars).")
    st.write("- Conserver une staging table pour les enregistrements non appariés et logger les erreurs.")

# =====================================================================
# FOOTER
# =====================================================================

# =====================================================================
# FIN
# =====================================================================
