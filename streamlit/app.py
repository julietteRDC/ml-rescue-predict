import streamlit as st
import pandas as pd
import geopandas as gpd
import folium
from streamlit_folium import st_folium
import snowflake.connector
from datetime import date
import plotly.graph_objects as go
import base64


st.set_page_config(layout="wide", page_title="Analyse Risque Incendie 77")


def get_image_as_base64(path):
    """Fonction pour encoder une image locale en base64"""
    with open(path, "rb") as image_file:
        return base64.b64encode(image_file.read()).decode()


# Note : Pour que cela fonctionne, vous devez avoir une image nommée 'logo.png'
# dans le même dossier que votre script app.py.
# Vous pouvez télécharger une icône de casque ici : https://www.flaticon.com/free-icon/firefighter_3721724
#
# Si vous ne pouvez/voulez pas télécharger de fichier, utilisez un emoji.
# Remplacez la ligne st.image par : st.markdown("<h1>👨‍🚒 Analyse de Risque Incendie</h1>", unsafe_allow_html=True)

try:
    # Utilise une mise en page en colonnes pour le header
    col1, col2 = st.columns([0.1, 0.9])
    with col1:
        st.image("logo.png", width=100)  # Assurez-vous d'avoir logo.png
    with col2:
        st.markdown(
            "<h1 style='color: #C21807; font-size: 48px;'><strong>Analyse des risques d'accident 💥 - Seine-et-Marne (77)</strong></h1>",
            unsafe_allow_html=True,
        )
except FileNotFoundError:
    st.error(
        "Le fichier 'logo.png' est introuvable. Veuillez le placer dans le même dossier que le script."
    )
    st.title("Analyse de Risque Incendie - Seine-et-Marne (77)")


st.markdown("""
*Cette application se connecte à **Snowflake** pour afficher une carte interactive du département de la Seine-et-Marne (77).
La couleur de chaque commune sur la carte correspond à sa classe de risque dans le tableau.*
""")
st.markdown("---")  # Ajoute une ligne de séparation
st.sidebar.header("Filtres")
selected_date = st.sidebar.date_input("📅 Sélectionnez une journée", date.today())


@st.cache_data
def load_data_from_snowflake(query_date):
    try:
        conn = snowflake.connector.connect(**st.secrets["snowflake"])
        cs = conn.cursor()
        date_str = query_date.strftime("%Y-%m-%d")
        query = """
        SELECT "com", "population", "jour", "mois", "an", "prediction_score" AS "score"
        FROM rescue_predict_db.public."accidents_predict" 
        WHERE "date" = %s AND "dep" = 77;
        """
        st.info(f"Exécution de la requête pour la date : {date_str}")
        cs.execute(query, (date_str,))
        df = cs.fetch_pandas_all()

        if not df.empty:
            df.columns = df.columns.str.lower()
            if "com" in df.columns:
                df.rename(columns={"com": "code_insee"}, inplace=True)
                df["code_insee"] = df["code_insee"].astype(str)
                st.success("Données chargées avec succès depuis Snowflake !")
                return df
            else:
                return pd.DataFrame()
        else:
            return pd.DataFrame()
    except Exception:
        return pd.DataFrame()
    finally:
        if "cs" in locals() and cs:
            cs.close()
        if "conn" in locals() and conn:
            conn.close()


@st.cache_data
def load_geodata(department_code="77"):
    url = "https://raw.githubusercontent.com/gregoiredavid/france-geojson/master/communes.geojson"
    try:
        gdf = gpd.read_file(url)
        gdf["code"] = gdf["code"].astype(str)
        return gdf[gdf["code"].str.startswith(department_code)]
    except Exception as e:
        st.error(f"Erreur lors du chargement des données géographiques : {e}")
        return None


df_scores = load_data_from_snowflake(selected_date)
gdf_communes = load_geodata("77")

# ==============================================================================
# 4. Fusion, Création de la Carte et du Tableau
# ==============================================================================
if not df_scores.empty and gdf_communes is not None:
    merged_gdf = gdf_communes.merge(
        df_scores, left_on="code", right_on="code_insee", how="left"
    )
    merged_gdf["score"] = merged_gdf["score"].fillna(0)

    # Définir les seuils et labels pour la classe de risque
    bins = [0.001, 0.01, 0.1, 0.2, 0.3, float("inf")]
    labels = ["A", "B", "C", "D", "E"]
    merged_gdf["Classe de risque"] = pd.cut(
        merged_gdf["score"], bins=bins, labels=labels, right=True
    )

    map_center = [48.65, 2.9]
    m = folium.Map(location=map_center, zoom_start=9, tiles="cartodbpositron")

    # Ce dictionnaire sera utilisé pour la carte ET pour le tableau
    color_map_hex = {
        "A": "#006400",  # Vert foncé
        "B": "#228B22",  # Vert
        "C": "#FFD700",  # Jaune
        "D": "#FFA500",  # Orange
        "E": "#DC143C",  # Rouge
    }

    col1, col2 = st.columns(
        [3, 1]
    )  # 3/4 de la largeur pour la carte, 1/4 pour la jauge

    with col1:
        st.subheader(f"Carte des Risques pour le {selected_date.strftime('%d/%m/%Y')}")
        map_center = [48.65, 2.9]
        m = folium.Map(location=map_center, zoom_start=9, tiles="cartodbpositron")

        def style_function(feature):
            classe = feature["properties"]["Classe de risque"]
            return {
                "fillColor": color_map_hex.get(classe, "#808080"),
                "color": "black",
                "weight": 0.5,
                "fillOpacity": 0.7,
            }

        folium.GeoJson(
            merged_gdf,
            style_function=style_function,
            tooltip=folium.features.GeoJsonTooltip(
                fields=["nom", "score", "Classe de risque"],
                aliases=["Commune:", "Score:", "Classe:"],
                style="background-color: white; color: #333333; font-family: arial; font-size: 12px; padding: 10px;",
            ),
        ).add_to(m)

        legend_html = """
         <div style="position: fixed; bottom: 50px; left: 50px; width: 180px; height: 130px; border:2px solid grey; z-index:9999; font-size:14px; background-color:white;">
         &nbsp; <b>Classe de Risque</b> <br>
         &nbsp; A &nbsp; <i class="fa fa-square" style="color:#006400"></i><br>
         &nbsp; B &nbsp; <i class="fa fa-square" style="color:#228B22"></i><br>
         &nbsp; C &nbsp; <i class="fa fa-square" style="color:#FFD700"></i><br>
         &nbsp; D &nbsp; <i class="fa fa-square" style="color:#FFA500"></i><br>
         &nbsp; E &nbsp; <i class="fa fa-square" style="color:#DC143C"></i></div>
         """
        m.get_root().html.add_child(folium.Element(legend_html))
        st_folium(m, use_container_width=True)

    with col2:
        # Création de la jauge ---
        st.subheader("Score Moyen du Département")

        # Calcul du score moyen
        average_score = merged_gdf["score"].mean()

        fig = go.Figure(
            go.Indicator(
                mode="gauge+number",
                value=average_score,
                title={"text": "Risque Global", "font": {"size": 20}},
                gauge={
                    "axis": {
                        "range": [0, 0.5],
                        "tickwidth": 1,
                        "tickcolor": "darkblue",
                    },
                    "bar": {"color": "#555"},
                    "bgcolor": "white",
                    "borderwidth": 2,
                    "bordercolor": "gray",
                    "steps": [
                        {"range": [0, 0.1], "color": color_map_hex["B"]},
                        {"range": [0.1, 0.2], "color": color_map_hex["C"]},
                        {"range": [0.2, 0.3], "color": color_map_hex["D"]},
                        {"range": [0.3, 0.5], "color": color_map_hex["E"]},
                    ],
                },
            )
        )
        fig.update_layout(height=350, margin={"t": 10, "b": 10, "l": 10, "r": 10})
        st.plotly_chart(fig, use_container_width=True)

    def style_function(feature):
        classe = feature["properties"]["Classe de risque"]
        return {
            "fillColor": color_map_hex.get(classe, "#808080"),  # Gris si pas de classe
            "color": "black",
            "weight": 0.5,
            "fillOpacity": 0.7,
        }

    folium.GeoJson(
        merged_gdf,
        style_function=style_function,
        tooltip=folium.features.GeoJsonTooltip(
            fields=["nom", "score", "Classe de risque"],
            aliases=["Commune:", "Score:", "Classe:"],
            style=(
                "background-color: white; color: #333333; font-family: arial; font-size: 12px; padding: 10px;"
            ),
        ),
    ).add_to(m)

    # Création d'une légende HTML personnalisée pour la carte ---
    legend_html = """
     <div style="position: fixed; 
     bottom: 50px; left: 50px; width: 180px; height: 130px; 
     border:2px solid grey; z-index:9999; font-size:14px;
     background-color:white;
     ">&nbsp; <b>Classe de Risque</b> <br>
     &nbsp; A &nbsp; <i class="fa fa-square" style="color:#006400"></i><br>
     &nbsp; B &nbsp; <i class="fa fa-square" style="color:#228B22"></i><br>
     &nbsp; C &nbsp; <i class="fa fa-square" style="color:#FFD700"></i><br>
     &nbsp; D &nbsp; <i class="fa fa-square" style="color:#FFA500"></i><br>
     &nbsp; E &nbsp; <i class="fa fa-square" style="color:#DC143C"></i></div>
     """
    m.get_root().html.add_child(folium.Element(legend_html))

    st_folium(m, width=1200, height=800)

    # --- Affichage du tableau avec recherche et pastille ---
    st.subheader("Analyse des risques par commune")

    # Barre de recherche
    search_term = st.text_input("🔍 Rechercher une commune ou un code INSEE")

    def score_with_badge(score, classe):
        """Retourne un HTML avec une pastille colorée et le score"""
        color = color_map_hex.get(classe, "#808080")
        return (
            f'<div style="display:flex;align-items:center;gap:6px;">'
            f'<div style="width:14px;height:14px;border-radius:50%;background-color:{color};border:1px solid #555;"></div>'
            f"<span>{score:.3f}</span></div>"
        )

    display_cols = ["nom", "code_insee", "score", "Classe de risque", "population"]
    df_to_display = (
        merged_gdf[display_cols]
        .sort_values(by="score", ascending=False)
        .rename(
            columns={"nom": "Commune", "code_insee": "Code INSEE", "score": "Score"}
        )
    )

    # Filtrage selon la recherche
    if search_term:
        search_lower = search_term.lower()
        df_to_display = df_to_display[
            df_to_display["Commune"].str.lower().str.contains(search_lower)
            | df_to_display["Code INSEE"].str.contains(search_lower)
        ]

    # Remplacement de la colonne Score par HTML avec pastille
    df_to_display["Score"] = df_to_display.apply(
        lambda row: score_with_badge(row["Score"], row["Classe de risque"]), axis=1
    )

    st.markdown(
        """
        <style>
        table {
            width: 100% !important;
        }
        table td, table th {
            padding: 8px;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    # Affichage HTML pour garder la pastille
    st.write(df_to_display.to_html(escape=False, index=False), unsafe_allow_html=True)


elif df_scores.empty:
    st.info("En attente de données de Snowflake pour la date sélectionnée...")
else:
    st.error(
        "Impossible d'afficher la carte car les données géographiques n'ont pas pu être chargées."
    )
