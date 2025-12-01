import geopandas as gpd
import pandas as pd

# Charger le fichier GeoJSON
gdf = gpd.read_file("data_source/circonscriptions-legislatives-p20.geojson")

# Renommer les colonnes
gdf = gdf.rename(columns={
    "codeCirconscription": "circonscription_id",
    "nomCirconscription": "circonscription_name",
    "codeDepartement": "departement_id",
    "nomDepartement": "departement_name",
    "geometry": "geopoints"
})

# 🔹 Ajouter manuellement les circonscriptions manquantes (sans géométrie)
extra_rows = [
    {
        "circonscription_id": "ZT01",
        "circonscription_name": "1ère circonscription",
        "departement_id": "ZT",
        "departement_name": "Saint-Barthélemy et Saint-Martin",
        "geopoints": None
    },
    {
        "circonscription_id": "ZP01",
        "circonscription_name": "1ère circonscription",
        "departement_id": "ZP",
        "departement_name": "Polynésie française",
        "geopoints": None
    },
    {
        "circonscription_id": "ZP02",
        "circonscription_name": "2ème circonscription",
        "departement_id": "ZP",
        "departement_name": "Polynésie française",
        "geopoints": None
    },
    {
        "circonscription_id": "ZP03",
        "circonscription_name": "3ème circonscription",
        "departement_id": "ZP",
        "departement_name": "Polynésie française",
        "geopoints": None
    },
    {
        "circonscription_id": "ZW01",
        "circonscription_name": "Wallis-et-Futuna",
        "departement_id": "ZW",
        "departement_name": "Wallis-et-Futuna",
        "geopoints": None
    },
    {
        "circonscription_id": "ZN01",
        "circonscription_name": "1ère circonscription",
        "departement_id": "ZN",
        "departement_name": "Nouvelle-Calédonie",
        "geopoints": None
    },
    {
        "circonscription_id": "ZN02",
        "circonscription_name": "2ème circonscription",
        "departement_id": "ZN",
        "departement_name": "Nouvelle-Calédonie",
        "geopoints": None
    }
]

extra_df = pd.DataFrame(extra_rows)

# Concaténer avec le GeoDataFrame existant
gdf = pd.concat([gdf, extra_df], ignore_index=True)

# Réordonner les colonnes
ordered_columns = [
    "circonscription_id",
    "circonscription_name",
    "departement_id",
    "departement_name",
    "geopoints"
] + [col for col in gdf.columns if col not in [
    "circonscription_id",
    "circonscription_name",
    "departement_id",
    "departement_name",
    "geopoints"
]]

gdf = gdf[ordered_columns]

# Exporter en CSV
gdf.to_csv("circonscriptions.csv", index=False)

print("✅ circonscriptions.csv généré avec succès !")
