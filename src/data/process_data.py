import pandas as pd
from pathlib import Path


ROOT_DIR = Path(__file__).resolve(strict=True).parent.parent.parent
DOMAIN = "https://inversionesyfinanzas.xyz"
DATA_DIR = f"{ROOT_DIR}/data/"


def check_correct_path(path: str) -> str:
    if path.startswith("(") and path.endswith(")"):
        path = path[1:-2]
    if not path.startswith(DOMAIN) or path == "/":
        path = f"{DOMAIN}{path}"
    return path


def arrange_data():
    visiteurs = pd.read_csv(f"{DATA_DIR}/raw/visiteurs.csv")
    visiteurs["is_bot"] = 0
    visiteurs["user_id"] = visiteurs["id"]
    visiteurs = visiteurs.drop(
        columns=[
            "country_name",
            "continent_code",
            "continent_name",
            "session_id",
            "first_visit_date",
            "id",
            "dma_code",
            "is_in_european_union",
            "latitude",
            "longitude",
            "city",
            "region",
            "time_zone",
            "postal_code",
        ]
    )
    visiteurs["http_user_agent"] = visiteurs["http_user_agent"].fillna("bot")
    visiteurs.loc[
        visiteurs["http_user_agent"].str.contains(
            "|".join(["python", "java", "go", "twitter", "facebook", "requests", "bot"])
        ),
        "is_bot",
    ] = 1
    visiteurs_journey = pd.read_csv(f"{DATA_DIR}/raw/visits_historial_visiteurs.csv")
    visiteurs_journey = visiteurs_journey.drop(columns=["id", "parsed", "comes_from"])
    total_seo = pd.merge(visiteurs_journey, visiteurs, on=["user_id"])
    total_seo = total_seo[
        total_seo["current_path"].str.contains("webmanifest.json") == False
    ]
    total_seo.loc[
        ~total_seo["current_path"].str.contains(
            "|".join(
                [
                    "/publicaciones/",
                    "/definicion/",
                    "/screener/analisis-de/",
                    "/p/",
                    "/",
                ]
            )
        ),
        "is_bot",
    ] = 1
    total_seo["current_path"] = total_seo.apply(
        lambda row: check_correct_path(row.current_path), axis=1
    )
    total_seo["country_code"] = total_seo["country_code"].fillna("Unknown")
    total_seo = pd.concat(
        [
            total_seo,
            pd.get_dummies(total_seo.country_code.explode()).sum(level=0),
        ],
        axis=1,
        join="inner",
    )
    total_seo["current_path"] = total_seo["current_path"].map(
        lambda x: x.replace("'", "")
    )
    total_seo = total_seo.drop(columns=["user_id", "country_code"])
    total_seo.to_csv(f"{DATA_DIR}/interim/merged_csv.csv", index=False)
