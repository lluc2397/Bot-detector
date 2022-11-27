import pandas as pd
from pathlib import Path


ROOT_DIR = Path(__file__).resolve(strict=True).parent.parent.parent
DOMAIN = "https://inversionesyfinanzas.xyz"
DATA_DIR = f"{ROOT_DIR}/data/"


def check_correct_path(path: str, **kargs) -> str:
    if kargs.get("fix", False):
        return "https://google.com"
    if path.startswith("(") and path.endswith(")"):
        path = path[1:-2]
    path = path.replace("'", "")
    if path[0] == "/":
        path = f"{DOMAIN}{path}"
    return path


def arrange_data():
    visiteurs = pd.read_csv(f"{DATA_DIR}/raw/visiteurs.csv")
    visiteurs["is_bot"] = False
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
    ] = True
    visiteurs_journey = pd.read_csv(f"{DATA_DIR}/raw/visits_historial_visiteurs.csv")
    # visiteurs_journey = visiteurs_journey.drop(columns=["id", "parsed", "comes_from"])
    visiteurs_journey = visiteurs_journey.drop(columns=["id", "parsed"])
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
    ] = True
    total_seo.loc[
        total_seo["current_path"].str.contains(
            "|".join(
                [
                    ".well-known/",
                    "/actuator/gate",
                    "/wp-login.php",
                    "Autodiscover",
                    "_ignition",
                    "Portal0000",
                    "sdk",
                    "HNAP1",
                    "pools",
                    "__media__",
                    "auth/login",
                    "CommPilot",
                    "plugins/installer",
                    "editor/filemanager",
                    "console",
                    "wp-includes",
                    "phpunit",
                    "index.php",
                    "solr",
                    "a=fetch&content",
                    "XDEBUG_SESSION",
                ]
            )
        ),
        "is_bot",
    ] = True
    total_seo["current_path"] = total_seo["current_path"].map(
        lambda row: check_correct_path(row)
    )
    total_seo["comes_from"] = total_seo["comes_from"].map(
        lambda row: check_correct_path(row, fix=True)
    )
    # Check the difference between map, apply and applymap to see if it's corrent to use one or the other
    # https://stackoverflow.com/questions/19798153/difference-between-map-applymap-and-apply-methods-in-pandas
    total_seo["country_code"] = total_seo["country_code"].fillna("unknown")
    total_seo["http_user_agent"] = total_seo["http_user_agent"].astype("category")
    total_seo["comes_from"] = total_seo["comes_from"].astype("category")
    total_seo["current_path"] = total_seo["current_path"].astype("category")
    total_seo["ip"] = total_seo["ip"].astype("category")
    total_seo["country_code"] = total_seo["country_code"].astype("category")
    total_seo["ip"] = total_seo["ip"].astype("category")
    total_seo["user_id"] = total_seo["user_id"].astype("int16")
    total_seo.to_parquet(f"{DATA_DIR}/interim/country_codes.parquet", index=False)
    total_seo = pd.concat(
        [
            total_seo,
            pd.get_dummies(total_seo.country_code.explode()).sum(level=0),
            # pd.get_dummies(total_seo.country_code.explode()).groupby(level=0).sum(),
            # FutureWarning: Using the level keyword in DataFrame and Series aggregations is deprecated and will be removed in a future version. Use groupby instead. df.sum(level=1) should use df.groupby(level=1).sum().
            # pd.get_dummies(total_seo.country_code.explode()).sum(level=0),
        ],
        axis=1,
        join="inner",
    )

    total_seo = total_seo.drop(columns=["user_id", "country_code"])
    total_seo.to_parquet(
        f"{DATA_DIR}/interim/country_codes_as_int.parquet", index=False
    )
