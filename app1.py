from __future__ import annotations

import base64
from datetime import date, datetime, timedelta
from io import BytesIO
import math
from pathlib import Path
import re
import unicodedata

import pandas as pd
import streamlit as st
from vba_finance import (
    DatePr_Cp,
    DateSerial,
    calcul_taux,
    mati,
    prix_amortissable,
)

from storage import (
    add_asfim_files,
    add_bam_files,
    get_asfim_records,
    get_bam_records,
    init_storage,
    list_asfim_dates,
    list_asfim_files,
    list_bam_dates,
    list_bam_files,
    summarize_asfim_history,
    summarize_bam_history,
)

st.set_page_config(page_title="Suivi des OPCVM", layout="wide")
init_storage()

APP_USER = "opcvmabb"
APP_PASSWORD = "albarid2026"


def _fix_ui_text(value: object) -> object:
    if not isinstance(value, str):
        return value
    txt = value
    # Recover common mojibake (UTF-8 interpreted as latin1), max 2 passes.
    for _ in range(2):
        try:
            fixed = txt.encode("latin1", errors="ignore").decode("utf-8", errors="ignore")
            if fixed and fixed != txt:
                txt = fixed
                continue
        except Exception:
            pass
        break
    replacements = {
        "Donn?es": "Donn\u00e9es",
        "donn?es": "donn\u00e9es",
        "R?sum?": "R\u00e9sum\u00e9",
        "r?sum?": "r\u00e9sum\u00e9",
        "M?diane": "M\u00e9diane",
        "m?diane": "m\u00e9diane",
        "March?": "March\u00e9",
        "march?": "march\u00e9",
        "s?lectionn?": "s\u00e9lectionn\u00e9",
        "s?lectionn?e": "s\u00e9lectionn\u00e9e",
        "d?finis": "d\u00e9finis",
        "d?finie": "d\u00e9finie",
        "trouv?": "trouv\u00e9",
        "T?l?charger": "T\u00e9l\u00e9charger",
        "?cart": "\u00c9cart",
        "Donn??es": "Donn\u00e9es",
        "donn??es": "donn\u00e9es",
        "R??sum??": "R\u00e9sum\u00e9",
        "March??": "March\u00e9",
        "M??diane": "M\u00e9diane",
        "s??lectionn??": "s\u00e9lectionn\u00e9",
        "s??lectionn??e": "s\u00e9lectionn\u00e9e",
        "T??l??charger": "T\u00e9l\u00e9charger",
    }
    for bad, good in replacements.items():
        txt = txt.replace(bad, good)
    return txt


def _fix_ui_obj(value: object) -> object:
    if isinstance(value, str):
        return _fix_ui_text(value)
    if isinstance(value, list):
        return [_fix_ui_obj(v) for v in value]
    if isinstance(value, tuple):
        return tuple(_fix_ui_obj(v) for v in value)
    return value


def _patch_streamlit_text_rendering() -> None:
    names = [
        "title",
        "header",
        "subheader",
        "markdown",
        "caption",
        "info",
        "warning",
        "error",
        "success",
        "button",
        "download_button",
        "text_input",
        "selectbox",
        "radio",
        "metric",
    ]
    for name in names:
        original = getattr(st, name, None)
        if original is None or getattr(original, "__name__", "") == "wrapped_ui_text":
            continue

        def wrapped_ui_text(*args, __orig=original, **kwargs):
            if args:
                fixed = list(args)
                fixed[0] = _fix_ui_obj(fixed[0])
                args = tuple(fixed)
            for key in ("label", "placeholder", "help", "value", "caption", "options"):
                if key in kwargs:
                    kwargs[key] = _fix_ui_obj(kwargs[key])
            return __orig(*args, **kwargs)

        setattr(st, name, wrapped_ui_text)


_patch_streamlit_text_rendering()


def _resolve_logo_path() -> Path | None:
    # Priority order:
    # 1) canonical path in assets/
    # 2) common root filenames (useful on GitHub/Streamlit Cloud)
    preferred = Path("assets/abb_logo.png")
    if preferred.exists():
        return preferred

    for candidate in ("ALBARID.png", "albarid.png", "logo.png"):
        p = Path(candidate)
        if p.exists():
            return p

    assets = Path("assets")
    if not assets.exists():
        return None
    for ext in ("*.png", "*.jpg", "*.jpeg", "*.webp"):
        files = sorted(assets.glob(ext))
        if files:
            return files[0]
    return None


LOGO_PATH = _resolve_logo_path()

ISIN_MAP = {
    "quotidien": {
        "OCT": {
            "MA0000038960",
            "MA0000040396",
            "MA0000040768",
            "MA0000041717",
            "MA0000037616",
            "MA0000041394",
            "MA0000042152",
            "MA0000037962",
            "MA0000038002",
            "MA0000036261",
            "MA0000038754",
            "MA0000040024",
            "MA0000037715",
            "MA0000037624",
            "MA0000038655",
        },
        "OMLT": {
            "MA0000042186",
            "MA0000041329",
            "MA0000041261",
            "MA0000040214",
            "MA0000038978",
            "MA0000038309",
            "MA0000038267",
            "MA0000038200",
            "MA0000030785",
            "MA0000035917",
            "MA0000036915",
            "MA0000030280",
            "MA0000040016",
            "MA0000042210",
            "MA0000039695",
        },
        "Diversifiés": {
            "MA0000030470",
            "MA0000042202",
            "MA0000040065",
            "MA0000038986",
            "MA0000038358",
            "MA0000038259",
            "MA0000038077",
            "MA0000030520",
            "MA0000036501",
        },
    },
    "hebdomadaire": {
        "OCT": set(),
        "OMLT": {
            "MA0000042079",
            "MA0000041170",
            "MA0000041014",
            "MA0000039190",
            "MA0000037475",
            "MA0000037087",
            "MA0000035099",
        },
        "Diversifiés": {
            "MA0000042087",
            "MA0000042004",
            "MA0000041725",
            "MA0000039554",
            "MA0000038408",
            "MA0000037665",
            "MA0000037640",
            "MA0000036634",
            "MA0000036782",
            "MA0000039398",
        },
    },
}

# Normalize any legacy/mojibake category key to the canonical label.
for _freq in ("quotidien", "hebdomadaire"):
    _cats = ISIN_MAP.get(_freq, {})
    for _k in list(_cats.keys()):
        if "Diversifi" in _k and _k != "Diversifiés":
            _cats["Diversifiés"] = _cats.pop(_k)

MARKET_DAILY_ISIN = {
    "OCT": {
        "MA0000030371","MA0000030413","MA0000030595","MA0000035826","MA0000035925","MA0000036048","MA0000036154","MA0000036246","MA0000036261","MA0000036287","MA0000036352","MA0000036873","MA0000037202","MA0000037392","MA0000037459","MA0000037483","MA0000037558","MA0000037616","MA0000037624","MA0000037715","MA0000037798","MA0000037889","MA0000037962","MA0000038002","MA0000038119","MA0000038382","MA0000038416","MA0000038432","MA0000038507","MA0000038531","MA0000038655","MA0000038754","MA0000038812","MA0000038929","MA0000038960","MA0000039018","MA0000039117","MA0000039141","MA0000039281","MA0000039463","MA0000039711","MA0000039745","MA0000039851","MA0000039869","MA0000040024","MA0000040107","MA0000040180","MA0000040248","MA0000040313","MA0000040396","MA0000040594","MA0000040677","MA0000040768","MA0000041121","MA0000041154","MA0000041238","MA0000041394","MA0000041402","MA0000041618","MA0000041717","MA0000041766","MA0000041840","MA0000042020","MA0000042061","MA0000042152","MA0000042236","MA0000042459","MA0000042491"
    },
    "OMLT": {
        "MA0000030280","MA0000030298","MA0000030587","MA0000030785","MA0000035677","MA0000035792","MA0000035917","MA0000035933","MA0000035941","MA0000036345","MA0000036600","MA0000036915","MA0000036972","MA0000037061","MA0000037186","MA0000037368","MA0000037376","MA0000037723","MA0000038051","MA0000038101","MA0000038168","MA0000038176","MA0000038200","MA0000038234","MA0000038267","MA0000038283","MA0000038309","MA0000038317","MA0000038523","MA0000038739","MA0000038770","MA0000038903","MA0000038978","MA0000039000","MA0000039075","MA0000039125","MA0000039174","MA0000039232","MA0000039372","MA0000039430","MA0000039448","MA0000039471","MA0000039513","MA0000039547","MA0000039570","MA0000039620","MA0000039638","MA0000039653","MA0000039661","MA0000039695","MA0000039703","MA0000039778","MA0000039802","MA0000039836","MA0000040016","MA0000040214","MA0000040388","MA0000040412","MA0000040420","MA0000040438","MA0000040503","MA0000040552","MA0000041261","MA0000041329","MA0000041501","MA0000041519","MA0000041592","MA0000042129","MA0000042186","MA0000042210","MA0000042368","MA0000042418"
    },
    "Diversifi\u00e9s": {
        "MA0000030470","MA0000030512","MA0000030520","MA0000030579","MA0000035842","MA0000035867","MA0000036329","MA0000036402","MA0000036501","MA0000037384","MA0000037749","MA0000038077","MA0000038143","MA0000038184","MA0000038259","MA0000038358","MA0000038374","MA0000038390","MA0000038556","MA0000038846","MA0000038853","MA0000038861","MA0000038879","MA0000038986","MA0000039166","MA0000039216","MA0000039273","MA0000039315","MA0000039323","MA0000039331","MA0000039356","MA0000039364","MA0000039455","MA0000039604","MA0000039679","MA0000039752","MA0000040065","MA0000040792","MA0000041667","MA0000042111","MA0000042202","MA0000042392"
    },
}

OUR_FUNDS_ISIN = {
    "quotidien": {
        "OCT": {
            "MA0000038960","MA0000040396","MA0000040768","MA0000041717","MA0000037616",
            "MA0000041394","MA0000042152","MA0000037962","MA0000038002","MA0000036261",
            "MA0000038754","MA0000040024","MA0000037715","MA0000037624","MA0000038655",
        },
        "OMLT": {
            "MA0000042186","MA0000041329","MA0000041261","MA0000040214","MA0000038978",
            "MA0000038309","MA0000038267","MA0000038200","MA0000030785","MA0000035917",
            "MA0000036915","MA0000030280","MA0000040016","MA0000042210","MA0000039695",
        },
        "Diversifi\u00e9s": {
            "MA0000030470","MA0000042202","MA0000040065","MA0000038986","MA0000038358",
            "MA0000038259","MA0000038077","MA0000030520","MA0000036501",
        },
    },
    "hebdomadaire": {
        "OCT": set(),
        "OMLT": {
            "MA0000042079","MA0000041170","MA0000041014","MA0000039190",
            "MA0000037475","MA0000037087","MA0000035099",
        },
        "Diversifi\u00e9s": {
            "MA0000042087","MA0000042004","MA0000041725","MA0000039554","MA0000038408",
            "MA0000037665","MA0000037640","MA0000036634","MA0000036782","MA0000039398",
        },
    },
}

if "active_page" not in st.session_state:
    st.session_state.active_page = "OCT"
if "authenticated" not in st.session_state:
    st.session_state.authenticated = False


def _auth_screen() -> None:
    st.markdown(
        """
        <style>
        .stApp {
          background:
            radial-gradient(900px 520px at 50% 0%, rgba(255,255,255,0.10), rgba(255,255,255,0)),
            radial-gradient(500px 300px at 85% 20%, rgba(242,211,0,0.10), rgba(242,211,0,0)),
            linear-gradient(135deg, #3a2a25 0%, #57423a 48%, #2b211d 100%);
        }
        .auth-shell {
          min-height: 90vh;
          display: flex;
          align-items: center;
          justify-content: center;
        }
        .auth-card {
          width: 420px;
          max-width: 100%;
          background: rgba(255,255,255,0.14);
          border: 1px solid rgba(255,255,255,0.28);
          border-radius: 16px;
          padding: 20px 18px 18px;
          color: #fff;
          box-shadow: 0 16px 40px rgba(0,0,0,0.35);
          backdrop-filter: blur(10px);
        }
        .auth-title { font-size: 30px; font-weight: 800; margin-bottom: 4px; color: #ffffff; text-align: center; }
        .auth-sub { color: #f7efea; margin-bottom: 10px; text-align: center; }
        .auth-brand { color: #ffe788; font-weight: 700; margin-bottom: 12px; text-align: center; }
        .auth-logo { display: flex; justify-content: center; margin-bottom: 8px; }
        .auth-logo img { width: 72px; height: 72px; object-fit: contain; }
        </style>
        <div class="auth-shell">
          <div class="auth-card">
            <div class="auth-logo" id="auth-logo-anchor"></div>
            <div class="auth-title">Suivi des OPCVM</div>
            <div class="auth-brand">Al Barid Bank</div>
            <div class="auth-sub">Connexion sécurisée à la plateforme interne</div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    c1, c2, c3 = st.columns([2, 3, 2])
    with c2:
        if LOGO_PATH and LOGO_PATH.exists():
            st.image(str(LOGO_PATH), width=72)
        with st.form("login_form"):
            username = st.text_input("Username", placeholder="opcvmabb")
            password = st.text_input("Code", type="password", placeholder="••••••••")
            submitted = st.form_submit_button("Se connecter", use_container_width=True)
    if submitted:
        if username == APP_USER and password == APP_PASSWORD:
            st.session_state.authenticated = True
            st.rerun()
        else:
            st.error("Identifiants invalides")
    st.stop()


if not st.session_state.authenticated:
    _auth_screen()


def _inject_theme() -> None:
    st.markdown(
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Manrope:wght@400;600;700;800&display=swap');

        :root {
          --abb-bg: #f2efec;
          --abb-card: #ffffff;
          --abb-yellow: #f2d300;
          --abb-ink: #1f2a37;
          --abb-muted: #5b6674;
          --abb-border: #e5ded8;
          --abb-choco: #5b463f;
        }

        .stApp {
          background:
            radial-gradient(1100px 520px at 80% -20%, rgba(242,211,0,0.22) 0%, rgba(242,211,0,0) 55%),
            radial-gradient(900px 500px at 10% -10%, rgba(91,70,63,0.16) 0%, rgba(91,70,63,0) 60%),
            linear-gradient(180deg, #f5f1ee 0%, var(--abb-bg) 100%);
          color: var(--abb-ink);
          font-family: Calibri, "Manrope", sans-serif;
        }

        [data-testid="stSidebar"] {
          background:
            radial-gradient(500px 240px at 10% 5%, rgba(255,255,255,0.10), rgba(255,255,255,0)),
            linear-gradient(180deg, rgba(74,56,50,0.96) 0%, rgba(56,42,37,0.96) 100%);
          backdrop-filter: blur(8px);
          border-right: 1px solid rgba(255,255,255,0.12);
        }

        h1, h2, h3 {
          color: var(--abb-ink);
          letter-spacing: -0.02em;
          font-family: Calibri, "Manrope", sans-serif;
        }

        [data-testid="stMetric"] {
          background: var(--abb-card);
          border: 1px solid var(--abb-border);
          border-radius: 14px;
          padding: 10px 12px;
          box-shadow: 0 1px 2px rgba(0, 0, 0, 0.04);
        }

        .abb-banner {
          background: linear-gradient(120deg, rgba(255,255,255,0.96) 0%, rgba(255,247,203,0.95) 100%);
          border: 1px solid var(--abb-border);
          border-left: 6px solid var(--abb-yellow);
          border-radius: 14px;
          padding: 12px 16px;
          margin-bottom: 10px;
        }

        .abb-banner-title {
          font-size: 1.05rem;
          font-weight: 800;
          color: var(--abb-ink);
          margin-bottom: 4px;
        }

        .abb-banner-sub {
          font-size: 0.92rem;
          color: var(--abb-muted);
        }

        .stButton > button,
        [data-testid="baseButton-secondary"],
        [data-testid="baseButton-primary"] {
          border-radius: 10px !important;
          border: 1px solid #d8cfc9 !important;
          background: #fffdf7 !important;
          color: #243042 !important;
          font-weight: 700 !important;
          font-family: Calibri, "Manrope", sans-serif !important;
        }

        .stDownloadButton > button {
          border-radius: 10px !important;
          background: #fff7cc !important;
          border: 1px solid #e8d77f !important;
          color: #1f2a37 !important;
          font-weight: 700 !important;
          font-family: Calibri, "Manrope", sans-serif !important;
        }

        [data-testid="stDataFrame"] {
          border: 1px solid var(--abb-border);
          border-radius: 10px;
          overflow: hidden;
        }

        .side-nav-title {
          font-size: 0.95rem;
          font-weight: 800;
          color: #f2e8e3;
          margin-top: 8px;
          margin-bottom: 6px;
          letter-spacing: 0.02em;
        }

        .side-brand {
          display: flex; align-items: center; gap: 10px;
          padding: 8px 4px 6px;
          border-bottom: 1px dashed rgba(255,255,255,0.25);
          margin-bottom: 8px;
        }
        .side-brand-text { font-weight: 800; color: #fff7e8; }
        .kpi-up { color: #0a8f2e; font-weight: 700; }
        .kpi-down { color: #c00000; font-weight: 700; }

        .section-card {
          background: #ffffff;
          border: 1px solid var(--abb-border);
          border-radius: 12px;
          padding: 12px;
          margin-bottom: 10px;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _render_brand_header() -> None:
    c1, c2 = st.columns([1, 9])
    with c1:
        if LOGO_PATH and LOGO_PATH.exists():
            st.image(str(LOGO_PATH), width=78)
    with c2:
        st.markdown(
            """
            <div class="abb-banner">
              <div class="abb-banner-title">Al Barid Bank</div>
              <div class="abb-banner-sub">Suivi des OPCVM • Plateforme interne de pilotage</div>
            </div>
            """,
            unsafe_allow_html=True,
        )


_inject_theme()


def _norm_col(value: object) -> str:
    text = "" if value is None else str(value)
    text = text.strip().lower().replace("\u00a0", " ")
    # Repair common mojibake sequences before normalization.
    text = (
        text.replace("Ã©", "e")
        .replace("Ã¨", "e")
        .replace("Ãª", "e")
        .replace("Ã«", "e")
        .replace("Ã ", "a")
        .replace("Ã¢", "a")
        .replace("Ã¹", "u")
        .replace("Ã»", "u")
        .replace("Ã´", "o")
        .replace("Ã®", "i")
        .replace("Ã¯", "i")
        .replace("â€™", "'")
        .replace("â€¢", "")
    )
    # Remove accents so matching works with both accented/non-accented headers.
    text = "".join(ch for ch in unicodedata.normalize("NFKD", text) if not unicodedata.combining(ch))
    text = re.sub(r"\s+", " ", text)
    return text


def _to_num(value: object) -> float | None:
    if value is None:
        return None
    txt = str(value).strip().replace("\u00a0", "")
    if not txt:
        return None
    txt = txt.replace("%", "").replace(" ", "").replace(",", ".")
    try:
        return float(txt)
    except ValueError:
        return None


def _format_amount(value: object) -> str:
    num = _to_num(value)
    if num is None:
        return str(value)
    return f"{num:,.2f}"


def _format_percent(value: object) -> str:
    raw = "" if value is None else str(value).strip()
    num = _to_num(value)
    if num is None:
        return raw
    pct = num
    if "%" not in raw and abs(num) <= 1:
        pct = num * 100
    return f"{pct:.3f}%"


def _format_perf_for_kpi(value: object) -> str:
    return _format_percent(value)


def _format_table(df: pd.DataFrame, perf_col: str) -> pd.DataFrame:
    out = df.copy()
    if "AN" in out.columns:
        out["AN"] = out["AN"].map(_format_amount)
    if "VL" in out.columns:
        out["VL"] = out["VL"].map(_format_amount)
    if "YTD" in out.columns:
        out["YTD"] = out["YTD"].map(_format_percent)
    if perf_col in out.columns:
        out[perf_col] = out[perf_col].map(_format_percent)
    return out


def _standardize_asfim_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    rename_map: dict[str, str] = {}
    for col in out.columns:
        n = _norm_col(col)
        if n == "societe de gestion":
            rename_map[col] = "Société de Gestion"
        elif n == "periodicite vl":
            rename_map[col] = "Périodicité VL"
        elif n == "code isin":
            rename_map[col] = "Code ISIN"
        elif n == "opcvm":
            rename_map[col] = "OPCVM"
        elif n == "classification":
            rename_map[col] = "Classification"
        elif n == "souscripteurs" or n == "souscripteur":
            rename_map[col] = "Souscripteurs"
        elif n == "an":
            rename_map[col] = "AN"
        elif n == "vl":
            rename_map[col] = "VL"
        elif n in {"ytd", "yield", "yld"}:
            rename_map[col] = "YTD"
        elif n == "maturite":
            rename_map[col] = "Maturité"
    if rename_map:
        out = out.rename(columns=rename_map)
    return out


def _col_by_norm(df: pd.DataFrame, target: str) -> str | None:
    t = _norm_col(target)
    for c in df.columns:
        if _norm_col(c) == t:
            return c
    return None


def _detect_headers(raw: pd.DataFrame, frequency: str) -> tuple[int, dict[int, str]] | tuple[None, None]:
    aliases = {
        "Code ISIN": {"code isin", "isin"},
        "OPCVM": {"opcvm"},
        "Societe de Gestion": {"societe de gestion", "soci??t?? de gestion"},
        "Periodicite VL": {"periodicite vl", "p??riodicit?? vl", "periodicite", "p??riodicit??"},
        "Classification": {"classification", "classe"},
        "Souscripteurs": {"souscripteurs", "souscripteur"},
        "AN": {"an"},
        "VL": {"vl"},
        "YTD": {"ytd", "yield", "yld"},
        "1 jour": {"1 jour", "1j", "1 journee", "1 journ??e"},
        "1 semaine": {"1 semaine", "1 sem", "1semaine"},
    }

    perf_required = "1 jour" if frequency == "quotidien" else "1 semaine"

    for r in range(min(30, len(raw))):
        vals = raw.iloc[r].tolist()
        mapped: dict[int, str] = {}
        for i, v in enumerate(vals):
            n = _norm_col(v)
            for canonical, syns in aliases.items():
                if n in syns:
                    mapped[i] = canonical
                    break

        found = set(mapped.values())
        required = {
            "Code ISIN",
            "OPCVM",
            "Societe de Gestion",
            "Periodicite VL",
            "Classification",
            "Souscripteurs",
            "AN",
            "VL",
            "YTD",
            perf_required,
        }
        if required.issubset(found):
            return r, mapped
    return None, None


@st.cache_data(show_spinner=False)
def parse_asfim_file(path: str, frequency: str) -> pd.DataFrame:
    xls = pd.ExcelFile(path)
    perf_col = "1 jour" if frequency == "quotidien" else "1 semaine"

    for sheet in xls.sheet_names:
        raw = xls.parse(sheet_name=sheet, header=None, dtype=str).fillna("")
        header_row, mapped = _detect_headers(raw, frequency)
        if header_row is None or mapped is None:
            continue

        keep = sorted(mapped.items(), key=lambda x: x[0])
        idxs = [i for i, _ in keep]
        body = raw.iloc[header_row + 1 :].copy()
        body = body.iloc[:, idxs]
        body.columns = [name for _, name in keep]
        body = body.fillna("")
        body = body[body["Code ISIN"].astype(str).str.strip() != ""]

        body = _standardize_asfim_columns(body)

        def col_for(norm_name: str) -> str | None:
            for c in body.columns:
                if _norm_col(c) == norm_name:
                    return c
            return None

        required = {
            "code isin": col_for("code isin"),
            "opcvm": col_for("opcvm"),
            "societe de gestion": col_for("societe de gestion"),
            "periodicite vl": col_for("periodicite vl"),
            "classification": col_for("classification"),
            "souscripteurs": col_for("souscripteurs"),
            "an": col_for("an"),
            "vl": col_for("vl"),
            "ytd": col_for("ytd"),
            "perf": col_for(_norm_col(perf_col)),
        }
        if any(v is None for v in required.values()):
            continue

        perf_name = "Performance quotidienne" if frequency == "quotidien" else "Performance hebdomadaire"
        out = pd.DataFrame(
            {
                "Code ISIN": body[required["code isin"]],
                "OPCVM": body[required["opcvm"]],
                "Soci??t?? de Gestion": body[required["societe de gestion"]],
                "P??riodicit?? VL": body[required["periodicite vl"]],
                "Classification": body[required["classification"]],
                "Souscripteurs": body[required["souscripteurs"]],
                "AN": body[required["an"]],
                "VL": body[required["vl"]],
                "YTD": body[required["ytd"]],
                perf_name: body[required["perf"]],
            }
        )
        out["performance_num"] = out[perf_name].map(_to_num)
        return out

    return pd.DataFrame(
        columns=[
            "Code ISIN",
            "OPCVM",
            "Soci??t?? de Gestion",
            "P??riodicit?? VL",
            "Classification",
            "Souscripteurs",
            "AN",
            "VL",
            "YTD",
            "Performance quotidienne" if frequency == "quotidien" else "Performance hebdomadaire",
            "performance_num",
        ]
    )


def _latest_file_for_date(frequency: str, date_key: str) -> str | None:
    records = get_asfim_records(frequency=frequency, date_key=date_key)
    for rec in records:
        path = Path(str(rec.get("storage_path", "")))
        if path.exists():
            return str(path)
    return None


def _build_export_excel(df: pd.DataFrame, perf_col: str) -> bytes:
    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
        df.to_excel(writer, sheet_name="Données", index=False)
        wb = writer.book
        ws = writer.sheets["Données"]

        header_fmt = wb.add_format({"bold": True, "font_color": "#FFFFFF", "bg_color": "#C00000", "border": 1})
        green_fmt = wb.add_format({"font_color": "#008000"})
        red_fmt = wb.add_format({"font_color": "#C00000"})

        for c, name in enumerate(df.columns):
            ws.write(0, c, name, header_fmt)
            ws.set_column(c, c, max(12, len(name) + 2))

        if perf_col in df.columns:
            c = df.columns.get_loc(perf_col)
            for r in range(1, len(df) + 1):
                raw = str(df.iloc[r - 1, c])
                num = _to_num(raw)
                fmt = None
                if num is not None:
                    if num > 0:
                        fmt = green_fmt
                    elif num < 0:
                        fmt = red_fmt
                ws.write(r, c, raw, fmt)

    return buffer.getvalue()


def _fund_history(frequency: str, category: str, isin: str) -> pd.DataFrame:
    dates = list_asfim_dates(frequency)
    perf_col = "Performance quotidienne" if frequency == "quotidien" else "Performance hebdomadaire"
    rows: list[dict[str, object]] = []

    for d in sorted(dates):
        path = _latest_file_for_date(frequency, d)
        if not path:
            continue
        df = parse_asfim_file(path, frequency)
        if df.empty:
            continue
        allowed = ISIN_MAP[frequency].get(category, set())
        df = df[df["Code ISIN"].astype(str).str.strip().isin(allowed)]
        item = df[df["Code ISIN"].astype(str).str.strip() == isin]
        if item.empty:
            continue
        val = item.iloc[0][perf_col]
        rows.append({"Date": d, "performance_num": _to_num(val), "Valeur": str(val)})

    return pd.DataFrame(rows)


def _segment_filter_by_classification(df: pd.DataFrame, category: str) -> pd.DataFrame:
    if df.empty or "Classification" not in df.columns:
        return pd.DataFrame(columns=df.columns)

    target = _norm_col(category)
    work = df.copy()
    work["_cls_norm"] = work["Classification"].astype(str).map(_norm_col)

    # Keep all funds where classification text contains the segment keyword.
    if "diversif" in target:
        mask = work["_cls_norm"].str.contains("diversif", na=False)
    elif target == "oct":
        mask = work["_cls_norm"].str.contains("oct", na=False)
    elif target == "omlt":
        mask = work["_cls_norm"].str.contains("omlt", na=False)
    else:
        mask = work["_cls_norm"].str.contains(target, na=False)

    out = work[mask].drop(columns=["_cls_norm"])
    return out


def _latest_segment_snapshot(frequency: str, category: str) -> tuple[pd.DataFrame, str | None]:
    dates = list_asfim_dates(frequency)
    if not dates:
        return pd.DataFrame(), None

    path = _latest_file_for_date(frequency, dates[0])
    if not path:
        return pd.DataFrame(), None

    df = parse_asfim_file(path, frequency)
    if df.empty:
        return pd.DataFrame(), dates[0]

    perf_col = "Performance quotidienne" if frequency == "quotidien" else "Performance hebdomadaire"
    required = ["Code ISIN", "OPCVM", "Classification", perf_col]
    if any(c not in df.columns for c in required):
        return pd.DataFrame(), dates[0]

    df = df.copy()
    df["Code ISIN"] = df["Code ISIN"].astype(str).str.strip().str.upper()

    # Quotidien: use explicit market ISIN universe (from provided lists) to avoid missing funds.
    if frequency == "quotidien":
        market_set = MARKET_DAILY_ISIN.get(category, set())
        if market_set:
            seg = df[df["Code ISIN"].isin({x.upper() for x in market_set})].copy()
        else:
            seg = _segment_filter_by_classification(df, category).copy()
    else:
        seg = _segment_filter_by_classification(df, category).copy()

    if seg.empty:
        return pd.DataFrame(), dates[0]

    seg["performance_num"] = seg[perf_col].map(_to_num)
    seg = seg[seg["Code ISIN"] != ""]
    return seg, dates[0]


def compute_market_stats(df: pd.DataFrame, perf_col: str) -> dict[str, object]:
    if df.empty or perf_col not in df.columns:
        return {}

    work = df.copy()
    total_count = int(len(work))
    work["perf_num"] = work[perf_col].map(_to_num)
    valid = work.dropna(subset=["perf_num"]).copy()
    if valid.empty:
        return {
            "count": total_count,
            "count_with_perf": 0,
            "best": None,
            "worst": None,
            "mean": None,
            "median": None,
            "q1": None,
            "q2": None,
            "q3": None,
            "best_name": None,
            "worst_name": None,
        }

    best_row = valid.loc[valid["perf_num"].idxmax()]
    worst_row = valid.loc[valid["perf_num"].idxmin()]

    return {
        "count": total_count,
        "count_with_perf": int(len(valid)),
        "best": float(valid["perf_num"].max()),
        "worst": float(valid["perf_num"].min()),
        "mean": float(valid["perf_num"].mean()),
        "median": float(valid["perf_num"].median()),
        "q1": float(valid["perf_num"].quantile(0.25)),
        "q2": float(valid["perf_num"].quantile(0.50)),
        "q3": float(valid["perf_num"].quantile(0.75)),
        "best_name": str(best_row.get("OPCVM", "N/A")),
        "worst_name": str(worst_row.get("OPCVM", "N/A")),
    }


def compute_score(perf_f: float | None, best: float | None, worst: float | None) -> float | None:
    if perf_f is None or best is None or worst is None:
        return None
    if best == worst:
        return 50.0
    score = 100.0 * (perf_f - worst) / (best - worst)
    return float(max(0.0, min(100.0, score)))


def compute_quartile(perf_f: float | None, q1: float | None, q2: float | None, q3: float | None) -> tuple[str, str]:
    if perf_f is None or q1 is None or q2 is None or q3 is None:
        return "N/A", "N/A"
    if perf_f >= q3:
        return "Q4", "Top 25%"
    if perf_f < q1:
        return "Q1", "Bas 25%"
    if perf_f >= q2:
        return "Q3", "Milieu"
    return "Q2", "Milieu"


def compute_fund_vs_market_metrics(selected_row: pd.Series, segment_df: pd.DataFrame, perf_col: str) -> dict[str, object]:
    if segment_df.empty or perf_col not in segment_df.columns:
        return {}

    work = segment_df.copy()
    work["Code ISIN"] = work["Code ISIN"].astype(str).str.strip().str.upper()
    work["perf_num"] = work[perf_col].map(_to_num)
    valid = work.dropna(subset=["perf_num"]).copy()
    if valid.empty:
        return {}

    selected_isin = str(selected_row.get("Code ISIN", "")).strip().upper()
    row = valid[valid["Code ISIN"] == selected_isin]
    if row.empty:
        return {}
    perf_f = float(row.iloc[0]["perf_num"])

    ranked_market = valid.sort_values("perf_num", ascending=False).reset_index(drop=True)
    ranked_market["rank_market"] = ranked_market.index + 1
    rank_market = int(ranked_market.loc[ranked_market["Code ISIN"] == selected_isin, "rank_market"].iloc[0])

    stats = compute_market_stats(valid, perf_col)
    score = compute_score(perf_f, stats.get("best"), stats.get("worst"))
    quartile, position = compute_quartile(perf_f, stats.get("q1"), stats.get("q2"), stats.get("q3"))

    return {
        "rank_market": rank_market,
        "population_market": int(len(valid)),
        "perf": perf_f,
        "score": score,
        "quartile": quartile,
        "position": position,
        "best": stats.get("best"),
        "worst": stats.get("worst"),
        "mean": stats.get("mean"),
        "gap_vs_best": (stats.get("best") - perf_f) if stats.get("best") is not None else None,
        "gap_vs_mean": (perf_f - stats.get("mean")) if stats.get("mean") is not None else None,
        "gap_vs_worst": (perf_f - stats.get("worst")) if stats.get("worst") is not None else None,
    }


def build_our_funds_table(segment_df: pd.DataFrame, our_funds_filter: set[str], perf_col: str) -> pd.DataFrame:
    if segment_df.empty or perf_col not in segment_df.columns:
        return pd.DataFrame()

    market = segment_df.copy()
    market["Code ISIN"] = market["Code ISIN"].astype(str).str.strip().str.upper()
    market["perf_num"] = market[perf_col].map(_to_num)
    market_valid = market.dropna(subset=["perf_num"]).copy()
    if market_valid.empty:
        return pd.DataFrame()

    filt = {x.strip().upper() for x in our_funds_filter}
    our = market_valid[market_valid["Code ISIN"].isin(filt)].copy()
    if our.empty:
        return pd.DataFrame()

    ranked_market = market_valid.sort_values("perf_num", ascending=False).reset_index(drop=True)
    ranked_market["rank_market"] = ranked_market.index + 1

    ranked_our = our.sort_values("perf_num", ascending=False).reset_index(drop=True)
    ranked_our["rank_internal"] = ranked_our.index + 1

    stats = compute_market_stats(market_valid, perf_col)
    rows: list[dict[str, object]] = []

    for _, r in ranked_our.iterrows():
        isin = str(r["Code ISIN"]).strip().upper()
        perf_f = float(r["perf_num"])

        rank_internal = int(r["rank_internal"])
        rank_market = int(ranked_market.loc[ranked_market["Code ISIN"] == isin, "rank_market"].iloc[0])

        score = compute_score(perf_f, stats.get("best"), stats.get("worst"))
        quartile, position = compute_quartile(perf_f, stats.get("q1"), stats.get("q2"), stats.get("q3"))

        rows.append(
            {
                "Code ISIN": isin,
                "OPCVM": r.get("OPCVM"),
                "Classification": r.get("Classification", "N/A"),
                perf_col: r.get(perf_col),
                "Rang interne": f"{rank_internal}/{len(ranked_our)}",
                "Rang marche": f"{rank_market}/{len(ranked_market)}",
                "Score": round(score) if score is not None else None,
                "Quartile": quartile,
                "Position": position,
                "Ecart vs meilleur": (stats.get("best") - perf_f) if stats.get("best") is not None else None,
                "Ecart vs moyenne": (perf_f - stats.get("mean")) if stats.get("mean") is not None else None,
                "Ecart vs moins performant": (perf_f - stats.get("worst")) if stats.get("worst") is not None else None,
            }
        )

    return pd.DataFrame(rows)


def _perf_color(v: object) -> str:
    n = _to_num(v)
    if n is None:
        return ""
    if n > 0:
        return "color: #00a650; font-weight: 700;"
    if n < 0:
        return "color: #c00000; font-weight: 700;"
    return ""


def _format_analysis_table(df: pd.DataFrame, perf_cols: list[str]) -> pd.DataFrame:
    out = df.copy()
    for c in perf_cols:
        if c in out.columns:
            out[c] = out[c].map(_format_percent)
    for c in ["Ecart vs meilleur", "Ecart vs moyenne", "Ecart vs moins performant"]:
        if c in out.columns:
            out[c] = out[c].map(_format_percent)
    return out


def _build_our_funds_excel(df: pd.DataFrame, perf_cols: list[str]) -> bytes:
    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
        df.to_excel(writer, sheet_name="Nos fonds vs marche", index=False)
        wb = writer.book
        ws = writer.sheets["Nos fonds vs marche"]

        header_fmt = wb.add_format({"bold": True, "font_color": "#FFFFFF", "bg_color": "#000000", "border": 1})
        green_fmt = wb.add_format({"font_color": "#008000", "bold": True})
        red_fmt = wb.add_format({"font_color": "#C00000", "bold": True})
        score_fmt = wb.add_format({"bold": True, "bg_color": "#FFF2CC", "border": 1})

        for c, name in enumerate(df.columns):
            ws.write(0, c, name, header_fmt)
            ws.set_column(c, c, max(14, len(str(name)) + 2))

        for r in range(1, len(df) + 1):
            for c, col_name in enumerate(df.columns):
                raw = df.iloc[r - 1, c]
                fmt = None
                safe_raw = raw
                if pd.isna(raw):
                    safe_raw = ""
                elif isinstance(raw, (float, int)) and not pd.isna(raw):
                    if math.isinf(float(raw)):
                        safe_raw = ""
                if col_name in perf_cols or col_name in ["Ecart vs meilleur", "Ecart vs moyenne", "Ecart vs moins performant"]:
                    n = _to_num(raw)
                    if n is not None:
                        fmt = green_fmt if n > 0 else red_fmt if n < 0 else None
                if col_name == "Score":
                    fmt = score_fmt
                ws.write(r, c, safe_raw, fmt)

    return buffer.getvalue()


def _render_market_summary(col, title: str, stats: dict[str, object]) -> None:
    with col:
        st.markdown(f"#### {title}")
        if not stats:
            st.warning("Donnees indisponibles.")
            return
        a1, a2 = st.columns(2)
        a1.metric("Nombre total", stats.get("count", 0))
        a2.metric("Moyenne", _format_percent(stats.get("mean")))
        st.caption(f"Avec performance exploitable: {stats.get('count_with_perf', 0)}")
        b1, b2 = st.columns(2)
        b1.metric("Mediane", _format_percent(stats.get("median")))
        b2.metric("Meilleur", _format_percent(stats.get("best")))
        st.caption(f"Meilleur fonds: {stats.get('best_name', 'N/A')}")
        st.metric("Moins performant", _format_percent(stats.get("worst")))
        st.caption(f"Moins performant: {stats.get('worst_name', 'N/A')}")


def _render_category_page(category: str) -> None:
    # Marche complet du segment: filtre uniquement par Classification (pas seulement nos ISIN)
    daily_df, daily_date = _latest_segment_snapshot("quotidien", category)
    weekly_df, weekly_date = _latest_segment_snapshot("hebdomadaire", category)

    with st.container(border=True):
        st.markdown(f"## Analyse du segment {category}")
        d1, d2 = st.columns(2)
        d1.caption(f"Date donnees quotidiennes: {daily_date or 'N/A'}")
        d2.caption(f"Date donnees hebdomadaires: {weekly_date or 'N/A'}")

    # Barre de dates pour telecharger le fichier source
    st.markdown("### Telechargement par date")
    b1, b2 = st.columns(2)
    with b1:
        q_dates = list_asfim_dates("quotidien")
        if q_dates:
            q_pick = st.selectbox("Date ASFIM Quotidien", q_dates, key=f"pick_daily_{category}")
            q_path = _latest_file_for_date("quotidien", q_pick)
            if q_path and Path(q_path).exists():
                st.download_button(
                    "Telecharger fichier quotidien",
                    data=Path(q_path).read_bytes(),
                    file_name=Path(q_path).name,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key=f"dl_daily_{category}",
                )
        else:
            st.info("Aucune date quotidienne disponible.")

    with b2:
        h_dates = list_asfim_dates("hebdomadaire")
        if h_dates:
            h_pick = st.selectbox("Date ASFIM Hebdomadaire", h_dates, key=f"pick_weekly_{category}")
            h_path = _latest_file_for_date("hebdomadaire", h_pick)
            if h_path and Path(h_path).exists():
                st.download_button(
                    "Telecharger fichier hebdomadaire",
                    data=Path(h_path).read_bytes(),
                    file_name=Path(h_path).name,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key=f"dl_weekly_{category}",
                )
        else:
            st.info("Aucune date hebdomadaire disponible.")

    st.markdown("### Resume du marche")
    left, right = st.columns(2)
    _render_market_summary(left, "Resume Quotidien (Marche)", compute_market_stats(daily_df, "Performance quotidienne"))
    if category == "OCT":
        with right:
            st.markdown("#### Resume Hebdomadaire (Marche)")
            st.info("Non applicable: OCT hebdomadaire absent.")
    elif weekly_df.empty:
        with right:
            st.markdown("#### Resume Hebdomadaire (Marche)")
            st.warning("Donnees hebdomadaires indisponibles.")
    else:
        _render_market_summary(right, "Resume Hebdomadaire (Marche)", compute_market_stats(weekly_df, "Performance hebdomadaire"))

    st.markdown("### Nos fonds AL BARID BANK vs Marche (segment)")
    our_daily_isin = OUR_FUNDS_ISIN.get("quotidien", {}).get(category, set())
    our_weekly_isin = OUR_FUNDS_ISIN.get("hebdomadaire", {}).get(category, set())

    daily_our = build_our_funds_table(daily_df, our_daily_isin, "Performance quotidienne") if not daily_df.empty else pd.DataFrame()

    weekly_our = pd.DataFrame()
    if category == "OCT" and not our_weekly_isin:
        st.info("Nos fonds hebdo OCT non definis")
    elif not weekly_df.empty:
        weekly_our = build_our_funds_table(weekly_df, our_weekly_isin, "Performance hebdomadaire")

    if daily_our.empty and weekly_our.empty:
        st.warning("Aucun de nos fonds trouve pour ce segment.")
        merged_our = pd.DataFrame()
    else:
        if daily_our.empty:
            merged_our = weekly_our.copy()
        elif weekly_our.empty:
            merged_our = daily_our.copy()
        else:
            keep_week = [
                "Code ISIN",
                "Performance hebdomadaire",
                "Rang interne",
                "Rang marche",
                "Score",
                "Quartile",
                "Position",
                "Ecart vs meilleur",
                "Ecart vs moyenne",
                "Ecart vs moins performant",
            ]
            merged_our = daily_our.merge(
                weekly_our[keep_week],
                on="Code ISIN",
                how="left",
                suffixes=("_Q", "_H"),
            )
            merged_our = merged_our.rename(
                columns={
                    "Rang interne_Q": "Rang interne (Q)",
                    "Rang marche_Q": "Rang marche (Q)",
                    "Score_Q": "Score (Q)",
                    "Quartile_Q": "Quartile (Q)",
                    "Position_Q": "Position (Q)",
                    "Ecart vs meilleur_Q": "Ecart Q vs meilleur",
                    "Ecart vs moyenne_Q": "Ecart Q vs moyenne",
                    "Ecart vs moins performant_Q": "Ecart Q vs moins performant",
                    "Rang interne_H": "Rang interne (H)",
                    "Rang marche_H": "Rang marche (H)",
                    "Score_H": "Score (H)",
                    "Quartile_H": "Quartile (H)",
                    "Position_H": "Position (H)",
                    "Ecart vs meilleur_H": "Ecart H vs meilleur",
                    "Ecart vs moyenne_H": "Ecart H vs moyenne",
                    "Ecart vs moins performant_H": "Ecart H vs moins performant",
                }
            )

        perf_cols = [c for c in ["Performance quotidienne", "Performance hebdomadaire"] if c in merged_our.columns]
        show = _format_analysis_table(merged_our, perf_cols)
        st.dataframe(show.style.applymap(_perf_color, subset=perf_cols), use_container_width=True)

        excel_bytes = _build_our_funds_excel(show, perf_cols)
        st.download_button(
            "Telecharger Excel",
            data=excel_bytes,
            file_name=f"nos_fonds_vs_marche_{category}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key=f"dl_our_vs_market_{category}",
        )

    st.markdown("### Analyse du fonds s\u00e9lectionn\u00e9")
    if daily_df.empty and weekly_df.empty:
        st.info("Aucune donn\u00e9e disponible pour ce segment.")
        return

    freq_options = ["Quotidien"] if category == "OCT" else ["Quotidien", "Hebdomadaire"]
    freq_ui = st.radio("Type d'analyse", freq_options, horizontal=True, key=f"freq_{category}")
    active_df = daily_df if freq_ui == "Quotidien" else weekly_df
    if active_df.empty:
        st.warning(f"Donnees {freq_ui.lower()} indisponibles pour ce segment.")
        return

    options = [f"{r['OPCVM']} ({r['Code ISIN']})" for _, r in active_df.iterrows()]
    selected = st.selectbox("Selectionner un fonds", options=options, key=f"fund_{category}_{freq_ui}")
    isin = selected.split("(")[-1].replace(")", "").strip().upper()
    row = active_df[active_df["Code ISIN"].astype(str).str.strip().str.upper() == isin]
    if row.empty:
        st.warning("Fonds introuvable.")
        return

    selected_row = row.iloc[0]
    d_metrics = compute_fund_vs_market_metrics(selected_row, daily_df, "Performance quotidienne") if not daily_df.empty else {}
    w_metrics = compute_fund_vs_market_metrics(selected_row, weekly_df, "Performance hebdomadaire") if not weekly_df.empty else {}

    id1, id2, id3 = st.columns(3)
    id1.metric("Nom fonds", str(selected_row.get("OPCVM", "N/A")))
    id2.metric("ISIN", str(selected_row.get("Code ISIN", "N/A")))
    id3.metric("Classification", str(selected_row.get("Classification", "N/A")))

    p1, p2 = st.columns(2)
    with p1:
        st.markdown("#### Positionnement Quotidien")
        if not d_metrics:
            st.info("Donnees quotidiennes insuffisantes.")
        else:
            st.metric("Rang marche", f"{d_metrics['rank_market']}/{d_metrics['population_market']}")
            st.metric("Score", f"{round(d_metrics['score']) if d_metrics.get('score') is not None else 'N/A'}")
            st.metric("Quartile", f"{d_metrics.get('quartile', 'N/A')} ({d_metrics.get('position', 'N/A')})")
            st.caption(f"Ecart vs meilleur: {_format_percent(d_metrics.get('gap_vs_best'))}")
            st.caption(f"Ecart vs moyenne: {_format_percent(d_metrics.get('gap_vs_mean'))}")
            st.caption(f"Ecart vs moins performant: {_format_percent(d_metrics.get('gap_vs_worst'))}")

    if category != "OCT":
        with p2:
            st.markdown("#### Positionnement Hebdomadaire")
            if not w_metrics:
                st.info("Donnees hebdomadaires insuffisantes.")
            else:
                st.metric("Rang marche", f"{w_metrics['rank_market']}/{w_metrics['population_market']}")
                st.metric("Score", f"{round(w_metrics['score']) if w_metrics.get('score') is not None else 'N/A'}")
                st.metric("Quartile", f"{w_metrics.get('quartile', 'N/A')} ({w_metrics.get('position', 'N/A')})")
                st.caption(f"Ecart vs meilleur: {_format_percent(w_metrics.get('gap_vs_best'))}")
                st.caption(f"Ecart vs moyenne: {_format_percent(w_metrics.get('gap_vs_mean'))}")
                st.caption(f"Ecart vs moins performant: {_format_percent(w_metrics.get('gap_vs_worst'))}")

    class_value = str(selected_row.get("Classification", "")).strip().lower()
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("#### Comparaison vs classification (Quotidien)")
        if d_metrics and not daily_df.empty and "Classification" in daily_df.columns:
            sub = daily_df[daily_df["Classification"].astype(str).str.strip().str.lower() == class_value].copy()
            sub["n"] = sub["Performance quotidienne"].map(_to_num)
            sub = sub.dropna(subset=["n"])
            if not sub.empty:
                mean_class = float(sub["n"].mean())
                msg = "Surperforme sa classification" if d_metrics["perf"] > mean_class else "Sous-performe sa classification"
                st.success(f"{msg} (moyenne classe: {_format_percent(mean_class)})")

    if category != "OCT":
        with c2:
            st.markdown("#### Comparaison vs classification (Hebdomadaire)")
            if w_metrics and not weekly_df.empty and "Classification" in weekly_df.columns:
                sub = weekly_df[weekly_df["Classification"].astype(str).str.strip().str.lower() == class_value].copy()
                sub["n"] = sub["Performance hebdomadaire"].map(_to_num)
                sub = sub.dropna(subset=["n"])
                if not sub.empty:
                    mean_class = float(sub["n"].mean())
                    msg = "Surperforme sa classification" if w_metrics["perf"] > mean_class else "Sous-performe sa classification"
                    st.success(f"{msg} (moyenne classe: {_format_percent(mean_class)})")

    g1, g2 = st.columns(2)
    if d_metrics:
        with g1:
            st.markdown("#### Mini graphique Quotidien")
            st.bar_chart(pd.DataFrame({"Valeur": [d_metrics["best"], d_metrics["mean"], d_metrics["perf"], d_metrics["worst"]]}, index=["Meilleur", "Moyenne", "Fonds", "Moins performant"]))
    if w_metrics:
        with g2:
            st.markdown("#### Mini graphique Hebdomadaire")
            st.bar_chart(pd.DataFrame({"Valeur": [w_metrics["best"], w_metrics["mean"], w_metrics["perf"], w_metrics["worst"]]}, index=["Meilleur", "Moyenne", "Fonds", "Moins performant"]))

    st.markdown("### Classement du segment")
    lb_mode = st.radio("Classement", ["Quotidien", "Hebdomadaire"], horizontal=True, key=f"lb_{category}")
    lb_df = daily_df.copy() if lb_mode == "Quotidien" else weekly_df.copy()
    lb_perf = "Performance quotidienne" if lb_mode == "Quotidien" else "Performance hebdomadaire"
    if lb_df.empty:
        st.info("Classement indisponible.")
    else:
        lb_df["perf_num"] = lb_df[lb_perf].map(_to_num)
        lb_show_raw = lb_df.sort_values("perf_num", ascending=False, na_position="last")[["Code ISIN", "OPCVM", "Classification", lb_perf]].copy()
        lb_show = lb_show_raw.copy()
        lb_show[lb_perf] = lb_show[lb_perf].map(_format_percent)
        st.dataframe(lb_show.style.applymap(_perf_color, subset=[lb_perf]), use_container_width=True)

        st.download_button(
            "Telecharger classement Excel",
            data=_build_export_excel(lb_show, lb_perf),
            file_name=f"classement_{category}_{lb_mode}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key=f"dl_rank_{category}_{lb_mode}",
        )

        if st.button("Afficher Top 5 / Bottom 5", key=f"tb_{category}"):
            t1, t2 = st.columns(2)
            with t1:
                st.markdown("**Top 5**")
                st.dataframe(lb_show.head(5), use_container_width=True)
            with t2:
                st.markdown("**Bottom 5**")
                st.dataframe(lb_show.tail(5), use_container_width=True)

    st.markdown("### Recommandation")
    if d_metrics and w_metrics:
        d = d_metrics["perf"]
        w = w_metrics["perf"]
        if d > 0 and w > 0:
            reco = "Dynamique positive sur le jour et la semaine."
        elif d < 0 and w > 0:
            reco = "Correction court terme, semaine positive."
        elif d > 0 and w < 0:
            reco = "Rebond court terme, semaine negative."
        else:
            reco = "Sous-performance persistante."

        q = d_metrics.get("quartile", "N/A")
        if q == "Q4":
            q_msg = "Fonds dans le Top 25% du segment."
        elif q == "Q1":
            q_msg = "Fonds dans le Bas 25% du segment."
        else:
            q_msg = "Fonds dans la zone m\u00e9diane du segment."

        st.info(f"{reco} {q_msg}")
    else:
        st.warning("Recommandation incomplete: donnees daily/weekly insuffisantes.")


def _mati_pivot_days(date_c1: date) -> int:
    return mati(date_c1, 1)


def _norm_bam_col(v: str) -> str:
    t = str(v).strip().lower().replace("\u00a0", " ")
    t = t.replace("’", "'")
    # Remove accents robustly (é -> e, etc.) for BAM header matching.
    t = "".join(ch for ch in unicodedata.normalize("NFKD", t) if not unicodedata.combining(ch))
    t = re.sub(r"\s+", " ", t)
    return t


def _parse_dt_any(v: object) -> date | None:
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    for dayfirst in (True, False):
        d = pd.to_datetime(s, dayfirst=dayfirst, errors="coerce")
        if pd.notna(d):
            return d.date()
    return None


@st.cache_data(show_spinner=False)
def _parse_bam_curve_file(path: str) -> tuple[pd.DataFrame, str | None]:
    xls = pd.ExcelFile(path)
    for sheet in xls.sheet_names:
        df = xls.parse(sheet_name=sheet, dtype=str).fillna("")
        if df.empty:
            continue
        cols = list(df.columns)
        m_col = None
        t_col = None
        v_col = None
        for c in cols:
            n = _norm_bam_col(str(c))
            if "date d'echeance" in n or "date d'cheance" in n or "echeance" in n:
                m_col = c
            if "taux moyen pondere" in n or n == "taux":
                t_col = c
            if "date de la valeur" in n or "date de valeur" in n:
                v_col = c
        if not (m_col and t_col):
            continue
        work = df[[m_col, t_col] + ([v_col] if v_col else [])].copy()
        work.columns = ["DateEcheance", "Taux"] + (["DateValeur"] if v_col else [])
        work = work[work["DateEcheance"].astype(str).str.strip() != ""]
        if work.empty:
            continue
        if "DateValeur" in work.columns:
            vals = work["DateValeur"].map(_parse_dt_any).dropna()
            if vals.empty:
                continue
            mode_vals = vals.mode()
            if mode_vals.empty:
                continue
            date_valeur = mode_vals.iloc[0]
        else:
            continue
        work["DateEcheance_dt"] = work["DateEcheance"].map(_parse_dt_any)
        work["maturity_days"] = work["DateEcheance_dt"].map(
            lambda d: (d - date_valeur).days if d is not None else None
        )
        work["rate_num"] = work["Taux"].map(_to_num)
        work = work.dropna(subset=["maturity_days", "rate_num"])
        work = work[work["maturity_days"] > 0]
        if work.empty:
            continue
        # Convert percent-like values to decimal rates for interpolation.
        work["rate_dec"] = work["rate_num"].map(lambda x: x / 100.0 if x > 1 else x)
        return work[["maturity_days", "rate_dec"]].sort_values("maturity_days"), date_valeur.strftime("%Y-%m-%d")
    return pd.DataFrame(columns=["maturity_days", "rate_dec"]), None


TARGET_MATS = [
    ("13 s", 13 * 7),
    ("26 s", 26 * 7),
    ("52 s", 52 * 7),
    ("2 ans", 2 * 365),
    ("5 ans", 5 * 365),
    ("10 ans", 10 * 365),
    ("15 ans", 15 * 365),
    ("20 ans", 20 * 365),
    ("30 ans", 30 * 365),
]


def _latest_bam_file_for_date(date_key: str) -> str | None:
    records = get_bam_records(date_key=date_key)
    for rec in records:
        p = Path(str(rec.get("storage_path", "")))
        if p.exists():
            return str(p)
    return None


@st.cache_data(show_spinner=False)
def _build_bam_curve_points(date_key: str) -> dict[str, float] | None:
    path = _latest_bam_file_for_date(date_key)
    if not path:
        return None
    curve, dstr = _parse_bam_curve_file(path)
    if curve.empty or not dstr:
        return None
    mt = [int(v) for v in curve["maturity_days"].tolist()]
    tx = [float(v) for v in curve["rate_dec"].tolist()]
    date_c1 = datetime.strptime(dstr, "%Y-%m-%d").date()
    pivot = _mati_pivot_days(date_c1)
    out: dict[str, float] = {}
    for label, days in TARGET_MATS:
        out[label] = calcul_taux(days, mt, tx, date_c1, pivot)
    return out


def _build_bam_compare_export(selected_j: str, selected_j1: str) -> bytes | None:
    j_curve = _build_bam_curve_points(selected_j)
    j1_curve = _build_bam_curve_points(selected_j1)
    if not j_curve or not j1_curve:
        return None

    cols = [label for label, _ in TARGET_MATS]
    j_vals = [j_curve.get(c) for c in cols]
    j1_vals = [j1_curve.get(c) for c in cols]
    var_vals = [(a - b) if a is not None and b is not None else None for a, b in zip(j_vals, j1_vals)]

    def fmt_pct(x: object) -> str:
        if x is None or (isinstance(x, float) and math.isnan(x)):
            return "-"
        return f"{float(x) * 100:.3f}%"

    df = pd.DataFrame(
        [j_vals, j1_vals, var_vals],
        columns=cols,
        index=[selected_j, selected_j1, "VAR"],
    ).reset_index()
    df = df.rename(columns={"index": "Maturité"})
    for c in cols:
        df[c] = df[c].map(fmt_pct)

    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
        df.to_excel(writer, sheet_name="Courbe BAM", index=False)
    return buffer.getvalue()


def _find_previous_valid_bam_date(selected_j: str, bam_dates: list[str]) -> str | None:
    """Return the closest previous BAM date that can be parsed into curve points."""
    if selected_j not in bam_dates:
        return None
    j_index = bam_dates.index(selected_j)
    for d in bam_dates[j_index + 1 :]:
        if _build_bam_curve_points(d):
            return d
    return None


def _curve_reco_comment_from_history(
    dates: list[str],
    start_index: int,
    cols: list[str],
    window: int = 5,
) -> tuple[str, str]:
    """Build recommendation/comment based on historical BAM transitions."""
    if len(dates) < 2 or start_index >= len(dates) - 1:
        return "Données", "Historique BAM insuffisant pour générer un commentaire."

    short_labels = ["13 s", "26 s", "52 s"]
    long_labels = ["10 ans", "15 ans", "20 ans", "30 ans"]
    avg_vars: list[float] = []
    slope_deltas: list[float] = []

    end_index = min(len(dates) - 1, start_index + max(1, window))
    for i in range(start_index, end_index):
        d_j = dates[i]
        d_j1 = dates[i + 1]
        c_j = _build_bam_curve_points(d_j)
        c_j1 = _build_bam_curve_points(d_j1)
        if not c_j or not c_j1:
            continue

        pair_vars = []
        for k in cols:
            a = c_j.get(k)
            b = c_j1.get(k)
            if a is not None and b is not None:
                pair_vars.append(a - b)
        if not pair_vars:
            continue
        avg_vars.append(sum(pair_vars) / len(pair_vars))

        short_vals = [c_j.get(k) - c_j1.get(k) for k in short_labels if c_j.get(k) is not None and c_j1.get(k) is not None]
        long_vals = [c_j.get(k) - c_j1.get(k) for k in long_labels if c_j.get(k) is not None and c_j1.get(k) is not None]
        if short_vals and long_vals:
            slope_deltas.append((sum(long_vals) / len(long_vals)) - (sum(short_vals) / len(short_vals)))

    if not avg_vars:
        return "Données", "Impossible de calculer la tendance historique."

    mean_hist = sum(avg_vars) / len(avg_vars)
    up_count = sum(1 for v in avg_vars if v > 0)
    down_count = sum(1 for v in avg_vars if v < 0)

    if mean_hist > 0:
        reco = f"Historique: pression haussière des taux ({up_count}/{len(avg_vars)} séances), posture prudente."
    elif mean_hist < 0:
        reco = f"Historique: détente des taux ({down_count}/{len(avg_vars)} séances), posture plus constructive."
    else:
        reco = "Historique: stabilité globale des taux."

    if slope_deltas:
        slope_mean = sum(slope_deltas) / len(slope_deltas)
        if slope_mean > 0:
            com = "Historique: pentification dominante (long terme évolue plus que court terme)."
        elif slope_mean < 0:
            com = "Historique: aplatissement dominant (court terme évolue plus que long terme)."
        else:
            com = "Historique: pente globalement stable."
    else:
        com = "Historique: données insuffisantes pour qualifier la pente."

    return reco, com


def _asfim_daily_fund_timeseries() -> tuple[dict[str, list[tuple[str, float]]], dict[str, str]]:
    dates = list_asfim_dates("quotidien")
    data: dict[str, list[tuple[str, float]]] = {}
    names: dict[str, str] = {}
    if not dates:
        return data, names
    allowed_all = set().union(*ISIN_MAP["quotidien"].values())
    for d in dates:
        path = _latest_file_for_date("quotidien", d)
        if not path:
            continue
        df = parse_asfim_file(path, "quotidien")
        if df.empty or "Performance quotidienne" not in df.columns:
            continue
        df = df[df["Code ISIN"].astype(str).str.strip().isin(allowed_all)]
        for _, r in df.iterrows():
            isin = str(r["Code ISIN"]).strip()
            perf = _to_num(r["Performance quotidienne"])
            if perf is None:
                continue
            names[isin] = str(r["OPCVM"])
            data.setdefault(isin, []).append((d, perf))
    return data, names


def _correlation_insights(curve_metric_by_date: dict[str, float]) -> tuple[str | None, float | None, str | None, float | None]:
    if not curve_metric_by_date:
        return None, None, None, None
    fund_ts, names = _asfim_daily_fund_timeseries()
    if not fund_ts:
        return None, None, None, None

    s_curve = pd.Series(curve_metric_by_date, name="curve")
    corr_scores: list[tuple[str, float]] = []
    for isin, points in fund_ts.items():
        s_fund = pd.Series({d: v for d, v in points}, name="fund")
        merged = pd.concat([s_fund, s_curve], axis=1, join="inner").dropna()
        if len(merged) < 3:
            continue
        corr = merged["fund"].corr(merged["curve"])
        if pd.notna(corr):
            corr_scores.append((f"{names.get(isin, isin)} ({isin})", float(corr)))
    if not corr_scores:
        return None, None, None, None

    most = max(corr_scores, key=lambda x: abs(x[1]))
    least = min(corr_scores, key=lambda x: abs(x[1]))
    return most[0], most[1] * 100.0, least[0], least[1] * 100.0


def _render_curve_page() -> None:
    st.subheader("Suivi de la courbe")
    dates = list_bam_dates()
    if len(dates) < 2:
        st.info("Il faut au moins 2 dates BAM pour comparer J et J-1.")
        return

    selected_j = st.selectbox("Date J", dates, index=0)
    j_index = dates.index(selected_j)
    if j_index + 1 >= len(dates):
        st.warning("Choisir une date J qui a une date précédente J-1.")
        return
    selected_j1 = dates[j_index + 1]

    j_curve = _build_bam_curve_points(selected_j)
    j1_curve = _build_bam_curve_points(selected_j1)
    if not j_curve or not j1_curve:
        st.error("Impossible de construire les courbes interpolées pour J/J-1.")
        return

    cols = [label for label, _ in TARGET_MATS]
    j_vals = [j_curve.get(c) for c in cols]
    j1_vals = [j1_curve.get(c) for c in cols]
    var_vals = [(a - b) if a is not None and b is not None else None for a, b in zip(j_vals, j1_vals)]

    table = pd.DataFrame([j_vals, j1_vals, var_vals], columns=cols, index=[selected_j, selected_j1, "VAR"]).reset_index()
    table = table.rename(columns={"index": "Maturité"})

    def fmt_pct(x: object) -> str:
        if x is None or (isinstance(x, float) and math.isnan(x)):
            return "-"
        return f"{float(x) * 100:.3f}%"

    display = table.copy()
    for c in cols:
        display[c] = display[c].map(fmt_pct)

    def style_var(v: object) -> str:
        n = _to_num(v)
        if n is None:
            return ""
        if n < 0:
            return "color: #C00000; font-weight: bold;"
        if n > 0:
            return "color: #008000; font-weight: bold;"
        return ""

    def _build_curve_compare_excel(df_display: pd.DataFrame, maturity_cols: list[str]) -> bytes:
        buffer = BytesIO()
        with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
            df_display.to_excel(writer, sheet_name="Comparaison", index=False)
            wb = writer.book
            ws = writer.sheets["Comparaison"]

            yellow_header = wb.add_format(
                {"bold": True, "bg_color": "#FFD966", "font_color": "#000000", "border": 1, "align": "center"}
            )
            yellow_first_col = wb.add_format({"bold": True, "bg_color": "#FFD966", "border": 1})
            default_cell = wb.add_format({"border": 1, "align": "center"})
            green_var = wb.add_format({"font_color": "#008000", "bold": True, "border": 1, "align": "center"})
            red_var = wb.add_format({"font_color": "#C00000", "bold": True, "border": 1, "align": "center"})

            # Header row (maturities + first column title) in yellow.
            for c, name in enumerate(df_display.columns):
                ws.write(0, c, name, yellow_header)
                ws.set_column(c, c, max(12, len(name) + 2))

            # Data rows.
            for r in range(1, len(df_display) + 1):
                first_col_val = str(df_display.iloc[r - 1, 0])
                ws.write(r, 0, first_col_val, yellow_first_col)
                for c in range(1, len(df_display.columns)):
                    raw = str(df_display.iloc[r - 1, c])
                    fmt = default_cell
                    if first_col_val == "VAR":
                        n = _to_num(raw)
                        if n is not None:
                            fmt = green_var if n > 0 else red_var
                    ws.write(r, c, raw, fmt)

        return buffer.getvalue()

    table_col, btn_col = st.columns([5, 1])
    with table_col:
        st.dataframe(display.style.applymap(style_var, subset=cols), use_container_width=True)
    with btn_col:
        st.download_button(
            "Télécharger\nExcel",
            data=_build_curve_compare_excel(display, cols),
            file_name=f"Courbe_J_vs_J-1_{selected_j}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )

    reco, com = _curve_reco_comment_from_history(dates, j_index, cols)
    st.markdown(f"**Recommandations:** {reco}")
    st.markdown(f"**Commentaires:** {com}")

    curve_metric_by_date: dict[str, float] = {}
    for d in dates:
        curve = _build_bam_curve_points(d)
        if not curve:
            continue
        vals = [curve.get(k) for k in cols if curve.get(k) is not None]
        if vals:
            curve_metric_by_date[d] = sum(vals) / len(vals)
    most_name, most_corr, least_name, least_corr = _correlation_insights(curve_metric_by_date)
    st.markdown("### Corrélation à la courbe BAM")
    if most_name is None:
        st.info("Données insuffisantes pour calculer les corrélations.")
    else:
        c1, c2 = st.columns(2)
        c1.metric("Plus corrélé", most_name)
        c1.caption(f"Corrélation: {most_corr:.2f}%")
        c2.metric("Moins corrélé", least_name or "N/A")
        if least_corr is not None:
            c2.caption(f"Corrélation: {least_corr:.2f}%")


def _category_from_isin(frequency: str, isin: str) -> str | None:
    for cat, values in ISIN_MAP[frequency].items():
        if isin in values:
            return cat
    return None


def _latest_universe_df() -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for frequency in ["quotidien", "hebdomadaire"]:
        dates = list_asfim_dates(frequency)
        if not dates:
            continue
        path = _latest_file_for_date(frequency, dates[0])
        if not path:
            continue
        df = parse_asfim_file(path, frequency)
        if df.empty:
            continue
        perf_col = "Performance quotidienne" if frequency == "quotidien" else "Performance hebdomadaire"
        allowed = set().union(*ISIN_MAP[frequency].values())
        df = df[df["Code ISIN"].astype(str).str.strip().isin(allowed)].copy()
        if df.empty:
            continue
        df["Frequency"] = frequency
        df["Date"] = dates[0]
        df["Category"] = df["Code ISIN"].astype(str).str.strip().map(lambda x: _category_from_isin(frequency, x))
        df["PerfLabel"] = perf_col
        frames.append(df)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


with st.sidebar:
    if LOGO_PATH and LOGO_PATH.exists():
        st.markdown('<div class="side-brand"><img src="data:image/png;base64,{}" width="34"/><div class="side-brand-text">Al Barid Bank</div></div>'.format(base64.b64encode(LOGO_PATH.read_bytes()).decode("utf-8")), unsafe_allow_html=True)
    else:
        st.markdown('<div class="side-brand"><div class="side-brand-text">Al Barid Bank</div></div>', unsafe_allow_html=True)
    st.markdown('<div class="side-nav-title">Navigation</div>', unsafe_allow_html=True)
    if st.button("Se d\u00e9connecter"):
        st.session_state.authenticated = False
        st.rerun()
    pages = ["OCT", "OMLT", "Diversifi\u00e9s", "Suivi de la courbe", "Analyse", "Export"]
    st.markdown('<div class="side-nav-title">Pages</div>', unsafe_allow_html=True)
    for p in pages:
        label = f"\u25b8 {p}" if p == st.session_state.active_page else p
        if st.button(label, key=f"nav_{p}", use_container_width=True):
            st.session_state.active_page = p
            st.rerun()
    page = st.session_state.active_page


_render_brand_header()

if page == "OCT":
    _render_category_page("OCT")
elif page == "OMLT":
    _render_category_page("OMLT")
elif page == "Diversifi\u00e9s":
    _render_category_page("Diversifi\u00e9s")
elif page == "Suivi de la courbe":
    _render_curve_page()
elif page == "Analyse":
    st.subheader("Analyse")
    universe = _latest_universe_df()
    if universe.empty:
        st.info("Aucune donn\u00e9e ASFIM disponible pour l'analyse.")
    else:
        work = universe.dropna(subset=["performance_num"]).copy()
        if work.empty:
            st.info("Donn\u00e9es de performance indisponibles.")
        else:
            best = work.loc[work["performance_num"].idxmax()]
            worst = work.loc[work["performance_num"].idxmin()]
            # DÃ©fensif: performance la plus proche de 0 (faible amplitude)
            defensive = work.assign(abs_perf=work["performance_num"].abs()).sort_values("abs_perf").iloc[0]
            # Offensif: performance la plus Ã©levÃ©e
            offensive = best

            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Plus performant", f"{best['OPCVM']} ({best['Code ISIN']})")
            c1.caption(f"{best['Category']} | {best['Frequency']} | Perf: {_format_percent(best[best['PerfLabel']])}")
            c1.markdown('<div class="kpi-up">&#9650; performance forte</div>', unsafe_allow_html=True)
            c2.metric("Moins performant", f"{worst['OPCVM']} ({worst['Code ISIN']})")
            c2.caption(f"{worst['Category']} | {worst['Frequency']} | Perf: {_format_percent(worst[worst['PerfLabel']])}")
            c2.markdown('<div class="kpi-down">&#9660; performance faible</div>', unsafe_allow_html=True)
            c3.metric("Fonds offensif", f"{offensive['OPCVM']} ({offensive['Code ISIN']})")
            c3.caption(f"{offensive['Category']} | {offensive['Frequency']} | Perf: {_format_percent(offensive[offensive['PerfLabel']])}")
            c4.metric("Fonds d\u00e9fensif", f"{defensive['OPCVM']} ({defensive['Code ISIN']})")
            c4.caption(f"{defensive['Category']} | {defensive['Frequency']} | Perf: {_format_percent(defensive[defensive['PerfLabel']])}")

            st.markdown("### D\u00e9tails analyse")
            # Afficher la colonne perf selon frequence dans un format unifie.
            details = work.copy()
            details["Performance"] = details.apply(lambda r: r[r["PerfLabel"]], axis=1)
            gestion_col = _col_by_norm(details, "Societe de Gestion")
            show_cols = ["Code ISIN", "OPCVM"]
            if gestion_col:
                show_cols.append(gestion_col)
            show_cols.extend(["Category", "Frequency", "Date", "AN", "VL", "YTD", "Performance"])
            show_cols = [c for c in show_cols if c in details.columns]
            details_display = _format_table(details[show_cols], "Performance")
            st.dataframe(details_display, use_container_width=True)
elif page == "Export":
    st.subheader("Export")
    c_refresh, _ = st.columns([1, 5])
    with c_refresh:
        if st.button("Mise à jour", use_container_width=True):
            st.cache_data.clear()
            st.rerun()

    st.markdown("### Section ASFIM - Upload Historique")
    frequency_ui = st.radio(
        "Type de fichier",
        ["ASFIM Quotidien", "ASFIM Hebdomadaire"],
        horizontal=True,
    )
    frequency = "quotidien" if frequency_ui == "ASFIM Quotidien" else "hebdomadaire"

    uploaded_files = st.file_uploader(
        "Uploader des fichiers ASFIM (.xlsx)",
        type=["xlsx"],
        accept_multiple_files=True,
        key="asfim_multi_upload",
    )

    batch_date_key = st.text_input(
        "Date du lot (optionnel, fallback si la date n'est pas détectée)",
        placeholder="ex: 2026-02-11",
    )

    if st.button("Enregistrer dans l’historique", use_container_width=True):
        if not uploaded_files:
            st.warning("Aucun fichier uploadé.")
        else:
            result = add_asfim_files(uploaded_files, frequency=frequency, batch_date_key=batch_date_key or None)
            saved_count = len(result["saved"])
            error_count = len(result["errors"])

            if saved_count:
                st.success(f"{saved_count} fichier(s) ASFIM enregistré(s).")
                for item in result["saved"]:
                    st.caption(
                        f"- {item['filename']} | date={item['date_key']} | type={item['frequency']} | source_date={item['date_source']}"
                    )

            if error_count:
                st.error(f"{error_count} fichier(s) non enregistrés.")
                for err in result["errors"]:
                    st.caption(f"- {err['filename']} : {err['error']}")

    with st.expander("Archive historique ASFIM", expanded=False):
        st.markdown("### Historique ASFIM")
        summary_rows = summarize_asfim_history()
        if not summary_rows:
            st.info("Aucun historique ASFIM enregistré.")
        else:
            for row in summary_rows:
                st.write(f"- **{row['Type']}** | Date: `{row['Date']}` | Fichiers: {row['Nombre de fichiers']}")

        st.markdown("### Détail des fichiers stockés")
        detail_frequency = st.selectbox("Type", ["quotidien", "hebdomadaire"], index=0)
        available_dates = list_asfim_dates(detail_frequency)

        if not available_dates:
            st.info("Aucune date disponible pour ce type.")
        else:
            detail_date = st.selectbox("Date", available_dates)
            if st.button("Voir fichiers stockés", use_container_width=True):
                files = list_asfim_files(detail_frequency, detail_date)
                if not files:
                    st.info("Aucun fichier pour cette date.")
                else:
                    for f in files:
                        st.write(
                            f"- `{f['filename']}` | original: `{f['original_filename']}` | path: `{f['storage_path']}` | upload: {f['uploaded_at']}"
                        )

    st.markdown("### Section BAM - Upload Historique")
    bam_uploaded_files = st.file_uploader(
        "Uploader des fichiers BAM (.xlsx)",
        type=["xlsx"],
        accept_multiple_files=True,
        key="bam_multi_upload",
    )
    bam_batch_date_key = st.text_input(
        "Date du lot BAM (optionnel, fallback si la date n'est pas détectée)",
        placeholder="ex: 2026-02-12",
        key="bam_batch_date",
    )

    if st.button("Enregistrer historique BAM", use_container_width=True):
        if not bam_uploaded_files:
            st.warning("Aucun fichier BAM uploadé.")
        else:
            result = add_bam_files(bam_uploaded_files, batch_date_key=bam_batch_date_key or None)
            saved_count = len(result["saved"])
            error_count = len(result["errors"])

            if saved_count:
                st.success(f"{saved_count} fichier(s) BAM enregistré(s).")
            if error_count:
                st.error(f"{error_count} fichier(s) BAM non enregistrés.")

    with st.expander("Archive historique BAM", expanded=False):
        bam_summary = summarize_bam_history()
        if not bam_summary:
            st.info("Aucun historique BAM enregistré.")
        else:
            for row in bam_summary:
                st.write(f"- Date: `{row['Date']}` | Fichiers: {row['Nombre de fichiers']}")

        bam_dates_detail = list_bam_dates()
        if bam_dates_detail:
            bam_date = st.selectbox("Date BAM", bam_dates_detail, key="bam_detail_date")
            if st.button("Voir fichiers BAM stock?s", use_container_width=True):
                files = list_bam_files(bam_date)
                if not files:
                    st.info("Aucun fichier BAM pour cette date.")
                else:
                    for f in files:
                        st.write(f"- `{f['filename']}` | path: `{f['storage_path']}` | upload: {f['uploaded_at']}")


    st.markdown("### Export courbe BAM")
    bam_dates = list_bam_dates()
    if not bam_dates:
        st.info("Aucun historique BAM disponible.")
    elif len(bam_dates) < 2:
        st.info("Il faut au moins 2 dates BAM pour exporter J vs J-1.")
    else:
        last_asfim_q = list_asfim_dates("quotidien")
        last_asfim_h = list_asfim_dates("hebdomadaire")
        c1, c2, c3 = st.columns(3)
        c1.metric("Derni\u00e8re date BAM", bam_dates[0])
        c2.metric("Derni\u00e8re date ASFIM quotidien", last_asfim_q[0] if last_asfim_q else "N/A")
        c3.metric("Derni\u00e8re date ASFIM hebdomadaire", last_asfim_h[0] if last_asfim_h else "N/A")

        selected_j = st.selectbox("Date BAM J (export)", bam_dates, key="exp_bam_j")
        if not _build_bam_curve_points(selected_j):
            st.warning(
                "La date J s\u00e9lectionn\u00e9e existe dans l'archive, mais son fichier BAM n'est pas lisible "
                "(colonnes/date de valeur). Choisis une autre date J."
            )
        else:
            selected_j1 = _find_previous_valid_bam_date(selected_j, bam_dates)
            if not selected_j1:
                st.warning("Aucune date J-1 valide trouv\u00e9e dans l'historique BAM pour cette date J.")
            else:
                st.caption(f"J-1 utilis\u00e9 automatiquement: {selected_j1}")
                data = _build_bam_compare_export(selected_j, selected_j1)
                if data is None:
                    st.warning("Impossible de construire l'export BAM pour ces dates.")
                else:
                    st.download_button(
                        "T\u00e9l\u00e9charger courbe BAM (J vs J-1)",
                        data=data,
                        file_name=f"Courbe_BAM_{selected_j}_vs_{selected_j1}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True,
                    )
    st.caption("Historique cumulatif: chaque nouveau fichier est ajout\u00e9, les anciens sont conserv\u00e9s apr\u00e8s red\u00e9marrage.")

