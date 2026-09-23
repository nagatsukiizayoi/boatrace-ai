
# -*- coding: utf-8 -*-
"""N20 LIVE viewer v0 (read-only)"""
import datetime as dt
import io

import pandas as pd
import requests
import streamlit as st

OWNER = "nagatsukiizayoi"
REPO = "boatrace-ai"
BRANCH = "data-live"
BASE_DIR = "data/live"

API = f"https://api.github.com/repos/{OWNER}/{REPO}/contents"
RAW = f"https://raw.githubusercontent.com/{OWNER}/{REPO}/{BRANCH}"

VENUE_NAMES = {}

st.set_page_config(page_title="N20 LIVE viewer", layout="wide")


def _headers():
    try:
        tok = st.secrets.get("GITHUB_TOKEN", "")
    except Exception:
        tok = ""
    h = {"Accept": "application/vnd.github+json"}
    if tok:
        h["Authorization"] = f"Bearer {tok}"
    return h


@st.cache_data(ttl=300, show_spinner=False)
def list_days():
    r = requests.get(f"{API}/{BASE_DIR}", params={"ref": BRANCH},
                     headers=_headers(), timeout=20)
    r.raise_for_status()
    return sorted([x["name"] for x in r.json() if x["type"] == "dir"], reverse=True)


@st.cache_data(ttl=120, show_spinner=False)
def list_csv(day):
    r = requests.get(f"{API}/{BASE_DIR}/{day}", params={"ref": BRANCH},
                     headers=_headers(), timeout=20)
    r.raise_for_status()
    return sorted([x["name"] for x in r.json()
                   if x["type"] == "file" and x["name"].endswith(".csv")])


@st.cache_data(ttl=120, show_spinner=False)
def load_day(day, names):
    frames = []
    for n in names:
        r = requests.get(f"{RAW}/{BASE_DIR}/{day}/{n}", timeout=30)
        if r.status_code != 200:
            continue
        df = pd.read_csv(io.BytesIO(r.content), dtype=str, keep_default_na=False)
        df["_src"] = n
        frames.append(df)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def prep(df):
    if df.empty:
        return df
    d = df.copy()
    for c in ("o", "q3t", "exhibition_time", "lag_sec", "boat_no", "race_no", "window"):
        if c in d.columns:
            d[c + "_n"] = pd.to_numeric(d[c], errors="coerce")
    d["valid"] = d["lag_sec_n"] > 0 if "lag_sec_n" in d.columns else True
    if "venue_code" in d.columns:
        d["venue"] = d["venue_code"].map(lambda v: VENUE_NAMES.get(v, v))
    return d


st.sidebar.header("データ選択")
try:
    days = list_days()
except Exception as e:
    st.error(f"日付一覧の取得に失敗しました: {e}")
    st.stop()

if not days:
    st.warning("data/live にデータがありません。")
    st.stop()

day = st.sidebar.selectbox("日付 (YYYYMMDD)", days, index=0)
files = list_csv(day)
sel_files = st.sidebar.multiselect("実行ファイル (run_*.csv)", files, default=files)
only_valid = st.sidebar.checkbox("締切前 (lag_sec > 0) のみ表示", value=True)
if st.sidebar.button("キャッシュを破棄して再読込"):
    st.cache_data.clear()
    st.rerun()

d = prep(load_day(day, tuple(sel_files)))
if d.empty:
    st.warning("選択したファイルにデータがありません。")
    st.stop()

st.title("N20 LIVE viewer")
st.caption("収集データの閲覧専用です。予想・買い目・期待収益は表示しません。")

n_all = len(d)
n_valid = int(d["valid"].sum())
exh_cov = float((d["exhibition_time"] != "").mean()) if "exhibition_time" in d.columns else float("nan")
q3t_cov = float(d["q3t_n"].notna().mean()) if "q3t_n" in d.columns else float("nan")
win_cov = float((d["o_status"] == "ok").mean()) if "o_status" in d.columns else float("nan")

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("行数", f"{n_all:,}")
c2.metric("締切前 (lag>0)", f"{n_valid:,}", f"{n_valid / n_all * 100:.1f}%")
c3.metric("exh_cov", f"{exh_cov:.3f}")
c4.metric("q3t_cov", f"{q3t_cov:.3f}")
c5.metric("win_cov", f"{win_cov:.3f}")

if n_valid < n_all:
    st.error(f"lag_sec <= 0（締切後取得）が {n_all - n_valid} 行あります。学習・評価では除外してください。")

if "window" in d.columns:
    st.subheader("window 別カバレッジ")
    g = d.groupby("window", dropna=False)
    cov = pd.DataFrame({
        "rows": g.size(),
        "lag_gt0": g["valid"].mean().round(3),
        "exh_cov": g["exhibition_time"].apply(lambda s: (s != "").mean()).round(3),
        "win_cov": g["o_status"].apply(lambda s: (s == "ok").mean()).round(3),
        "lag_med": g["lag_sec_n"].median().round(1),
    }).reset_index()
    st.dataframe(cov, use_container_width=True, hide_index=True)

st.subheader("レース一覧")
keys = [k for k in ("venue", "race_no", "window") if k in d.columns]
races = (d.groupby(keys, dropna=False)
          .agg(rows=("boat_no", "size"),
               exh=("exhibition_time", lambda s: int((s != "").sum())),
               lag_min=("lag_sec_n", "min"))
          .reset_index().sort_values(keys))
st.dataframe(races, use_container_width=True, hide_index=True)

st.subheader("明細")
view = d[d["valid"]] if only_valid else d
cols = [c for c in ("venue", "race_no", "window", "boat_no", "o", "o_status",
                    "q3t", "exhibition_time", "obs_ts", "deadline", "lag_sec", "_src")
        if c in view.columns]
tbl = view[cols].copy()


def _row_style(row):
    try:
        bad = float(row.get("lag_sec", "nan")) <= 0
    except (TypeError, ValueError):
        bad = False
    return ["background-color: #ffe0e0"] * len(row) if bad else [""] * len(row)


st.dataframe(tbl.style.apply(_row_style, axis=1),
             use_container_width=True, hide_index=True, height=520)

st.download_button("表示中のデータをCSVダウンロード",
                   tbl.to_csv(index=False).encode("utf-8-sig"),
                   file_name=f"n20_{day}_view.csv", mime="text/csv")

st.caption(f"source: {OWNER}/{REPO}@{BRANCH} {BASE_DIR}/{day} / "
           f"rendered {dt.datetime.now():%Y-%m-%d %H:%M:%S}")
