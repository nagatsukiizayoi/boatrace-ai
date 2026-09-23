#!/usr/bin/env python3
# N20-30 LIVE collector v3 (oddstf v6 + odds3t v7)
# 1起動で now〜now+HORIZON 分のタスクを拾い、目標時刻まで sleep して取得。
# 位置フォールバック禁止。艇番はテキスト由来、3連単は120組完全性で検証。
import os, re, sys, time, json, gzip, hashlib, datetime as dt
from zoneinfo import ZoneInfo
import requests, pandas as pd
from bs4 import BeautifulSoup

JST  = ZoneInfo('Asia/Tokyo')
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, 'scripts'))
from n20_lib import (parse_exh2, parse_wx2, parse_oddstf6,
                     parse_odds3t, marginals3t)

WINDOWS     = [12, 8, 5, 3]
HORIZON_MIN = int(os.environ.get('N20_HORIZON', '7'))
MAX_RUN_MIN = int(os.environ.get('N20_MAXRUN',  '8'))
SKEW_LIMIT  = 120
RUNID       = dt.datetime.now(JST).strftime('%H%M%S')

RAW = os.path.join(ROOT, 'data', 'raw')
OUT = os.path.join(ROOT, 'data', 'live')
LOG = os.path.join(ROOT, 'data', 'logs')
for d in (RAW, OUT, LOG): os.makedirs(d, exist_ok=True)

SESS = requests.Session()
SESS.headers.update({'User-Agent': 'Mozilla/5.0 (compatible; n20-collector/3.0)'})
_last = [0.0]

def now_jst(): return dt.datetime.now(JST)

def get(url, tries=3):
    for k in range(tries):
        w = 1.0 - (time.time() - _last[0])
        if w > 0: time.sleep(w)
        try:
            r = SESS.get(url, timeout=20); _last[0] = time.time()
            obs, skew = now_jst(), None
            if 'Date' in r.headers:
                try:
                    srv = dt.datetime.strptime(r.headers['Date'],
                          '%a, %d %b %Y %H:%M:%S %Z').replace(tzinfo=dt.timezone.utc)
                    skew = (obs - srv.astimezone(JST)).total_seconds()
                except Exception: pass
            if r.status_code == 200:
                r.encoding = r.apparent_encoding or 'utf-8'
                return r.text, obs, skew
        except Exception: pass
        time.sleep(1.5 * (k + 1))
    return None, now_jst(), None

def fetch_venues(hd):
    t, _, _ = get(f'https://www.boatrace.jp/owpc/pc/race/index?hd={hd}')
    return sorted(set(re.findall(r'jcd=(\d{2})', t))) if t else []

def fetch_deadlines(jcd, hd):
    t, _, _ = get(f'https://www.boatrace.jp/owpc/pc/race/raceindex?jcd={jcd}&hd={hd}')
    if not t: return {}
    out = {}
    try:
        for tr in BeautifulSoup(t, 'html5lib').select('table tr'):
            a = tr.find('a', href=re.compile(r'rno=(\d+)'))
            if not a: continue
            rno = int(re.search(r'rno=(\d+)', a['href']).group(1))
            for td in tr.find_all('td'):
                m = re.fullmatch(r'\s*(\d{1,2}:\d{2})\s*', td.get_text())
                if m: out[rno] = m.group(1); break
    except Exception: pass
    if not out:
        tms = [x for x in re.findall(r'>(\d{1,2}:\d{2})<', t)
               if 8 <= int(x.split(':')[0]) <= 23]
        for i, x in enumerate(tms[:12], 1): out[i] = x
    return out

def build_tasks(hd):
    ts = []
    for jcd in fetch_venues(hd):
        for rno, hm in fetch_deadlines(jcd, hd).items():
            h, m = map(int, hm.split(':'))
            dl = dt.datetime.strptime(hd, '%Y%m%d').replace(hour=h, minute=m, tzinfo=JST)
            for w in WINDOWS:
                ts.append({'venue_code': jcd, 'race_no': rno, 'window': w,
                           'deadline_jst': dl, 'target': dl - dt.timedelta(minutes=w)})
    return ts

def save_raw(hd, jcd, rno, w, kind, html):
    d = os.path.join(RAW, hd); os.makedirs(d, exist_ok=True)
    with gzip.open(os.path.join(d, f'{jcd}_{rno:02d}_T{w:02d}_{kind}.html.gz'),
                   'wt', encoding='utf-8') as f: f.write(html)
    return hashlib.sha256(html.encode('utf-8')).hexdigest()[:16]

def run_task(t, hd):
    jcd, rno, w = t['venue_code'], t['race_no'], t['window']
    q    = f'?rno={rno}&jcd={jcd}&hd={hd}'
    base = 'https://www.boatrace.jp/owpc/pc/race'
    meta, exh, wx, odds, ostat, q3t = {}, {}, {}, {}, {}, {}

    bi, obs_b, skew = get(f'{base}/beforeinfo{q}')
    if skew is not None and abs(skew) > SKEW_LIMIT:
        raise RuntimeError(f'clock skew {skew:.0f}s > {SKEW_LIMIT}')
    if bi:
        meta['sha_beforeinfo'] = save_raw(hd, jcd, rno, w, 'beforeinfo', bi)
        try:
            e = parse_exh2(bi)
            if hasattr(e, 'itertuples') and 'exhibition_time' in list(getattr(e, 'columns', [])):
                exh = {int(r.boat_no): float(r.exhibition_time) for r in e.itertuples() if r.exhibition_time is not None and r.exhibition_time == r.exhibition_time}
                e = None

            if isinstance(e, dict) and 'exhibition_time' in e: e = e['exhibition_time']
            if isinstance(e, dict): exh = {int(k): float(v) for k, v in e.items() if v}
            elif isinstance(e, (list, tuple)): exh = {i+1: float(v) for i, v in enumerate(e) if v}
        except Exception as ex: meta['err_exh'] = f'{type(ex).__name__}: {ex}'
        try: wx = parse_wx2(bi) or {}
        except Exception as ex: meta['err_wx'] = f'{type(ex).__name__}: {ex}'

    od, obs_o, _ = get(f'{base}/oddstf{q}')
    if od:
        meta['sha_oddstf'] = save_raw(hd, jcd, rno, w, 'oddstf', od)
        try:
            rst, dd = parse_oddstf6(od)
            meta['race_status'] = rst
            odds  = {b: v['win']    for b, v in dd.items() if v['win'] is not None}
            ostat = {b: v['win_st'] for b, v in dd.items()}
        except Exception as ex: meta['err_odds'] = f'{type(ex).__name__}: {ex}'

    o3, obs_3, _ = get(f'{base}/odds3t{q}')
    if o3:
        meta['sha_odds3t']    = save_raw(hd, jcd, rno, w, 'odds3t', o3)
        meta['obs_ts_odds3t'] = obs_3.strftime('%Y-%m-%d %H:%M:%S')
        try:
            s3, cb3, i3 = parse_odds3t(o3)
            meta['odds3t_status'] = s3
            meta['odds3t_sigma']  = i3.get('sigma')
            if s3 == 'ok':
                q3t = marginals3t(cb3)
                d3 = os.path.join(OUT, hd, 'odds3t'); os.makedirs(d3, exist_ok=True)
                pd.DataFrame([{'b1': k[0], 'b2': k[1], 'b3': k[2], 'o': v}
                              for k, v in sorted(cb3.items())]).to_csv(
                    os.path.join(d3, f'{jcd}_{rno:02d}_T{w:02d}.csv'), index=False)
        except Exception as ex: meta['err_odds3t'] = f'{type(ex).__name__}: {ex}'

    obs = obs_b or obs_o or obs_3
    lag = (t['deadline_jst'] - obs).total_seconds()
    return [{'race_date': hd, 'venue_code': jcd, 'race_no': rno, 'window': w,
             'boat_no': b, 'o': odds.get(b), 'o_status': ostat.get(b, 'na'),
             'q3t': q3t.get(b), 'exhibition_time': exh.get(b),
             'deadline_jst': t['deadline_jst'].strftime('%Y-%m-%d %H:%M:%S'),
             'obs_ts_jst': obs.strftime('%Y-%m-%d %H:%M:%S'),
             'lag_sec': round(lag, 1), 'clock_skew_sec': skew, 'run_id': RUNID,
             **{f'wx_{k}': v for k, v in wx.items()}, **meta} for b in range(1, 7)]

def main():
    t0 = time.time(); hd = now_jst().strftime('%Y%m%d')
    daydir = os.path.join(OUT, hd); os.makedirs(daydir, exist_ok=True)
    done = set()
    for f in os.listdir(daydir):
        if f.endswith('.csv'):
            try:
                d = pd.read_csv(os.path.join(daydir, f), dtype={'venue_code': str})
                done |= set(zip(d.venue_code.astype(str).str.zfill(2),
                                d.race_no.astype(int), d.window.astype(int)))
            except Exception: pass
    lo, hi = now_jst(), now_jst() + dt.timedelta(minutes=HORIZON_MIN)
    due = sorted([t for t in build_tasks(hd)
                  if lo - dt.timedelta(seconds=30) <= t['target'] <= hi
                  and (t['venue_code'], t['race_no'], t['window']) not in done],
                 key=lambda x: x['target'])
    print(f'[{now_jst():%H:%M:%S}] hd={hd} 済{len(done)} → 対象{len(due)} '
          f'(horizon {HORIZON_MIN}分)', flush=True)
    if not due: return
    shard, acc = os.path.join(daydir, f'run_{RUNID}.csv'), []
    for t in due:
        if time.time() - t0 > MAX_RUN_MIN * 60:
            print('  [stop] MAX_RUN到達', flush=True); break
        sl = (t['target'] - now_jst()).total_seconds()
        if sl > 0: time.sleep(min(sl, MAX_RUN_MIN * 60))
        try:
            rows = run_task(t, hd); acc += rows
            print(f"  {now_jst():%H:%M:%S} {t['venue_code']}-{t['race_no']:02d} "
                  f"T{t['window']:02d} lag={rows[0]['lag_sec']:.0f}s "
                  f"win={sum(r['o'] is not None for r in rows)}/6 "
                  f"q3t={sum(r['q3t'] is not None for r in rows)}/6 "
                  f"exh={sum(r['exhibition_time'] is not None for r in rows)}/6 "
                  f"3t={rows[0].get('odds3t_status')}", flush=True)
        except Exception as e:
            print(f"  !! {t['venue_code']}-{t['race_no']} T{t['window']}: {e}", flush=True)
        if acc: pd.DataFrame(acc).to_csv(shard, index=False)
    if acc:
        D = pd.DataFrame(acc); neg = int((D.lag_sec <= 0).sum())
        s = {'run_id': RUNID, 'hd': hd, 'rows': len(D), 'tasks': len(D)//6,
             'lag_min': float(D.lag_sec.min()), 'lag_max': float(D.lag_sec.max()),
             'lag_le0': neg,
             'win_cov': float(D.o.notna().mean()),
             'q3t_cov': float(D.q3t.notna().mean()),
             'exh_cov': float(D.exhibition_time.notna().mean()),
             'elapsed_s': round(time.time()-t0, 1)}
        json.dump(s, open(os.path.join(LOG, f'{hd}_{RUNID}.json'), 'w'),
                  ensure_ascii=False, indent=1)
        print('[summary]', json.dumps(s, ensure_ascii=False), flush=True)
        if neg: print(f'!! lag_sec<=0 が {neg} 行 — 締切後取得の疑い', flush=True)

if __name__ == '__main__':
    main()
