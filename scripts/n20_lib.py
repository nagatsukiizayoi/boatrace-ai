
import re, warnings
import pandas as pd
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
warnings.filterwarnings('ignore', category=XMLParsedAsHTMLWarning)
def parse_exh2(html):
    """beforeinfo -> DataFrame[boat_no, exhibition_time, tilt, st_tenji, exh_method]"""
    soup = BeautifulSoup(html, 'html5lib')
    rows, method = [], 'table'
    for tb in soup.find_all('tbody'):
        tds = [c.get_text(' ', strip=True) for c in tb.find_all('td')]
        if not tds: continue
        bn = next((t for t in tds[:2] if re.fullmatch(r'[1-6]', t)), None)
        if bn is None: continue
        ex = next((t for t in tds if re.fullmatch(r'[67]\.\d{2}', t)), None)
        ti = next((t for t in tds if re.fullmatch(r'-?\d\.\d', t)), None)
        st = next((t for t in tds if re.fullmatch(r'F?\.\d{2}', t)), None)
        rows.append(dict(boat_no=bn, exhibition_time=float(ex) if ex else None,
                         tilt=float(ti) if ti else None, st_tenji=st))
    got = [r for r in rows if r['exhibition_time'] is not None]
    if len(got) != 6:                                   # フォールバック：文書順
        vals = re.findall(r'\b[67]\.\d{2}\b', html)
        if len(vals) == 6:
            rows = [dict(boat_no=str(i+1), exhibition_time=float(v), tilt=None, st_tenji=None)
                    for i, v in enumerate(vals)]
            method = 'regex'
        else:
            method = f'FAIL(n={len(got)},re={len(vals)})'
    d = pd.DataFrame(rows).drop_duplicates('boat_no').sort_values('boat_no')
    d['exh_method'] = method
    return d.reset_index(drop=True)

def parse_wx2(html):
    """beforeinfo -> dict(気象)"""
    soup = BeautifulSoup(html, 'html5lib')
    t = soup.get_text(' ', strip=True)
    def num(pat):
        m = re.search(pat, t)
        return float(m.group(1)) if m else None
    w = dict(air_temp=num(r'気温\s*([\d.]+)\s*℃'),
             wind_speed=num(r'風速\s*([\d.]+)\s*m'),
             water_temp=num(r'水温\s*([\d.]+)\s*℃'),
             wave_height=num(r'波高\s*([\d.]+)\s*cm'))
    m = re.search(r'is-wind(\d+)', html); w['wind_dir'] = int(m.group(1)) if m else None
    m = re.search(r'is-weather(\d+)', html); w['weather_cd'] = int(m.group(1)) if m else None
    return w


# === v6: oddstf 艇番明示 + ステータス分類（2026-09-22 事故対策）===
import re as _re, warnings as _w
from bs4 import BeautifulSoup as _BS, XMLParsedAsHTMLWarning as _XW
_w.filterwarnings('ignore', category=_XW)
_ZT = str.maketrans('０１２３４５６７８９．－','0123456789.-')

def _sp(h): return _BS(_re.sub(r'^\s*<\?xml[^>]*\?>','',h), 'lxml')

def _nv(t):
    if t is None: return (None,'row_absent')
    t=t.replace(',','').replace('\u3000','').strip().translate(_ZT)
    if not t: return (None,'empty')
    if '欠場' in t: return (None,'scratch')
    if '取消' in t or '不成立' in t: return (None,'cancelled')
    if _re.fullmatch(r'\d+(\.\d+)?', t):
        v=float(t)
        if v==0.0: return (None,'zero_pool')
        if v<1.0: return (None,'lt1')
        return (v,'ok')
    return (None,'unparsed:'+t[:10])

def parse_oddstf6(html):
    """戻り: (race_status, {boat_no: {'win':float|None,'win_st':str}})"""
    if 'is-boatColor' not in html:
        return ('race_cancelled' if '中止' in html else 'no_boat_rows'), {}
    s=_sp(html); occ={}
    for el in s.select('[class*="is-boatColor"]'):
        m=_re.search(r'is-boatColor([1-6])\b', ' '.join(el.get('class') or []))
        if not m: continue
        tr=el.find_parent('tr')
        if tr is None: continue
        occ.setdefault(int(m.group(1)),[]).append(
            [x.get_text(strip=True) for x in tr.find_all(['td','th'])])
    out={}
    for b in range(1,7):
        c=occ.get(b,[])
        w,st=_nv(c[0][-1] if c else None)
        out[b]={'win':w,'win_st':st}
    return 'ok', out


# === v7: odds3t パーサ（rowspan展開・テキスト艇番・完全性検証） 2026-09-22 ===
import itertools as _it
_ALL120 = set(_it.permutations(range(1,7), 3))

def _cls(c): return ' '.join(c.get('class') or []) if c is not None else ''

def _tx(c):
    return '' if c is None else c.get_text(strip=True).replace(',','').replace('\u3000','').translate(_ZT)

def _bcol(c):
    if c is None: return None
    m = _re.search(r'is-boatColor([1-6])\b', _cls(c))
    return int(m.group(1)) if m else None

def _bnum(t):
    """テキストから艇番。class色は2着艇の色を引きずるため使用禁止。"""
    return int(t) if (len(t)==1 and t.isdigit() and 1 <= int(t) <= 6) else None

def _grid(table):
    pend, grid = {}, []
    for tr in table.find_all('tr'):
        row = {}
        for col,(rem,c) in list(pend.items()):
            row[col] = c
            if rem-1 > 0: pend[col] = (rem-1, c)
            else: del pend[col]
        ci = 0
        for c in tr.find_all(['td','th']):
            rs = int(c.get('rowspan') or 1); cs = int(c.get('colspan') or 1)
            for _ in range(cs):
                while ci in row: ci += 1
                row[ci] = c
                if rs > 1: pend[ci] = (rs-1, c)
                ci += 1
        grid.append([row.get(i) for i in range(max(row)+1)] if row else [])
    return grid

def _hdr_map(g0, width):
    hdr, cur = [], None
    for j in range(width):
        b = _bcol(g0[j] if j < len(g0) else None)
        if b: cur = b
        hdr.append(cur)
    return hdr

def parse_odds3t(html):
    """戻り: (status, {(b1,b2,b3): odds}, info)。完全性(120組)を満たさなければ ERR。"""
    if 'is-boatColor' not in html:
        return ('race_cancelled' if '中止' in html else 'no_rows'), {}, {}
    sp = _sp(html)
    tb = max(sp.find_all('table'), key=lambda x: len(x.find_all('td')), default=None)
    if tb is None: return 'no_table', {}, {}
    g = _grid(tb)
    if not g: return 'empty_grid', {}, {}
    W  = max(len(r) for r in g)
    hm = _hdr_map(g[0], W)
    if sorted({x for x in hm if x}) != [1,2,3,4,5,6]:
        return f'bad_header:{sorted({x for x in hm if x})}', {}, {}
    combos, anom, nonnum = {}, [], {}
    for ri, row in enumerate(g[1:], 1):
        for j in range(len(row)):
            if 'oddsPoint' not in _cls(row[j]): continue
            b1 = hm[j] if j < len(hm) else None
            b2 = _bnum(_tx(row[j-2])) if j >= 2 else None
            b3 = _bnum(_tx(row[j-1])) if j >= 1 else None
            if b1 is None or b2 is None or b3 is None or len({b1,b2,b3}) != 3:
                anom.append((ri, j, b1, b2, b3)); continue
            t = _tx(row[j])
            if _re.fullmatch(r'\d+(\.\d+)?', t):
                v = float(t)
                if v >= 1.0: combos[(b1,b2,b3)] = v
            else:
                nonnum[(b1,b2,b3)] = t[:6]
    sig = sum(1/v for v in combos.values()) if combos else 0.0
    got = set(combos) | set(nonnum)
    info = {'n': len(combos), 'n_nonnum': len(nonnum), 'sigma': round(sig,4),
            'anomaly': len(anom), 'cover120': len(got), 'complete120': got == _ALL120,
            'per_first': {b: sum(1 for k in combos if k[0]==b) for b in range(1,7)}}
    if anom: return f'ERR_anomaly={len(anom)}', combos, info
    if got != _ALL120: return f'ERR_cover={len(got)}', combos, info
    if len(combos) == 120 and not (1.15 <= sig <= 1.55):
        return f'ERR_sigma={sig:.4f}', combos, info
    return 'ok', combos, info

def marginals3t(combos):
    """3連単から1着確率マージナル q_i（控除率で正規化）"""
    m = {b: sum(1/v for k,v in combos.items() if k[0]==b) for b in range(1,7)}
    t = sum(m.values())
    return {b: (m[b]/t if t else None) for b in m}
