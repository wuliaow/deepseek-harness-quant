# -*- coding: utf-8 -*-
"""DSHQuant 盘中监控 - 规则引擎v2（AND/OR 组合 + 冷却 + 降级源）+ 落盘
用法: python scripts/_intraday_rules.py
v3 变更（2026-10-22 14:41）：
1) 修复本地 bars.db 单位混用：2026-10-15 及之后 volume=手、amount=千元；10-14 及之前为原始股/元。
   证据：10-21 太极 local 1,340,286.3手*100=134.03M股 vs CLI 同日盘中累计106.04M股；amount 2,007,726.296千元*1000=2.008B元 vs CLI 1.588B元（盘中）→ 全日量价自洽。
   归一化后量比口径一致，代理值从名义 462x 修正为 ~1.6x。
2) 防骚扰冷却细化到"同标的同规则"：候选四强规则按 (rule, 标的代码) 去重；太极规则按 rule 去重。
3) CLI 原始返回优先读 logs/_raw_cli_<日期>.json（当日实时采集落盘），内嵌 JSON 仅历史兜底。
v4 变更（2026-10-22）：修复沪深300盘中度 gap。
- 根因：hithink `market snapshot` 不含指数行（FUYAO_3001 Target not found，非瞬时错误），
  旧代码把 index_000300 硬编码为 unavailable，导致 index_000300_drop_gt1_5 规则永远不触发。
- 修法：新增 fetch_hs300_index()，主源 hithink `index snapshot --thscodes 000300.SH`（shell=True 解析 npm .cmd shim），
  降级源新浪 hq.sinajs.cn/list=sh000300；两源都失败才 available=False（不编造信号）。
- 效果：index_000300_drop_gt1_5 规则恢复真实生效（日内跌幅>1.5%并入通知）。
"""
import sqlite3, glob, os, json, sys, subprocess, urllib.request
from datetime import datetime, timezone, timedelta

TZ = timezone(timedelta(hours=8))
NOW = datetime.now(TZ)
DATE = NOW.strftime('%Y%m%d')
LOG = os.path.join('logs', 'intraday_monitor_%s.jsonl' % DATE)

_RAW_FILE = os.environ.get('INTRA_RAW_CLI', os.path.join('logs', '_raw_cli_%s.json' % DATE))
if os.path.exists(_RAW_FILE):
    with open(_RAW_FILE, 'r', encoding='utf-8') as _f:
        RAW_CLI_JSON = _f.read()
else:
    RAW_CLI_JSON = r"""
{"ok":true,"command":"market.snapshot","data":{"item":[{"thscode":"600667.SH","ticker":"600667","volume":106037130,"turnover":1588193400,"last_price":15.04,"price_change":0.45,"price_change_ratio_pct":3.084304,"open_price":14.74,"high_price":15.23,"low_price":14.62,"prev_price":14.59},{"thscode":"600497.SH","ticker":"600497","volume":44771020,"turnover":363118440,"last_price":8.1,"price_change":0.14,"price_change_ratio_pct":1.758794,"open_price":8.02,"high_price":8.19,"low_price":8.01,"prev_price":7.96},{"thscode":"000630.SZ","ticker":"000630","volume":70246878,"turnover":366482770,"last_price":5.2,"price_change":0.02,"price_change_ratio_pct":0.3861,"open_price":5.17,"high_price":5.25,"low_price":5.16,"prev_price":5.18},{"thscode":"002558.SZ","ticker":"002558","volume":28284548,"turnover":596903510,"last_price":20.76,"price_change":-0.45,"price_change_ratio_pct":-2.121641,"open_price":21.25,"high_price":21.49,"low_price":20.74,"prev_price":21.21},{"thscode":"002709.SZ","ticker":"002709","volume":50625699,"turnover":1387645000,"last_price":26.84,"price_change":-1.25,"price_change_ratio_pct":-4.449982,"open_price":28.01,"high_price":28.18,"low_price":26.72,"prev_price":28.09}],"timestamp":1792561967000,"total":5},"meta":{"source":"remote","request_id":"d688fe050a364e54b2c6020ec1f39a0b","count":5,"truncated":false,"schema_version":"1"}}
"""

CACHE = 'data/cache'
DBS = [os.path.join(CACHE, 'bars.db')] + sorted(glob.glob(os.path.join(CACHE, 'bars_incr*.db')))
UNIT_FIX_FROM = '2026-10-15'  # 该日期起 local volume=手 / amount=千元（见文件头证据）

CODES = ['600667.SH', '600497.SH', '000630.SZ', '002558.SZ', '002709.SZ']
CAND_CODES = ['600497.SH', '000630.SZ', '002558.SZ', '002709.SZ']


def local_rows(code, limit=8):
    """合并各库该 code 的 qfq 日线，按日期倒序去重（只读）；volume/amount 归一为 股/元"""
    merged = {}
    for db in DBS:
        if not os.path.exists(db):
            continue
        try:
            con = sqlite3.connect('file:%s?mode=ro' % db.replace('\\', '/'), uri=True)
            q = "SELECT date, close, volume, amount FROM daily_bar WHERE code=? AND adjust='qfq' ORDER BY date DESC LIMIT ?"
            for d, c, v, a in con.execute(q, (code, limit)):
                if d >= UNIT_FIX_FROM:
                    v, a = v * 100.0, a * 1000.0
                merged[d] = (c, v, a)
            con.close()
        except Exception as e:
            print('WARN %s: %s' % (db, e), file=sys.stderr)
    return sorted(merged.items(), key=lambda kv: kv[0], reverse=True)


local = {}
for code in CODES:
    rs = local_rows(code)
    info = {'dates': [r[0] for r in rs], 'closes': [r[1][0] for r in rs]}
    if len(rs) >= 4:
        info['close_t_minus_3'] = rs[3][1][0]
        info['date_t_minus_3'] = rs[3][0]
    hist = [r[1] for r in rs[:5]]
    if len(hist) >= 2:
        info['avg_vol_5d'] = sum(h[1] for h in hist) / len(hist)
        info['avg_amt_5d'] = sum(h[2] for h in hist) / len(hist)
    local[code] = info

# ---- 解析 CLI 快照 ----
cli = json.loads(RAW_CLI_JSON)
snap = {}
for it in cli['data']['item']:
    snap[it['thscode']] = it

# ---- 派生指标 ----
derived = {}
for code in CODES:
    s = snap.get(code)
    d = {'last': s['last_price'], 'prev': s['prev_price'], 'pct_chg': s['price_change_ratio_pct'],
         'volume': s['volume'], 'turnover': s['turnover']}
    li = local[code]
    d['vol_ratio_vol'] = round(s['volume'] / li['avg_vol_5d'], 3)
    d['vol_ratio_amt'] = round(s['turnover'] / li['avg_amt_5d'], 3)
    d['vol_ratio'] = max(d['vol_ratio_vol'], d['vol_ratio_amt'])
    d['vol_ratio_method'] = 'proxy_fullday_avg（本地bars.db无同时点历史，按当日累计/近5日全日均量、均额两口径；单位已归一为股/元，两口径应接近，取大者保守）'
    d['dev_3d'] = round(s['last_price'] / li['close_t_minus_3'] - 1, 4)
    d['dev_3d_base'] = {'date': li['date_t_minus_3'], 'close': li['close_t_minus_3']}
    derived[code] = d

# ---- 沪深300 实时指数（双源降级：hithink index snapshot 主源 + 新浪降级源）----
def fetch_hs300_index():
    """沪深300 实时指数。

    关键坑：hithink `market snapshot` 不含指数行（FUYAO_3001 Target not found，非瞬时错误），
    故指数必须走 `index snapshot` 子命令。两层都失败时返回 ok=False，规则不触发（不编造信号）。
    返回: {ok, source, last_price, prev_price, pct_chg} 或 {ok:False, reason}
    """
    # 主源：hithink index snapshot（npm 全局 bin 为 .cmd shim，须 shell=True 才能解析）
    try:
        out = subprocess.run(
            'hithink-finance index snapshot --thscodes 000300.SH',
            capture_output=True, text=True, shell=True, timeout=15)
        if out.returncode == 0:
            data = json.loads(out.stdout)
            items = data.get('data', {}).get('item', [])
            if items:
                it = items[0]
                return {'ok': True, 'source': 'hithink-index-snapshot',
                        'last_price': float(it['last_price']),
                        'prev_price': float(it.get('prev_price') or 0),
                        'pct_chg': float(it.get('price_change_ratio_pct') or 0)}
    except Exception:
        pass
    # 降级源：新浪指数行情（GBK 编码）
    try:
        req = urllib.request.Request(
            'https://hq.sinajs.cn/list=sh000300',
            headers={'Referer': 'https://finance.sina.com.cn'})
        with urllib.request.urlopen(req, timeout=10) as r:
            txt = r.read().decode('gbk', errors='ignore')
        m = txt.split('"')[1].split(',')
        # sina 指数格式：名称,今开,昨收,现价,最高,最低,...（无独立涨跌幅字段，需自算）
        if len(m) >= 4:
            last = float(m[3]); prev = float(m[2])
            pct = (last - prev) / prev * 100 if prev else 0.0
            return {'ok': True, 'source': 'sina', 'last_price': last,
                    'prev_price': prev, 'pct_chg': pct}
    except Exception:
        pass
    return {'ok': False, 'source': None, 'last_price': None,
            'prev_price': None, 'pct_chg': None, 'reason': 'both sources failed'}


hs300 = fetch_hs300_index()

# ---- 规则引擎 ----
taiji = derived['600667.SH']
cands = {c: derived[c] for c in CAND_CODES}

r1_price = taiji['last'] >= 14.70
r1_vol = taiji['vol_ratio'] < 1.5
r2_price = taiji['last'] <= 14.34
r2_pct = taiji['pct_chg'] <= -5.0

triggered_cands = [c for c in CAND_CODES if abs(cands[c]['pct_chg']) > 3.0]
dev18_cands = [c for c in CAND_CODES if cands[c]['dev_3d'] >= 0.18]

def code_short(c):
    return c.split('.')[0]

rules = {
    '600667.R1_loss_cut': {
        'target': '600667.SH',
        'triggered': bool(r1_price and r1_vol),
        'reason': ('现价%.2f≥14.70成立，量比%.2f<1.5成立 → 减损提示' % (taiji['last'], taiji['vol_ratio'])) if (r1_price and r1_vol)
                  else ('现价%.2f≥14.70成立，但量比%.2f≥1.5（放量），减损条件不成立 → 转R3只记录' % (taiji['last'], taiji['vol_ratio'])) if r1_price
                  else '现价%.2f<14.70' % taiji['last'],
    },
    '600667.R2_stop_loss': {
        'target': '600667.SH',
        'triggered': bool(r2_price or r2_pct),
        'reason': ('现价%.2f≤14.34 → 止损提示' % taiji['last']) if r2_price else (('当日跌幅%.2f%%≤-5%% → 止损提示' % taiji['pct_chg']) if r2_pct else '现价%.2f>14.34且当日%+.2f%%>-5%%，未触发' % (taiji['last'], taiji['pct_chg'])),
    },
    '600667.R3_volume_surge': {
        'target': '600667.SH',
        'triggered': False, 'record_only': True,
        'reason': ('现价%.2f≥14.70且量比%.2f≥1.5 → 放量冲高，只记录不通知（14.70限价单已可自动成交）' % (taiji['last'], taiji['vol_ratio'])) if (r1_price and not r1_vol) else '未同时满足（只记录）',
    },
    '600667.dev3d_m18': {
        'target': '600667.SH',
        'triggered': bool(taiji['dev_3d'] <= -0.18),
        'reason': '3日偏离%+.2f%%（现价%.2f / %s收盘%.2f - 1），%s' % (taiji['dev_3d'] * 100, taiji['last'], taiji['dev_3d_base']['date'], taiji['dev_3d_base']['close'], '≤-18%% 已触发' if taiji['dev_3d'] <= -0.18 else '未达-18%预警线'),
    },
    'candidates_abs_chg_gt3': {
        'target': 'PER_CANDIDATE',
        'triggered': bool(triggered_cands),
        'detail': {c: {'pct_chg': cands[c]['pct_chg'], 'last': cands[c]['last'], 'prev': cands[c]['prev']} for c in triggered_cands},
        'reason': ('触发标的: ' + ', '.join('%s(%s) %+.2f%%' % (code_short(c), c, cands[c]['pct_chg']) for c in triggered_cands)) if triggered_cands else '四强当日涨跌幅绝对值最大%.2f%%，均未>3%%' % max(abs(cands[c]['pct_chg']) for c in CAND_CODES),
    },
    'candidates_dev3d_p18': {
        'target': 'PER_CANDIDATE',
        'triggered': bool(dev18_cands),
        'reason': ('四强3日偏离≥+18%%: ' + ', '.join(dev18_cands)) if dev18_cands else '四强3日偏离最高%+.2f%%（%s），均未≥+18%%' % (max(cands[c]['dev_3d'] for c in CAND_CODES) * 100, max(CAND_CODES, key=lambda c: cands[c]['dev_3d'])),
    },
    'index_000300_drop_gt1_5': {
        'target': '000300.SH',
        'triggered': bool(hs300['ok'] and hs300['pct_chg'] <= -1.5),
        'reason': ('沪深300 日内 %+.2f%% ≤ -1.5%% → 大盘弱势，并入通知（源:%s）' % (hs300['pct_chg'], hs300['source'])) if hs300['ok']
                  else '沪深300 双源均不可用，跳过（不编造信号）',
    },
}

# ---- 防骚扰冷却：同标的同规则当日只发一次 ----
def is_per_candidate(rname, rval):
    """候选四强类规则（按标的去重）：新格式 target=PER_CANDIDATE，旧格式以 detail 字典识别"""
    return rval.get('target') == 'PER_CANDIDATE' or isinstance(rval.get('detail'), dict)

prior_keys = set()   # 'rule'（单标规则）或 'rule|target'（候选类规则）
if os.path.exists(LOG):
    with open(LOG, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except Exception:
                continue
            for rname, rval in (row.get('rules') or {}).items():
                if rval.get('triggered'):
                    if is_per_candidate(rname, rval):
                        for tc in (rval.get('detail') or {}):
                            prior_keys.add('%s|%s' % (rname, tc))
                    else:
                        prior_keys.add(rname)
            # notification_sent 行（含旧格式）：triggers + trigger_basis 的标的键
            trigs = row.get('triggers') or []
            basis = row.get('trigger_basis') or {}
            if trigs and basis and isinstance(basis, dict):
                for t in trigs:
                    for tc in basis:
                        prior_keys.add('%s|%s' % (t, tc))
            for t in trigs:
                if t not in [k.split('|')[0] for k in prior_keys]:
                    prior_keys.add(t)

def rule_new(rname, rval):
    """该规则本次是否有新标的需要通知（返回新目标列表；非PER_CANDIDATE规则返回 [None] 或 []）"""
    if not rval.get('triggered'):
        return []
    if is_per_candidate(rname, rval):
        return [tc for tc in (rval.get('detail') or {}) if '%s|%s' % (rname, tc) not in prior_keys]
    return [None] if rname not in prior_keys else []

new_triggers = []      # [(rule, target_or_None), ...]
suppressed = []        # 被冷却压制的 (rule, target)
for rname, rval in rules.items():
    for t in rule_new(rname, rval):
        new_triggers.append((rname, t))
    if rval.get('triggered'):
        if is_per_candidate(rname, rval):
            for tc in (rval.get('detail') or {}):
                if '%s|%s' % (rname, tc) in prior_keys:
                    suppressed.append((rname, tc))
        elif rname in prior_keys:
            suppressed.append((rname, None))

row = {
    'ts': NOW.isoformat(timespec='seconds'),
    'session': 'continuous_session',
    'source': 'cli',
    'request_id': cli['meta']['request_id'],
    'snapshot': [{'thscode': c, 'last': snap[c]['last_price'], 'prev': snap[c]['prev_price'],
                  'pct_chg': snap[c]['price_change_ratio_pct'], 'volume': snap[c]['volume'],
                  'turnover': snap[c]['turnover']} for c in CODES],
    'index_000300': ({'available': True, 'source': hs300['source'], 'last_price': hs300['last_price'],
                      'prev_price': hs300['prev_price'], 'pct_chg': hs300['pct_chg']}
                     if hs300['ok'] else {'available': False, 'reason': hs300.get('reason', 'unknown')}),
    'derived': {c: {k: v for k, v in d.items()} for c, d in derived.items()},
    'rules': rules,
    'notification': {
        'pending': bool(new_triggers),
        'new_triggers': [{'rule': r, 'target': t} for r, t in new_triggers],
        'suppressed_by_cooldown': [{'rule': r, 'target': t} for r, t in suppressed],
    },
}

os.makedirs('logs', exist_ok=True)
with open(LOG, 'a', encoding='utf-8') as f:
    f.write(json.dumps(row, ensure_ascii=False) + '\n')

print(json.dumps({
    'ts': row['ts'], 'log': LOG,
    'derived': {c: {'last': d['last'], 'pct_chg': d['pct_chg'], 'vol_ratio_vol': d['vol_ratio_vol'],
                    'vol_ratio_amt': d['vol_ratio_amt'], 'vol_ratio': d['vol_ratio'],
                    'dev_3d': d['dev_3d'], 'dev_3d_base': d['dev_3d_base']} for c, d in derived.items()},
    'prior_keys_today': sorted(prior_keys),
    'new_triggers': new_triggers,
    'suppressed_by_cooldown': suppressed,
    'rules': rules,
}, ensure_ascii=False, indent=1))
