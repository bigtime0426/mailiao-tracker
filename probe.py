"""麥寮轉型追蹤：資料來源連線探測（從 GitHub 的機器跑一次，看哪些來源連得到）

只做「連線測試」：每個來源抓一次，記錄連不連得到、回應多快、內容長什麼樣。
不需要任何金鑰，也不會寫入任何金鑰。結果存成 probe_report.md（人看）與 probe_report.json（程式用）。
任何一個來源失敗都只記錄，不會讓整支程式中斷。
"""
import concurrent.futures as cf
import datetime as dt
import json
import os
import time
import urllib.parse

import requests

TZ = dt.timezone(dt.timedelta(hours=8))
UA = {"User-Agent": "Mozilla/5.0 (compatible; mailiao-probe)"}
CONNECT = float(os.environ.get("PROBE_CONNECT", "10"))      # 連線等待秒數
READ = float(os.environ.get("PROBE_READ", "20"))            # 讀取等待秒數
RETRY_CONNECT = float(os.environ.get("PROBE_RETRY_CONNECT", "15"))
RETRY_READ = float(os.environ.get("PROBE_RETRY_READ", "40"))
BODY_CAP = 3_000_000                                       # 最多讀這麼多位元組，避免抓太大的檔
OUT_MD = "probe_report.md"
OUT_JSON = "probe_report.json"


def last_weekday(now):
    """昨天往回找最近的平日（證交所當天資料收盤後才有，保險起見用前一個交易日）。"""
    d = (now - dt.timedelta(days=1)).date()
    while d.weekday() >= 5:
        d -= dt.timedelta(days=1)
    return d


def default_targets(now):
    d = last_weekday(now)
    ymd = d.strftime("%Y%m%d")
    ym01 = d.strftime("%Y%m") + "01"
    news_q = urllib.parse.quote("麥寮 六接 台塑")
    return [
        # conf：已知格式＝這個網址格式我有把握；猜測＝我不確定網址是否正確，連不上不代表網站有問題
        {"id": "twse_day", "group": "股價與籌碼", "name": "證交所 6505 個股日成交（本月）", "kind": "json",
         "url": "https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY?date=%s&stockNo=6505&response=json" % ym01,
         "must": '"stat"', "conf": "已知格式", "purpose": "台塑化股價與成交量"},
        {"id": "twse_t86", "group": "股價與籌碼", "name": "證交所 三大法人買賣超（%s）" % ymd, "kind": "json",
         "url": "https://www.twse.com.tw/rwd/zh/fund/T86?date=%s&selectType=ALL&response=json" % ymd,
         "must": '"stat"', "conf": "已知格式", "purpose": "法人買賣超，對照事件時間軸"},
        {"id": "openapi_all", "group": "股價與籌碼", "name": "證交所 OpenAPI 全部上市股票收盤", "kind": "json",
         "url": "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL",
         "must": "6505", "conf": "已知格式", "purpose": "每日收盤快照，一次抓全部"},
        {"id": "openapi_rev", "group": "股價與籌碼", "name": "證交所 OpenAPI 上市公司月營收", "kind": "json",
         "url": "https://openapi.twse.com.tw/v1/opendata/t187ap05_L",
         "must": "6505", "conf": "已知格式", "purpose": "台塑化月營收（可驗證）"},
        {"id": "openapi_news", "group": "公司公告", "name": "證交所 OpenAPI 上市公司每日重大訊息", "kind": "json",
         "url": "https://openapi.twse.com.tw/v1/opendata/t187ap04_L",
         "must": "", "conf": "已知格式", "purpose": "重大訊息，篩出台塑化與相關公司"},
        {"id": "mops", "group": "公司公告", "name": "公開資訊觀測站 首頁", "kind": "html",
         "url": "https://mops.twse.com.tw/mops/", "must": "", "conf": "猜測",
         "purpose": "法說會、財報原文；新版網站多為動態頁，只能先測首頁是否連得到"},
        {"id": "fpcc", "group": "公司公告", "name": "台塑化官網", "kind": "html",
         "url": "https://www.fpcc.com.tw/", "must": "", "conf": "猜測", "purpose": "公司新聞與簡報"},
        {"id": "eia1", "group": "政府公告", "name": "環境部 環評書件查詢系統", "kind": "html",
         "url": "https://eiadoc.moenv.gov.tw/", "must": "", "conf": "猜測", "purpose": "環評審查、監測報告"},
        {"id": "eia2", "group": "政府公告", "name": "環境部 環評資訊（另一個網域）", "kind": "html",
         "url": "https://eia.moenv.gov.tw/", "must": "", "conf": "猜測", "purpose": "同上，兩個網域各測一次"},
        {"id": "moeaea", "group": "政府公告", "name": "經濟部能源署", "kind": "html",
         "url": "https://www.moeaea.gov.tw/", "must": "", "conf": "猜測", "purpose": "電業許可、接收站相關公告"},
        {"id": "taipower", "group": "政府公告", "name": "台電", "kind": "html",
         "url": "https://www.taipower.com.tw/", "must": "", "conf": "猜測", "purpose": "購電合約、機組調度"},
        {"id": "gnews", "group": "新聞", "name": "Google 新聞 RSS（麥寮 六接 台塑）", "kind": "rss",
         "url": "https://news.google.com/rss/search?q=%s&hl=zh-TW&gl=TW&ceid=TW:zh-Hant" % news_q,
         "must": "<item>", "conf": "已知格式", "purpose": "新聞時間軸（只取標題與連結，不轉載全文）"},
    ]


def get(url, connect, read):
    t0 = time.time()
    r = requests.get(url, headers=UA, timeout=(connect, read), stream=True)
    body = b""
    for chunk in r.iter_content(65536):
        body += chunk
        if len(body) >= BODY_CAP:
            break
    r.close()
    return r.status_code, r.headers.get("content-type", ""), body, round(time.time() - t0, 1)


def shape(text):
    """JSON 的外形（讓我之後寫解析程式時知道欄位）。不是 JSON 回 (None, '')。"""
    try:
        obj = json.loads(text)
    except ValueError:
        return None, ""
    if isinstance(obj, list):
        keys = list(obj[0].keys())[:6] if obj and isinstance(obj[0], dict) else []
        return obj, "list %d 筆" % len(obj) + ("；欄位：" + "、".join(keys) if keys else "")
    if isinstance(obj, dict):
        extra = "；stat=%s" % str(obj["stat"])[:30] if "stat" in obj else ""
        return obj, "dict；欄位：%s%s" % ("、".join(list(obj.keys())[:6]), extra)
    return obj, type(obj).__name__


def judge(t, status, ctype, text):
    """回傳 (結果, 說明, 內容形狀)。結果：通／連到但內容不符。"""
    if status != 200:
        return "不通", "HTTP %s" % status, ""
    desc = ""
    if t["kind"] == "json":
        obj, desc = shape(text)
        if obj is None:
            return "連到但內容不符", "回應不是 JSON（可能被擋或改版）", ""
        if isinstance(obj, list) and not obj:
            return "連到但內容不符", "回傳空清單", desc
    if t.get("must") and t["must"] not in text:
        return "連到但內容不符", "內容裡找不到 %s" % t["must"], desc
    if not desc:
        desc = "內容 %d 字元" % len(text)
    return "通", "", desc


def sample_of(text):
    s = " ".join(text.split())[:90]
    return s.replace("|", "/")


def err_name(e):
    if isinstance(e, requests.exceptions.ConnectTimeout):
        return "連線逾時"
    if isinstance(e, requests.exceptions.ReadTimeout):
        return "讀取逾時"
    if isinstance(e, requests.exceptions.SSLError):
        return "SSL 錯誤"
    if isinstance(e, requests.exceptions.ConnectionError):
        return "連線被拒或中斷"
    return type(e).__name__


def probe_one(t):
    res = {k: t.get(k, "") for k in ("id", "group", "name", "url", "conf", "purpose")}
    res.update({"verdict": "不通", "detail": "", "status": None, "seconds": None, "ctype": "", "bytes": 0,
                "shape": "", "sample": "", "variant": ""})
    last_err, attempts = None, [(CONNECT, READ), (RETRY_CONNECT, RETRY_READ)]
    for i, (c, r) in enumerate(attempts):
        try:
            status, ctype, body, secs = get(t["url"], c, r)
        except requests.RequestException as e:
            last_err = e
            continue
        res.update({"status": status, "seconds": secs, "ctype": ctype[:40], "bytes": len(body)})
        text = body.decode("utf-8", "replace")
        verdict, detail, shp = judge(t, status, ctype, text)
        res.update({"verdict": verdict, "detail": detail, "shape": shp, "sample": sample_of(text)})
        if status in (429, 500, 502, 503, 504) and i == 0:     # 暫時性錯誤再試一次
            time.sleep(3)
            continue
        last_err = None
        break
    else:
        pass
    if last_err is not None and res["status"] is None:
        res["detail"] = err_name(last_err)
        if t["url"].startswith("https://"):                    # 整個連不上時，換 http 再測（判斷是被擋還是只是慢）
            try:
                status, _ct, _b, secs = get(t["url"].replace("https://", "http://", 1), CONNECT, READ)
                res["variant"] = "改用 http：HTTP %s（%s 秒）" % (status, secs)
            except requests.RequestException as e2:
                res["variant"] = "改用 http：%s" % err_name(e2)
    return res


def esc(s):
    return str(s).replace("|", "/").replace("\n", " ")


def report(results, now):
    ok = [r for r in results if r["verdict"] == "通"]
    odd = [r for r in results if r["verdict"] == "連到但內容不符"]
    bad = [r for r in results if r["verdict"] == "不通"]
    lines = ["# 麥寮轉型追蹤：資料來源連線探測", "",
             "執行時間：%s（台北時間）" % now.strftime("%Y-%m-%d %H:%M"), "",
             "結果：通 %d、連到但內容不符 %d、不通 %d（共 %d 個來源）" % (len(ok), len(odd), len(bad), len(results)), "",
             "「把握度」欄：已知格式＝這個網址格式我有把握；猜測＝網址是我推測的，連不上不一定是網站擋你，可能是網址不對。", ""]
    groups = []
    for r in results:
        if r["group"] not in groups:
            groups.append(r["group"])
    for g in groups:
        lines += ["## " + g, "", "| 結果 | 來源 | 狀態 | 秒 | 內容形狀／原因 | 把握度 |", "| --- | --- | --- | --- | --- | --- |"]
        for r in [x for x in results if x["group"] == g]:
            why = r["shape"] if r["verdict"] == "通" else (r["detail"] + ("；" + r["shape"] if r["shape"] else ""))
            if r["variant"]:
                why += "；" + r["variant"]
            lines.append("| %s | %s | %s | %s | %s | %s |" % (
                r["verdict"], esc(r["name"]), r["status"] if r["status"] is not None else "-",
                r["seconds"] if r["seconds"] is not None else "-", esc(why), r["conf"]))
        lines.append("")
    lines += ["## 內容樣本（連得到的來源，前 90 字）", ""]
    for r in ok + odd:
        lines.append("- %s：`%s`" % (esc(r["name"]), esc(r["sample"])))
    if not (ok or odd):
        lines.append("（沒有連得到的來源）")
    lines += ["", "## 說明", "",
              "- 這份報告只代表「這一次、從這台機器」的結果；政府網站偶爾會暫時連不上，建議在不同時間再跑一次確認。",
              "- 「連到但內容不符」＝網站有回應，但不是預期的格式（可能被擋、改版，或當天是假日沒有資料）。",
              ""]
    return "\n".join(lines)


def main():
    now = dt.datetime.now(TZ)
    path = os.environ.get("PROBE_TARGETS_FILE")
    targets = json.load(open(path, encoding="utf-8")) if path else default_targets(now)
    print("探測 %d 個來源" % len(targets))
    with cf.ThreadPoolExecutor(max_workers=4) as ex:
        results = list(ex.map(probe_one, targets))       # map 會保持原本順序
    md = report(results, now)
    open(OUT_MD, "w", encoding="utf-8").write(md + "\n")
    json.dump({"updated": now.isoformat(), "results": results}, open(OUT_JSON, "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)
    print(md)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:  # noqa: BLE001
        print("探測程式發生錯誤：", str(e)[:200])
