#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json, subprocess, re, os, signal, time, html, random, logging, argparse
from datetime import datetime
from pathlib import Path

# ---- 配置 ----
TARGET_UID = ""
OUTPUT_DIR = "weibo_data"
COOKIE = ''

SKIP_COMMENTS = False
SKIP_LONGTEXT = False

PAGE_SIZE = 20
CMT_PAGE_SIZE = 20
MAX_CMT_PAGES = 30
DELAY = (3, 6)
CMT_DELAY = (2, 4)

# ---- logging ----
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("weibo")

TAG_RE = re.compile(r"<[^>]+>")
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"

# ---- graceful shutdown ----
_stop = False
def _on_signal(sig, _frame):
    global _stop
    log.warning("收到 %d，跑完当前这条就退", sig)
    _stop = True
signal.signal(signal.SIGINT, _on_signal)
signal.signal(signal.SIGTERM, _on_signal)


def strip_tags(t):
    if not t:
        return ""
    return html.unescape(TAG_RE.sub("", str(t))).strip()


def sleep_rand(lo, hi):
    time.sleep(random.uniform(lo, hi))


def fmt_size(n):
    return f"{n/1024/1024:.1f}MB" if n > 1048576 else f"{n/1024:.0f}KB"


def fetch(url, cookie, timeout=30):
    ref = "https://weibo.com/"
    uid_m = re.search(r"uid=(\d+)", url)
    if uid_m:
        ref = f"https://weibo.com/{uid_m.group(1)}"
    id_m = re.search(r"[?&]id=(\d+)", url)
    if id_m and uid_m:
        ref = f"https://weibo.com/{uid_m.group(1)}/weibo?mid={id_m.group(1)}"

    cmd = [
        "curl", "-s", "-m", str(timeout),
        "-w", "\n__HTTP_CODE__%{http_code}",
        url,
        "-H", f"User-Agent: {UA}",
        "-H", f"Cookie: {cookie}",
        "-H", f"Referer: {ref}",
        "-H", "Accept: application/json, text/plain, */*",
        "-H", "X-Requested-With: XMLHttpRequest",
    ]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True,
                           encoding="utf-8", errors="replace",
                           timeout=timeout + 5)
        out = r.stdout
        if "__HTTP_CODE__" in out:
            body, code_s = out.rsplit("__HTTP_CODE__", 1)
            code = int(code_s.strip())
        else:
            body, code = out, 0
        return (json.loads(body), code) if code == 200 else (None, code)
    except Exception as e:
        log.error("curl炸了: %s", e)
        return None, 0


# ---- 断点：从已有 JSONL 提取已抓 ID ----
def load_done(path):
    done = set()
    if not path.exists():
        return done
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                if "id" in obj:
                    done.add(obj["id"])
            except json.JSONDecodeError:
                pass
    log.info("已有 %d 条，跳过", len(done))
    return done


# ---- 微博分页 ----
def get_page(uid, cookie, page):
    url = f"https://weibo.com/ajax/statuses/mymblog?uid={uid}&count={PAGE_SIZE}&page={page}"
    sleep_rand(*DELAY)
    data, code = fetch(url, cookie)
    if code == 403:
        log.error("403了，cookie多半过期")
        return None, True
    if code != 200:
        log.warning("HTTP %d，等会儿重试", code)
        sleep_rand(10, 15)
        data, code = fetch(url, cookie)
        if code != 200:
            log.error("重试还是 %d，溜了", code)
            return None, True
    return data.get("data", {}).get("list", []), False


# ---- 长微博 ----
def get_long(wid, cookie):
    data, code = fetch(f"https://weibo.com/ajax/statuses/longtext?id={wid}", cookie)
    if code == 200 and data:
        return strip_tags(data.get("data", {}).get("longTextContent", ""))
    return None


# ---- 评论含二级----
def get_comments(uid, wid, cookie):
    results = []
    max_id = 0
    for pg in range(MAX_CMT_PAGES):
        if _stop:
            break
        url = (f"https://weibo.com/ajax/statuses/buildComments"
               f"?is_show_bulletin=2&is_mix=0&id={wid}"
               f"&is_show_cmt_num=0&comment_type=0"
               f"&count={CMT_PAGE_SIZE}&uid={uid}")
        if max_id:
            url += f"&max_id={max_id}"
        data, code = fetch(url, cookie)
        if code != 200:
            break
        cmts = (data or {}).get("data", [])
        if not cmts:
            break
        for c in cmts:
            item = {
                "cid": str(c.get("id", "")),
                "user": c.get("user", {}).get("screen_name", ""),
                "text": strip_tags(c.get("text_raw") or c.get("text", "")),
                "time": c.get("created_at", ""),
                "likes": c.get("like_counts", 0),
                "replies": [],
            }
            for rc in c.get("comments", []):
                item["replies"].append({
                    "cid": str(rc.get("id", "")),
                    "user": rc.get("user", {}).get("screen_name", ""),
                    "text": strip_tags(rc.get("text_raw") or rc.get("text", "")),
                    "time": rc.get("created_at", ""),
                    "likes": rc.get("like_count", 0),
                })
            results.append(item)
        has_more = data.get("has_more", False)
        new_max = data.get("max_id", 0)
        if not has_more or new_max == 0 or new_max == max_id:
            break
        max_id = new_max
        sleep_rand(*CMT_DELAY)
    return results


# ---- 1 ----
def dump_jsonl(path, item):
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(item, ensure_ascii=False) + "\n")
        f.flush()
        os.fsync(f.fileno())


def dump_txt(path, idx, w):
    with open(path, "a", encoding="utf-8") as f:
        f.write(f"--- #{idx} [{w['id']}] ---\n")
        f.write(f"时间: {w['created_at']}  来源: {w['source']}\n")
        f.write(f"赞:{w['attitudes_count']} 评:{w['comments_count']} 转:{w['reposts_count']}\n")
        f.write(w.get("full_text") or w["text_raw"] + "\n")
        rt = w.get("retweeted_status")
        if rt:
            f.write(f"转发 @{rt['user']}: {rt['text_raw']}\n")
        for c in w.get("comments", []):
            f.write(f"  [{c['user']}]: {c['text']}\n")
            for r in c.get("replies", []):
                f.write(f"    ↳ [{r['user']}]: {r['text']}\n")
        f.write("\n" + "-" * 60 + "\n\n")
        f.flush()
        os.fsync(f.fileno())


# ---- main ----
def main():
    cookie = COOKIE
    out = Path(OUTPUT_DIR)
    out.mkdir(parents=True, exist_ok=True)

    jsonl_p = out / f"weibo_{TARGET_UID}.jsonl"
    txt_p = out / f"weibo_{TARGET_UID}.txt"
    meta_p = out / f"weibo_{TARGET_UID}_meta.json"

    done = load_done(jsonl_p)

    # 拉用户信息
    log.info("拉用户信息...")
    user = {}
    data, code = fetch(f"https://weibo.com/ajax/profile/info?uid={TARGET_UID}", cookie)
    if code == 200 and data:
        user = data.get("data", {}).get("user", {})
        log.info("%s (uid=%s) 粉丝:%s 微博:%s",
                 user.get("screen_name", "?"), TARGET_UID,
                 user.get("followers_count", "?"), user.get("statuses_count", "?"))
    else:
        log.warning("用户信息拉不到，接着抓微博")

    # txt 头
    if not txt_p.exists():
        with open(txt_p, "w", encoding="utf-8") as f:
            f.write(f"用户: {user.get('screen_name','?')} (UID: {TARGET_UID})\n")
            f.write(f"开始: {datetime.now():%Y-%m-%d %H:%M:%S}\n")
            f.write("=" * 60 + "\n\n")

    # 开始
    log.info("开始抓微博...")
    page = 0
    new = 0
    skipped = 0
    total_cmt = 0
    empty_run = 0

    while not _stop:
        page += 1
        log.info("第 %d 页 (新:%d 跳过:%d)", page, new, skipped)

        wlist, fatal = get_page(TARGET_UID, cookie, page)
        if fatal:
            break
        if not wlist:
            empty_run += 1
            if empty_run >= 2:
                log.info("连续空页，没东西了")
                break
            continue
        empty_run = 0

        for w in wlist:
            if _stop:
                break
            wid = str(w.get("id", ""))
            if not wid:
                continue
            if wid in done:
                skipped += 1
                continue

            try:
                item = {
                    "id": wid,
                    "text_raw": strip_tags(w.get("text_raw") or w.get("text", "")),
                    "created_at": w.get("created_at", ""),
                    "source": strip_tags(w.get("source", "")),
                    "reposts_count": w.get("reposts_count", 0),
                    "comments_count": w.get("comments_count", 0),
                    "attitudes_count": w.get("attitudes_count", 0),
                    "is_long": w.get("isLongText", False),
                    "pic_num": w.get("pic_num", 0),
                    "pic_ids": w.get("pic_ids", []),
                    "retweeted_status": None,
                    "comments": [],
                }
                rt = w.get("retweeted_status")
                if rt:
                    item["retweeted_status"] = {
                        "id": str(rt.get("id", "")),
                        "text_raw": strip_tags(rt.get("text_raw") or rt.get("text", "")),
                        "user": rt.get("user", {}).get("screen_name", ""),
                    }

                # 长微博展开
                if item["is_long"] and not SKIP_LONGTEXT:
                    full = get_long(wid, cookie)
                    if full and len(full) > len(item["text_raw"]):
                        item["full_text"] = full
                        log.info("  长微博 %s: %d→%d字", wid, len(item["text_raw"]), len(full))
                    sleep_rand(1, 2)

                # 评论
                if item["comments_count"] > 0 and not SKIP_COMMENTS:
                    cmts = get_comments(TARGET_UID, wid, cookie)
                    item["comments"] = cmts
                    total_cmt += len(cmts)
                    log.info("  %s: %d条评论", wid, len(cmts))

                # 写
                dump_jsonl(jsonl_p, item)
                dump_txt(txt_p, new + 1, item)
                done.add(wid)
                new += 1

                if new % 10 == 0:
                    log.info("  已落盘 %d 条", new)

            except Exception as e:
                log.error("微博 %s 出错了: %s", wid, e, exc_info=True)

    # 收尾
    meta = {
        "uid": TARGET_UID,
        "screen_name": user.get("screen_name", ""),
        "followers": user.get("followers_count", 0),
        "statuses": user.get("statuses_count", 0),
        "desc": user.get("description", ""),
        "verified": user.get("verified_reason", ""),
        "end_time": f"{datetime.now():%Y-%m-%d %H:%M:%S}",
        "total": new + skipped,
        "new": new,
        "skipped": skipped,
        "comments": total_cmt,
        "interrupted": _stop,
    }
    meta_p.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    log.info("=" * 50)
    if _stop:
        log.info(" 被中断了，已存 %d 条(新:%d 跳过:%d)，再跑一次就能续", new+skipped, new, skipped)
    else:
        log.info(" 搞定！微博 %d 条(新:%d 跳过:%d) 评论 %d 条", new+skipped, new, skipped, total_cmt)
    log.info("  JSONL: %s (%s)", jsonl_p, fmt_size(jsonl_p.stat().st_size) if jsonl_p.exists() else "?")
    log.info("  TXT:   %s (%s)", txt_p, fmt_size(txt_p.stat().st_size) if txt_p.exists() else "?")
    log.info("=" * 50)


if __name__ == "__main__":
    main()
