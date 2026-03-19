import hashlib
import os
import re
import sqlite3
import threading
import time
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

import feedparser
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv(dotenv_path=".env", override=True)

CURATION_BOT_TOKEN = os.getenv("CURATION_BOT_TOKEN", "").strip()
PUBLISH_BOT_TOKEN = os.getenv("PUBLISH_BOT_TOKEN", "").strip()

PRIVATE_CHAT_ID = os.getenv("PRIVATE_CHAT_ID", "").strip()
PUBLIC_CHAT_ID = os.getenv("PUBLIC_CHAT_ID", "").strip()

RSS_FEEDS = [x.strip() for x in os.getenv("RSS_FEEDS", "").split(",") if x.strip()]
KEYWORDS = [x.strip().lower() for x in os.getenv("KEYWORDS", "").split(",") if x.strip()]

SCAN_INTERVAL_SECONDS = int(os.getenv("SCAN_INTERVAL_SECONDS", "30"))
PUBLIC_COOLDOWN_MINUTES = int(os.getenv("PUBLIC_COOLDOWN_MINUTES", "720"))
DB_PATH = os.getenv("DB_PATH", "techdrop.db")

UTC = timezone.utc
USER_AGENT = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0 Safari/537.36"
    )
}


def now_iso() -> str:
    return datetime.now(UTC).isoformat()


def normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip().lower())


def normalize_link(link: str) -> str:
    if not link:
        return ""
    parsed = urlparse(link.strip())
    return f"{parsed.netloc.lower().replace('www.', '')}{parsed.path.rstrip('/')}"


def make_product_id(title: str, link: str) -> str:
    base = f"{normalize_text(title)}|{normalize_link(link)}"
    return hashlib.md5(base.encode("utf-8")).hexdigest()[:12].upper()


def detect_platform(link: str) -> str:
    domain = urlparse(link).netloc.lower()
    if "mercadolivre" in domain or "mercadolibre" in domain:
        return "Mercado Livre"
    if "shopee" in domain:
        return "Shopee"
    if "amazon" in domain:
        return "Amazon"
    if "magalu" in domain:
        return "Magalu"
    if "pelando" in domain:
        return "Pelando"
    if "promobit" in domain:
        return "Promobit"
    return "Oferta"


def extract_title_from_link(link: str) -> str:
    parsed = urlparse(link)
    slug = parsed.path.strip("/").split("/")[-1]
    slug = slug.replace("-", " ").replace("_", " ")
    slug = re.sub(r"\s+", " ", slug).strip()
    return slug.title() if slug else "Oferta Manual"


def matches_keywords(title: str) -> bool:
    title = normalize_text(title)
    return any(k in title for k in KEYWORDS)


def clean_title(title: str) -> str:
    title = re.sub(r"\s+", " ", (title or "").strip())
    title = re.sub(r"\s*[\|\-–—]\s*(Shopee Brasil|Shopee|Amazon\.com\.br|Amazon|Mercado Livre|Magalu).*?$", "", title, flags=re.I)
    return title.strip()


def fetch_product_metadata(url: str) -> dict:
    """
    Tenta pegar título e imagem real da página usando meta tags Open Graph/Twitter.
    """
    try:
        response = requests.get(url, headers=USER_AGENT, timeout=20, allow_redirects=True)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")

        title = None
        image = None

        # Open Graph
        og_title = soup.find("meta", property="og:title")
        og_image = soup.find("meta", property="og:image")

        if og_title and og_title.get("content"):
            title = og_title["content"].strip()

        if og_image and og_image.get("content"):
            image = og_image["content"].strip()

        # Twitter Card fallback
        if not title:
            tw_title = soup.find("meta", attrs={"name": "twitter:title"})
            if tw_title and tw_title.get("content"):
                title = tw_title["content"].strip()

        if not image:
            tw_image = soup.find("meta", attrs={"name": "twitter:image"})
            if tw_image and tw_image.get("content"):
                image = tw_image["content"].strip()

        # HTML <title> fallback
        if not title and soup.title and soup.title.string:
            title = soup.title.string.strip()

        title = clean_title(title) if title else extract_title_from_link(url)

        return {
            "title": title or "Oferta",
            "image": image or None
        }

    except Exception as e:
        print("Erro ao buscar metadados:", e)
        return {
            "title": extract_title_from_link(url),
            "image": None
        }


DB = sqlite3.connect(DB_PATH, check_same_thread=False)
DB.row_factory = sqlite3.Row
DB_LOCK = threading.Lock()


def init_db():
    with DB_LOCK:
        cur = DB.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS products (
            product_id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            original_url TEXT,
            platform TEXT,
            affiliate_url TEXT,
            approved INTEGER DEFAULT 0,
            created_at TEXT,
            updated_at TEXT,
            last_seen_at TEXT,
            last_public_post_at TEXT
        )
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS seen_entries (
            entry_key TEXT PRIMARY KEY,
            seen_at TEXT
        )
        """)
        DB.commit()


def tg_request(token: str, method: str, payload: dict):
    url = f"https://api.telegram.org/bot{token}/{method}"
    r = requests.post(url, data=payload, timeout=30)
    r.raise_for_status()
    data = r.json()
    if not data.get("ok"):
        raise RuntimeError(f"Telegram erro em {method}: {data}")
    return data


def send_message(token: str, chat_id: str, text: str, preview: bool = False):
    tg_request(token, "sendMessage", {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": str(not preview).lower(),
    })


def send_photo(token: str, chat_id: str, photo_url: str, caption: str):
    tg_request(token, "sendPhoto", {
        "chat_id": chat_id,
        "photo": photo_url,
        "caption": caption,
        "parse_mode": "HTML",
    })


def get_updates(token: str, offset=None, timeout=25):
    payload = {"timeout": timeout}
    if offset is not None:
        payload["offset"] = offset
    return tg_request(token, "getUpdates", payload).get("result", [])


def upsert_product(product_id: str, title: str, original_url: str, platform: str):
    ts = now_iso()
    with DB_LOCK:
        cur = DB.cursor()
        cur.execute("""
        INSERT INTO products (
            product_id, title, original_url, platform,
            created_at, updated_at, last_seen_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(product_id) DO UPDATE SET
            title=excluded.title,
            original_url=excluded.original_url,
            platform=excluded.platform,
            updated_at=excluded.updated_at,
            last_seen_at=excluded.last_seen_at
        """, (product_id, title, original_url, platform, ts, ts, ts))
        DB.commit()


def get_product(product_id: str):
    with DB_LOCK:
        cur = DB.cursor()
        cur.execute("SELECT * FROM products WHERE product_id = ?", (product_id,))
        return cur.fetchone()


def save_affiliate(product_id: str, affiliate_url: str):
    with DB_LOCK:
        cur = DB.cursor()
        cur.execute("""
        UPDATE products
        SET affiliate_url = ?, approved = 1, updated_at = ?
        WHERE product_id = ?
        """, (affiliate_url.strip(), now_iso(), product_id))
        DB.commit()
        return cur.rowcount > 0


def reject_product(product_id: str):
    with DB_LOCK:
        cur = DB.cursor()
        cur.execute("""
        UPDATE products
        SET affiliate_url = NULL, approved = 0, updated_at = ?
        WHERE product_id = ?
        """, (now_iso(), product_id))
        DB.commit()
        return cur.rowcount > 0


def mark_seen(entry_key: str):
    with DB_LOCK:
        cur = DB.cursor()
        cur.execute("INSERT OR IGNORE INTO seen_entries (entry_key, seen_at) VALUES (?, ?)", (entry_key, now_iso()))
        DB.commit()


def was_seen(entry_key: str):
    with DB_LOCK:
        cur = DB.cursor()
        cur.execute("SELECT 1 FROM seen_entries WHERE entry_key = ?", (entry_key,))
        return cur.fetchone() is not None


def can_publish(product):
    if not product["affiliate_url"]:
        return False
    last = product["last_public_post_at"]
    if not last:
        return True
    last_dt = datetime.fromisoformat(last)
    return datetime.now(UTC) - last_dt >= timedelta(minutes=PUBLIC_COOLDOWN_MINUTES)


def mark_public_post(product_id: str):
    with DB_LOCK:
        cur = DB.cursor()
        cur.execute("""
        UPDATE products
        SET last_public_post_at = ?, updated_at = ?
        WHERE product_id = ?
        """, (now_iso(), now_iso(), product_id))
        DB.commit()


def list_pending(limit=10):
    with DB_LOCK:
        cur = DB.cursor()
        cur.execute("""
        SELECT * FROM products
        WHERE affiliate_url IS NULL
        ORDER BY last_seen_at DESC
        LIMIT ?
        """, (limit,))
        return cur.fetchall()


def build_private_message(product):
    return (
        f"🆔 <b>{product['product_id']}</b>\n"
        f"📦 <b>{product['title']}</b>\n"
        f"🏪 <b>{product['platform']}</b>\n"
        f"🔗 <b>Original:</b> {product['original_url']}\n\n"
        f"Comandos:\n"
        f"<code>/afiliado {product['product_id']} SEU_LINK_AQUI</code>\n"
        f"<code>/publicar {product['product_id']}</code>\n"
        f"<code>/rejeitar {product['product_id']}</code>"
    )


def build_public_message(product):
    title = product["title"]
    title_low = normalize_text(title)

    hook = "⚡ Achado tech do dia"
    if "ssd" in title_low:
        hook = "⚡ SSD com preço bom"
    elif "mouse" in title_low:
        hook = "🎮 Mouse gamer custo-benefício"
    elif "teclado" in title_low:
        hook = "⌨️ Teclado mecânico em destaque"
    elif "headset" in title_low or "fone" in title_low:
        hook = "🎧 Áudio bom gastando pouco"
    elif "monitor" in title_low:
        hook = "🖥️ Upgrade de setup"
    elif "smartwatch" in title_low:
        hook = "⌚ Smartwatch em destaque"
    elif "smartphone" in title_low or "celular" in title_low:
        hook = "📱 Celular custo-benefício"
    elif "memoria" in title_low or "ram" in title_low:
        hook = "💾 Upgrade de memória"

    return (
        f"🔥 <b>{product['title']}</b>\n"
        f"{hook}\n"
        f"🏪 <b>{product['platform']}</b>\n\n"
        f"🛒 <b>Pegar oferta:</b>\n{product['affiliate_url']}\n\n"
        f"⚫🔵 <i>Tech Drop | Ofertas</i>"
    )


def publish_product(product_id: str, manual=False):
    product = get_product(product_id)
    if not product:
        return False
    if not manual and not can_publish(product):
        return False
    if not product["affiliate_url"]:
        return False

    meta = fetch_product_metadata(product["affiliate_url"])

    payload = {
        "title": meta["title"] or product["title"],
        "platform": product["platform"],
        "affiliate_url": product["affiliate_url"]
    }

    caption = build_public_message(payload)

    if meta["image"]:
        send_photo(PUBLISH_BOT_TOKEN, PUBLIC_CHAT_ID, meta["image"], caption)
    else:
        send_message(PUBLISH_BOT_TOKEN, PUBLIC_CHAT_ID, caption, preview=True)

    mark_public_post(product_id)
    return True


def handle_private_command(text: str):
    text = (text or "").strip()
    if not text:
        return None

    if text.startswith("/start"):
        return (
            "✅ Bot de curadoria ligado.\n\n"
            "Comandos:\n"
            "<code>/pendentes</code>\n"
            "<code>/add LINK</code>\n"
            "<code>/afiliado ID LINK</code>\n"
            "<code>/publicar ID</code>\n"
            "<code>/publicarlink LINK</code>\n"
            "<code>/rejeitar ID</code>\n"
            "<code>/buscar</code>"
        )

    if text.startswith("/pendentes"):
        rows = list_pending()
        if not rows:
            return "✅ Não há produtos pendentes."
        return "📝 <b>Pendentes:</b>\n" + "\n".join(
            f"• <code>{r['product_id']}</code> — {r['title']}" for r in rows
        )

    if text.startswith("/add"):
        parts = text.split(maxsplit=1)
        if len(parts) < 2:
            return "Use: <code>/add LINK</code>"

        original_url = parts[1].strip()
        meta = fetch_product_metadata(original_url)
        title = meta["title"]
        platform = detect_platform(original_url)
        product_id = make_product_id(title, original_url)

        upsert_product(product_id, title, original_url, platform)
        product = get_product(product_id)

        return (
            f"✅ Oferta adicionada manualmente.\n\n"
            f"🆔 <code>{product_id}</code>\n"
            f"📦 {product['title']}\n"
            f"🏪 {product['platform']}\n\n"
            f"Agora envie:\n"
            f"<code>/afiliado {product_id} SEU_LINK_AQUI</code>\n"
            f"ou publique com:\n"
            f"<code>/publicar {product_id}</code>"
        )

    if text.startswith("/publicarlink"):
        parts = text.split(maxsplit=1)
        if len(parts) < 2:
            return "Use: <code>/publicarlink LINK</code>"

        affiliate_url = parts[1].strip()
        meta = fetch_product_metadata(affiliate_url)

        temp_product = {
            "title": meta["title"],
            "platform": detect_platform(affiliate_url),
            "affiliate_url": affiliate_url
        }

        caption = build_public_message(temp_product)

        if meta["image"]:
            send_photo(PUBLISH_BOT_TOKEN, PUBLIC_CHAT_ID, meta["image"], caption)
        else:
            send_message(PUBLISH_BOT_TOKEN, PUBLIC_CHAT_ID, caption, preview=True)

        return "🚀 Link publicado diretamente no grupo público."

    if text.startswith("/afiliado"):
        parts = text.split(maxsplit=2)
        if len(parts) < 3:
            return "Use: <code>/afiliado ID LINK</code>"
        product_id = parts[1].strip().upper()
        affiliate_url = parts[2].strip()
        ok = save_affiliate(product_id, affiliate_url)
        if not ok:
            return f"❌ Produto <code>{product_id}</code> não encontrado."
        return f"✅ Link afiliado salvo para <code>{product_id}</code>."

    if text.startswith("/publicar"):
        parts = text.split(maxsplit=1)
        if len(parts) < 2:
            return "Use: <code>/publicar ID</code>"
        product_id = parts[1].strip().upper()
        ok = publish_product(product_id, manual=True)
        return (
            f"🚀 Produto <code>{product_id}</code> publicado."
            if ok else
            f"❌ Não foi possível publicar <code>{product_id}</code>."
        )

    if text.startswith("/rejeitar"):
        parts = text.split(maxsplit=1)
        if len(parts) < 2:
            return "Use: <code>/rejeitar ID</code>"
        product_id = parts[1].strip().upper()
        ok = reject_product(product_id)
        return (
            f"🚫 Produto <code>{product_id}</code> rejeitado."
            if ok else
            f"❌ Produto <code>{product_id}</code> não encontrado."
        )

    if text.startswith("/buscar"):
        found = fetch_offers_once(send_private=True)
        return f"🔎 Busca concluída. {found} ofertas enviadas ao grupo privado."

    return None


def poll_private_bot():
    offset = None
    while True:
        try:
            updates = get_updates(CURATION_BOT_TOKEN, offset=offset, timeout=25)
            for upd in updates:
                offset = upd["update_id"] + 1
                msg = upd.get("message") or upd.get("edited_message")
                if not msg:
                    continue
                if str(msg["chat"]["id"]) != PRIVATE_CHAT_ID:
                    continue

                response = handle_private_command(msg.get("text", ""))
                if response:
                    send_message(CURATION_BOT_TOKEN, PRIVATE_CHAT_ID, response, preview=False)
        except Exception as e:
            print("Erro no bot privado:", e)
            time.sleep(5)


def fetch_offers_once(send_private: bool = True) -> int:
    sent_count = 0
    for feed_url in RSS_FEEDS:
        feed = feedparser.parse(feed_url)
        for entry in getattr(feed, "entries", [])[:40]:
            title = getattr(entry, "title", "") or ""
            link = getattr(entry, "link", "") or ""

            if not title or not link:
                continue
            if not matches_keywords(title):
                continue

            entry_key = hashlib.md5(f"{title}|{link}".encode("utf-8")).hexdigest()
            if was_seen(entry_key):
                continue

            product_id = make_product_id(title, link)
            platform = detect_platform(link)

            upsert_product(product_id, title, link, platform)
            product = get_product(product_id)

            if product["affiliate_url"]:
                if publish_product(product_id):
                    sent_count += 1
            else:
                if send_private:
                    send_message(CURATION_BOT_TOKEN, PRIVATE_CHAT_ID, build_private_message(product), preview=False)
                    sent_count += 1

            mark_seen(entry_key)
    return sent_count


def scan_feeds_loop():
    while True:
        try:
            fetch_offers_once(send_private=True)
        except Exception as e:
            print("Erro no scanner RSS:", e)

        time.sleep(SCAN_INTERVAL_SECONDS)


def validate_env():
    missing = []
    for name, value in {
        "CURATION_BOT_TOKEN": CURATION_BOT_TOKEN,
        "PUBLISH_BOT_TOKEN": PUBLISH_BOT_TOKEN,
        "PRIVATE_CHAT_ID": PRIVATE_CHAT_ID,
        "PUBLIC_CHAT_ID": PUBLIC_CHAT_ID,
    }.items():
        if not value:
            missing.append(name)

    if missing:
        raise SystemExit("Faltam variáveis no .env: " + ", ".join(missing))


def main():
    validate_env()
    init_db()

    print("✅ Sistema Tech Drop PRO iniciado.")
    print("✅ Bot 1: Curadoria privada")
    print("✅ Bot 2: Publicação pública com imagem")

    t1 = threading.Thread(target=poll_private_bot, daemon=True)
    t2 = threading.Thread(target=scan_feeds_loop, daemon=True)

    t1.start()
    t2.start()

    while True:
        time.sleep(60)


if __name__ == "__main__":
    main()