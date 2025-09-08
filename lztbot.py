import os, re, time, json, math, asyncio, requests, logging, hashlib, html as _html, random
from datetime import datetime
from typing import Dict, Any, Optional, List, Tuple
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, Router, F
from aiogram.client.default import DefaultBotProperties
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup
from aiogram.filters import CommandStart, Command
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.exceptions import TelegramBadRequest


load_dotenv()
logging.basicConfig(level=logging.INFO)

TG_BOT_TOKEN     = os.getenv("TG_BOT_TOKEN", "")
LZT_FORUM_TOKEN  = os.getenv("LZT_FORUM_TOKEN", "")
LZT_MARKET_TOKEN = os.getenv("LZT_MARKET_TOKEN", "")
ADMIN_USER_ID    = int(os.getenv("ADMIN_USER_ID", "0"))

if not TG_BOT_TOKEN or not LZT_FORUM_TOKEN or not LZT_MARKET_TOKEN or not ADMIN_USER_ID:
    raise RuntimeError("Env vars required: TG_BOT_TOKEN, LZT_FORUM_TOKEN, LZT_MARKET_TOKEN, ADMIN_USER_ID")

FORUM_BASE = "https://prod-api.lolz.live"
MARKET_BASE = "https://prod-api.lzt.market"
SITE_FORUM = "https://lolz.live"
SITE_MARKET = "https://lzt.market"
INVOICE_SUCCESS_URL = (os.getenv("INVOICE_SUCCESS_URL", "https://lolz.live/") or "").strip()
INVOICE_CALLBACK_URL = (os.getenv("INVOICE_CALLBACK_URL", "") or "").strip()

SAFE_CHARS = set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_.")
def _assert_ascii_token(name: str, value: str):
    bad = [c for c in (value or "") if c not in SAFE_CHARS]
    if bad:
        raise RuntimeError(f"{name} содержит недопустимые символы ({''.join(sorted(set(bad)))}) — проверь токен.")
_assert_ascii_token("LZT_FORUM_TOKEN", LZT_FORUM_TOKEN)
_assert_ascii_token("LZT_MARKET_TOKEN", LZT_MARKET_TOKEN)

def is_admin(tg_id: Optional[int]) -> bool:
    return ADMIN_USER_ID == 0 or int(tg_id or 0) == ADMIN_USER_ID

SETTINGS_FILE = "settings.json"
NOTES_FILE = "notes.json"
BUMPS_FILE = "bumps.json"

def _load(path, default):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default

def _save(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def get_settings() -> Dict[str,Any]:
    s = _load(SETTINGS_FILE, {})
    s.setdefault("push_cards_enabled", True)
    s.setdefault("last_notif_key", "")
    s.setdefault("notify_comments", True)
    s.setdefault("notify_mentions", True)
    s.setdefault("notify_likes", True)
    s.setdefault("notify_payment_in", True)
    s.setdefault("notify_hold_released", True)
    s.setdefault("notify_profile_post", True)
    s.setdefault("notify_profile_comment", True)
    _save(SETTINGS_FILE, s)
    return s

def set_setting(key: str, val: Any):
    s = get_settings(); s[key] = val; _save(SETTINGS_FILE, s)


_LAST_CALL = 0.0
def _rl():
    global _LAST_CALL
    now = time.time(); dt = now - _LAST_CALL
    if dt < 0.25:
        time.sleep(0.25 - dt)
    _LAST_CALL = time.time()

def api_req(method: str, url: str, token: str, *, params: Optional[Dict[str, Any]] = None, json_: Optional[Dict[str, Any]] = None, timeout: int = 25) -> Dict[str, Any]:
    _rl()
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    try:
        for k, v in headers.items():
            k.encode("latin-1"); v.encode("latin-1")
    except UnicodeEncodeError as e:
        return {"ok": False, "status": 0, "error": {"message": f"Некорректный символ в заголовке HTTP: {e}. Проверь токен."}}
    try:
        r = requests.request(method, url, headers=headers, params=params, json=json_, timeout=timeout)
        ct = (r.headers.get("content-type") or "")
        try:
            body = r.json() if "application/json" in ct else {"raw": r.text, "status_code": r.status_code}
        except Exception:
            body = {"raw": r.text, "status_code": r.status_code}
        if r.status_code >= 400:
            return {"ok": False, "status": r.status_code, "error": body}
        return {"ok": True, "status": r.status_code, "data": body}
    except requests.RequestException as e:
        return {"ok": False, "status": 0, "error": {"message": str(e)}}

def market_me():
    return api_req("GET", f"{FORUM_BASE}/market/me", LZT_FORUM_TOKEN)

def market_history(limit: Optional[int] = 20):
    params = {}
    if limit: params["limit"] = limit
    return api_req("GET", f"{MARKET_BASE}/user/payments", LZT_MARKET_TOKEN, params=params)

def market_fee(amount: int):
    return api_req("GET", f"{MARKET_BASE}/balance/transfer/fee", LZT_MARKET_TOKEN, params={"amount": amount})

def market_transfer(*, user_id: Optional[int]=None, username: Optional[str]=None, amount: int, comment: str = "", hold_value: Optional[int]=None, hold_option: Optional[str]=None):
    payload: Dict[str, Any] = {"amount": int(amount), "currency": "rub"}
    if user_id is not None:
        payload["user_id"] = int(user_id)
    elif username:
        payload["username"] = username.lstrip("@")
    if comment:
        payload["comment"] = comment
    if hold_value and hold_option:
        payload["transfer_hold"] = True
        payload["hold_length_value"] = int(hold_value)
        payload["hold_length_option"] = hold_option
    return api_req("POST", f"{MARKET_BASE}/balance/transfer", LZT_MARKET_TOKEN, json_=payload)

def market_payout_services():
    return api_req("GET", f"{MARKET_BASE}/balance/payout/services", LZT_MARKET_TOKEN)

def market_create_payout_v2(payment_system: str, wallet: str, amount: float, include_fee: bool=False, extra: Optional[Dict[str,Any]]=None):
    body = {"payment_system": str(payment_system), "wallet": str(wallet), "amount": float(amount), "currency": "rub", "include_fee": bool(include_fee), "extra": extra or {}}
    return api_req("POST", f"{MARKET_BASE}/balance/payout", LZT_MARKET_TOKEN, json_=body)

def market_create_payout(service_id: int, amount: float, requisites: Dict[str, Any]):
    return api_req("POST", f"{MARKET_BASE}/balance/payout", LZT_MARKET_TOKEN, json_={"service_id": service_id, "sum": amount, "requisites": requisites})

def forum_notification_content(notification_id: int):
    return api_req("GET", f"{FORUM_BASE}/notifications/{notification_id}/content", LZT_FORUM_TOKEN)

def forum_notifications(limit: Optional[int] = 20):
    params = {}
    if limit: params["limit"] = limit
    return api_req("GET", f"{FORUM_BASE}/notifications", LZT_FORUM_TOKEN, params=params)

def thread_bump(thread_id: int):
    return api_req("POST", f"{FORUM_BASE}/threads/{thread_id}/bump", LZT_FORUM_TOKEN)

def _ts(sec: int) -> str:
    try:
        return datetime.fromtimestamp(sec).strftime("%d.%m %H:%M")
    except Exception:
        return str(sec)

def _plural(n: int, one: str, few: str, many: str) -> str:
    n = abs(int(n)); n10 = n % 10; n100 = n % 100
    if n10 == 1 and n100 != 11: return one
    if 2 <= n10 <= 4 and not (12 <= n100 <= 14): return few
    return many

def human_hold(value: Optional[int], unit: Optional[str]) -> str:
    if not value or not unit: return "без удержания"
    u = {"hour": ("час", "часа", "часов"), "day": ("день", "дня", "дней"), "week": ("неделя", "недели", "недель"), "month": ("месяц", "месяца", "месяцев")}.get(unit, ("секунда","секунды","секунд"))
    return f"{value} {_plural(value, *u)}"

def normalize_url(u: str) -> str:
    u = (u or "").strip()
    return u.replace("prod-api.lolz.live", "lolz.live")

ANCHOR_RE = re.compile(r'<a[^>]+href=(?P<q>[\'"])(?P<href>.*?)(?P=q)[^>]*>(?P<text>.*?)</a>', re.IGNORECASE | re.DOTALL)

def _clean_text(s: str) -> str:
    s = re.sub(r"</?(br|p|li|ul|ol|div)[^>]*>", "\n", s, flags=re.IGNORECASE)
    s = re.sub(r"<[^>]+>", "", s)
    s = _html.unescape(s)
    s = s.replace("prod-api.lolz.live", "lolz.live")
    s = re.sub(r"[ \t]+\n", "\n", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    s = re.sub(r"[ \t]{2,}", " ", s).strip()
    return s

def _hash_notif(item: dict) -> str:
    nid = str(item.get("notification_id") or "")
    if nid: return "id:" + nid
    raw = f"{item.get('notification_create_date','')}-{item.get('notification_html','')}"
    return "h:" + hashlib.md5(raw.encode("utf-8", errors="ignore")).hexdigest()

def _extract_amount(text: str) -> Optional[str]:
    m = re.search(r'(\d[\d\s.,]*)\s*₽', text.replace('\xa0',' '))
    if not m: return None
    amt = m.group(1).replace(' ', '')
    return amt

def _grab_hold_deadline(text: str) -> Optional[str]:
    txt = _clean_text(text)
    m = re.search(r'(Холд\s+(?:закончится|до)\s+[^\n]+)', txt, re.IGNORECASE)
    return m.group(1) if m else None

def parse_notif(html: str, content: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    out = {
        "actor_name": "", "actor_url": "", "action": "", "type": "other",
        "thread_title": "", "thread_url": "", "thread_id": None,
        "post_id": None, "post_url": "", "snippet": ""
    }

    c = content or {}
    actor_obj = (c.get("actor") or c.get("user") or c.get("from_user")
                 or c.get("author") or c.get("sender") or {})
    if isinstance(actor_obj, dict):
        out["actor_name"] = actor_obj.get("username") or actor_obj.get("name") or out["actor_name"]
        uid = actor_obj.get("user_id") or actor_obj.get("id")
        if uid:
            out["actor_url"] = f"{SITE_FORUM}/members/{uid}"

    thread = c.get("thread") or {}
    post   = c.get("post") or c.get("comment") or {}
    if isinstance(thread, dict):
        out["thread_title"] = thread.get("title") or out["thread_title"]
        tid = thread.get("thread_id") or thread.get("id")
        if tid:
            out["thread_id"] = int(tid)
            out["thread_url"] = f"{SITE_FORUM}/threads/{tid}/"
    if isinstance(post, dict):
        pid = post.get("post_id") or post.get("comment_id") or post.get("id")
        if pid:
            out["post_id"] = int(pid)
            out["post_url"] = post.get("permalink") or f"{SITE_FORUM}/posts/{pid}/"
        body = post.get("body") or post.get("message") or post.get("text") or ""
        body = _clean_text(body)
        if body:
            out["snippet"] = (body[:300]).strip()

    raw_html = html or ""
    if raw_html:
        norm = raw_html.replace("\u2009", " ").replace("\xa0", " ")
        anchors = list(ANCHOR_RE.finditer(norm))

        if not out["actor_url"] or not out["actor_name"]:
            member_links = []
            for m in anchors:
                href = normalize_url(m.group("href") or "")
                if re.search(r"/members/\d+", href):
                    member_links.append((m.start(), href, _clean_text(m.group("text") or "")))
            raw_lower = _clean_text(norm).lower()
            verb_pat = re.compile(
                r"(упомянул\(а\)|упомянул|прокомментировал\(а\)|прокомментировал|"
                r"нравится ваше сообщение|нравится ваш комментарий|написал\(а\) сообщение в вашем профиле)"
            )
            mv = verb_pat.search(raw_lower)
            verb_idx = mv.start() if mv else None

            chosen = None
            if member_links:
                if verb_idx is not None:
                    before = [x for x in member_links if x[0] < verb_idx]
                    if before:
                        chosen = before[-1]          
                if not chosen:
                    chosen = member_links[0]        

            if chosen:
                _, href, text = chosen
                out["actor_url"] = href
                out["actor_name"] = text or out["actor_name"]

            if (not out["actor_url"] or not out["actor_name"]) and anchors:
                out["actor_url"] = normalize_url(anchors[0].group("href") or "")
                out["actor_name"] = _clean_text(anchors[0].group("text") or "") or out["actor_name"] or "Пользователь"

        if not out["thread_url"] and anchors:
            for m in anchors:
                href = normalize_url(m.group("href"))
                if re.search(r"/threads/(\d+)", href):
                    out["thread_url"] = href
                    title_text = _clean_text((m.group("text") or "")).strip()
                    if title_text:
                        out["thread_title"] = out["thread_title"] or title_text
                    mm = re.search(r"/threads/(\d+)", href)
                    if mm:
                        out["thread_id"] = int(mm.group(1))
                    break

        if not out["post_url"] and anchors:
            for m in anchors:
                href = normalize_url(m.group("href"))
                if (re.search(r"/posts/(comments/)?\d+/?$", href) or
                    re.search(r"#post-\d+$", href) or
                    re.search(r"/profile-posts(/comments)?/\d+/?$", href)):
                    out["post_url"] = href
                    break

        mm = re.search(r"/threads/(\d+)/#post-(\d+)", norm)
        if mm and not out["post_url"]:
            out["thread_id"] = out["thread_id"] or int(mm.group(1))
            out["post_id"] = int(mm.group(2))
            out["thread_url"] = out["thread_url"] or f"{SITE_FORUM}/threads/{out['thread_id']}/"
            out["post_url"]   = f"{SITE_FORUM}/posts/{out['post_id']}/"

        raw = _clean_text(norm).lower()
        def has(parts: List[str]) -> bool:
            return any(p in raw for p in parts)

        if not out["action"] or out["type"] in {"other", ""}:
            if has(["холд на платеж", "холд закончился", "холд по платежу снят", "холд завершился"]):
                out["type"], out["action"] = "hold_released", "холд закончился"
            elif "нравится ваше сообщение" in raw or "нравится ваш комментарий" in raw:
                out["type"], out["action"] = "like", "поставил(а) ❤️ либо 👍 вашему сообщению"
            elif has(["упомянул(а) вас", "упомянул вас", "упомянул(а) в сообщении"]):
                out["type"], out["action"] = "mention", "упомянул(а) вас"
            elif has(["прокомментировал(а) ваше сообщение", "прокомментировал ваше сообщение"]):
                out["type"], out["action"] = "comment", "прокомментировал(а) ваше сообщение"
            elif has(["прокомментировал(а) запись в вашем профиле", "прокомментировал вашу запись на стене", "вашей записи на стене", "запись в вашем профиле"]):
                out["type"], out["action"] = "profile_comment", "прокомментировал(а) запись в вашем профиле"
            elif has(["написал(а) на вашей стене", "оставил(а) сообщение в вашем профиле", "сообщение на вашей стене"]):
                out["type"], out["action"] = "profile_post", "написал(а) сообщение в вашем профиле"
            elif has(["зачислены на ваш баланс", "пополнение баланса", "получен платеж"]):
                out["type"], out["action"] = "payment_in", "зачисление на баланс"
            elif has(["отправил(а) вам", "перевёл вам", "перевел вам"]):
                if "холд закончится" in raw or "установлен холд" in raw or "холд до" in raw:
                    out["type"], out["action"] = "transfer_in_hold", "перевёл(а) вам"
                else:
                    out["type"], out["action"] = "transfer_in", "перевёл(а) вам"
            else:
                out["type"], out["action"] = "other", _clean_text(norm)

        if not out["snippet"]:
            m_snip = re.search(r'<div[^>]+class="[^"]*\bcontentRow-snippet\b[^"]*"[^>]*>(.*?)</div>', norm, re.IGNORECASE | re.DOTALL)
            if m_snip:
                out["snippet"] = _clean_text(m_snip.group(1))[:300].strip()
        if not out["snippet"]:
            m_msg = re.search(r'<(div|article)[^>]+class="[^"]*(message-body|message-content|message-cell|bbWrapper|bbCodeBlock-content)[^"]*"[^>]*>(.*?)</\1>', norm, re.IGNORECASE | re.DOTALL)
            if m_msg:
                out["snippet"] = _clean_text(m_msg.group(3))[:300].strip()
        if not out["snippet"]:
            m_bq = re.search(r'<blockquote[^>]*>(.*?)</blockquote>', norm, re.IGNORECASE | re.DOTALL)
            if m_bq:
                out["snippet"] = _clean_text(m_bq.group(1))[:300].strip()
        if not out["snippet"]:
            all_quotes = re.findall(r'«([^»]{1,300})»', _clean_text(norm))
            if all_quotes:
                q_candidates = [q.strip() for q in all_quotes
                                if q.strip() and q.strip() != (out.get("thread_title") or "").strip()]
                if q_candidates:
                    out["snippet"] = max(q_candidates, key=len)

        if out["type"] in {"transfer_in", "transfer_in_hold", "hold_released", "payment_in"}:
            amt = _extract_amount(_clean_text(html or ""))
            if amt:
                if out["type"] in {"transfer_in", "transfer_in_hold"}:
                    out["action"] = f"перевёл(а) вам +{amt} ₽" + (" (холд)" if out["type"] == "transfer_in_hold" else "")
                elif out["type"] == "hold_released":
                    out["snippet"] = out["snippet"] or f"Сумма: {amt} ₽"
                elif out["type"] == "payment_in":
                    out["snippet"] = out["snippet"] or f"Сумма: +{amt} ₽"
            if out["type"] == "transfer_in_hold":
                hold_line = _grab_hold_deadline(html)
                if hold_line:
                    out["snippet"] = hold_line

        if out["snippet"] and out["snippet"].strip() == (out.get("thread_title") or "").strip():
            out["snippet"] = ""

    out["actor_name"] = out["actor_name"] or "Пользователь"
    return out



def _a(name: str, url: str) -> str:
    if url and name:
        return f'<a href="{url}">{_html.escape(name)}</a>'
    return _html.escape(name or "Пользователь")

def _action_prefix(t: str) -> str:
    return {
        "like": "❤️",
        "comment": "💬",
        "mention": "🏷️",
        "payment_in": "✅",
        "transfer_in": "💵",
        "transfer_in_hold": "💵",
        "hold_released": "🟢",
        "profile_post": "🧱",
        "profile_comment": "🧩"
    }.get(t, "🔔")

def render_notif_line(item: dict, content: Optional[Dict[str, Any]] = None) -> Tuple[str, Optional[InlineKeyboardMarkup]]:
    dt = _ts(int(item.get("notification_create_date", 0)))
    m = parse_notif(item.get("notification_html", "") or "", content)
    actor = _a(m.get("actor_name") or "", m.get("actor_url") or "")
    icon  = _action_prefix(m.get("type") or "other")
    lines: List[str] = [f"🕒 {dt}", f"{icon} {actor} {m.get('action') or ''}".strip()]
    if (m.get("thread_title") or "").strip():
        lines.append(f"🧵 <a href=\"{m['thread_url']}\">{_html.escape(m['thread_title'])}</a>")
    if m.get("snippet"):
        lines.append(f"«{_html.escape(m['snippet'])}»")

    kb = None
    kbldr = InlineKeyboardBuilder()
    if m.get("post_url"):
        kbldr.button(text="К записи" if (m.get("type") in {"profile_post","profile_comment"}) else "К сообщению", url=m["post_url"])
    if m.get("thread_url"):
        kbldr.button(text="К теме", url=m["thread_url"])
    if kbldr.buttons:
        kbldr.adjust(2)
        kb = kbldr.as_markup()
    return "\n".join([s for s in lines if s.strip()]), kb


def _onoff(flag: bool) -> str:
    return "Вкл" if flag else "Выкл"

def kb_main() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="💸 Перевод", callback_data="act:transfer")
    kb.button(text="➕ Инвойс (RUB)", callback_data="act:invoice")
    kb.button(text="🏦 Вывод", callback_data="act:payout")
    kb.button(text="💼 Баланс", callback_data="act:balance")
    kb.button(text="🔔 Уведомления", callback_data="act:notifs_menu")
    kb.button(text="📌 Автоподнятие", callback_data="act:autobump")
    kb.button(text="🗒 Заметки", callback_data="act:notes")
    kb.button(text="❌ Закрыть", callback_data="act:cancel")
    kb.adjust(2,2,2,2,1)
    return kb.as_markup()

def kb_form(cancel=True, back=True) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    if back: kb.button(text="⬅️ Назад", callback_data="go:menu")
    if cancel: kb.button(text="❌ Отмена", callback_data="act:cancel")
    return kb.as_markup()

def kb_notifs_menu() -> InlineKeyboardMarkup:
    s = get_settings()
    kb = InlineKeyboardBuilder()
    kb.button(text=f"💬 Комментарии: {_onoff(s['notify_comments']).upper()}.", callback_data="notifs:t:comments")
    kb.button(text=f"📙 Упоминания: {_onoff(s['notify_mentions']).upper()}.", callback_data="notifs:t:mentions")
    kb.button(text=f"❤️ Лайки: {_onoff(s['notify_likes']).upper()}.", callback_data="notifs:t:likes")
    kb.button(text=f"✅ Зачисления: {_onoff(s['notify_payment_in']).upper()}.", callback_data="notifs:t:payment_in")
    kb.button(text=f"🟢 Снятие холда: {_onoff(s['notify_hold_released']).upper()}.", callback_data="notifs:t:hold_released")
    kb.button(text=f"🧱 Сообщения на стене: {_onoff(s['notify_profile_post']).upper()}.", callback_data="notifs:t:profile_post")
    kb.button(text=f"🧩 Коммент. к стене: {_onoff(s['notify_profile_comment']).upper()}.", callback_data="notifs:t:profile_comment")
    kb.button(text=f"{'🔔 ВКЛ. автопуш' if s.get('push_cards_enabled', True) else '🔕 ВЫКЛ. автопуш'}", callback_data="notifs:t:autopush")
    kb.button(text="🏠 Меню", callback_data="go:menu")
    kb.adjust(1,1,1,1,1,1,1,1,1)
    return kb.as_markup()


class TransferState(StatesGroup):
    ident = State(); amount = State(); comment = State(); hold = State(); note = State()
class InvoiceState(StatesGroup):
    amount = State(); merchant_id = State(); payment_id = State(); comment = State(); note = State()
class PayoutState(StatesGroup):
    service_pick = State(); amount = State(); wallet = State(); include_fee = State(); extra = State()
class BumpState(StatesGroup):
    menu = State(); add = State(); del_ = State()

bot = Bot(TG_BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher(); rt = Router(); dp.include_router(rt)

async def guard(obj) -> bool:
    uid = obj.from_user.id
    if not is_admin(uid):
        if isinstance(obj, Message):
            await obj.answer("⛔ Доступ ограничен.")
        else:
            await obj.answer("⛔ Доступ ограничен.", show_alert=True)
        return False
    return True

@rt.message(CommandStart())
async def on_start(m: Message, state: FSMContext):
    if not await guard(m): return await state.clear()
    await m.answer("Главное меню.", reply_markup=kb_main())

@rt.message(Command("menu"))
async def on_menu(m: Message, state: FSMContext):
    if not await guard(m): return await state.clear()
    await m.answer("Главное меню.", reply_markup=kb_main())

@rt.callback_query(F.data == "go:menu")
async def go_menu(cb: CallbackQuery, state: FSMContext):
    if not await guard(cb): return await state.clear()
    try:
        await cb.message.edit_text("Главное меню.", reply_markup=kb_main())
    except TelegramBadRequest:
        await cb.message.answer("Главное меню.", reply_markup=kb_main())
    await cb.answer()

@rt.callback_query(F.data == "act:cancel")
async def on_cancel_root(cb: CallbackQuery, state: FSMContext):
    if not await guard(cb): return await state.clear()
    try:
        await cb.message.edit_text("❌ Закрыто. Нажми /menu для открытия.")
    except TelegramBadRequest:
        await cb.message.answer("❌ Закрыто. Нажми /menu для открытия.")
    await cb.answer()

@rt.callback_query(F.data == "act:balance")
async def act_balance(cb: CallbackQuery):
    if not await guard(cb): return
    me = market_me()
    if me["ok"]:
        u = (me["data"] or {}).get("user", {})
        bal = u.get("balance", 0); hold = u.get("hold", 0)
        cur = (u.get("currency") or "rub").upper()
        header = f"💼 <b>Баланс</b>\nВалюта: <b>{cur}</b> • Доступно: <b>{bal}</b> • Холд: <b>{hold}</b>"
    else:
        header = "💼 <b>Баланс</b>\n(не удалось получить /market/me)"
    hist = market_history(limit=10)
    body = render_payments_short(hist["data"], 10) if hist["ok"] else f"⚠️ История недоступна ({hist.get('status')})."
    await cb.message.answer(f"{header}\n\n🧾 <b>Последние операции</b>\n{body}")

def _notifs_header() -> str:
    s = get_settings()
    t = datetime.now().strftime("%H:%M:%S")
    return (
        "🔔 <b>Уведомления</b>\n"
        f"💬 Комментарии: <b>{_onoff(s['notify_comments'])}</b>\n"
        f"📙 Упоминания: <b>{_onoff(s['notify_mentions'])}</b>\n"
        f"❤️ Лайки: <b>{_onoff(s['notify_likes'])}</b>\n"
        f"✅ Зачисления: <b>{_onoff(s['notify_payment_in'])}</b>\n"
        f"🟢 Снятие холда: <b>{_onoff(s['notify_hold_released'])}</b>\n"
        f"🧱 Сообщения на стене: <b>{_onoff(s['notify_profile_post'])}</b>\n"
        f"🧩 Коммент. к стене: <b>{_onoff(s['notify_profile_comment'])}</b>\n"
        f"Автопуш новых: <b>{'включён' if s.get('push_cards_enabled', True) else 'выключен'}</b>    <i>{t}</i>"
    )

@rt.callback_query(F.data == "act:notifs_menu")
async def act_notifs_menu(cb: CallbackQuery):
    if not await guard(cb): return
    txt = _notifs_header()
    try:
        await cb.message.edit_text(txt, reply_markup=kb_notifs_menu())
    except TelegramBadRequest:
        await cb.message.answer(txt, reply_markup=kb_notifs_menu())
    await cb.answer()

@rt.callback_query(F.data.startswith("notifs:t:"))
async def toggle_notif(cb: CallbackQuery):
    if not await guard(cb): return
    key = cb.data.split(":", 2)[2]
    s = get_settings()
    if key == "autopush":
        set_setting("push_cards_enabled", not s.get("push_cards_enabled", True))
    else:
        map_keys = {
            "comments": "notify_comments",
            "mentions": "notify_mentions",
            "likes": "notify_likes",
            "payment_in": "notify_payment_in",
            "hold_released": "notify_hold_released",
            "profile_post": "notify_profile_post",
            "profile_comment": "notify_profile_comment",
        }
        skey = map_keys.get(key)
        if skey:
            set_setting(skey, not s.get(skey, True))
    try:
        await cb.message.edit_text(_notifs_header(), reply_markup=kb_notifs_menu())
    except TelegramBadRequest:
        await cb.message.answer(_notifs_header(), reply_markup=kb_notifs_menu())
    await cb.answer()


def render_payments_short(data: Any, n: int = 10) -> str:
    items: List[Dict[str, Any]] = []
    payments = (data or {}).get("payments", {})
    for v in payments.values():
        items.append(v)
    items.sort(key=lambda x: x.get("operation_date", 0), reverse=True); items = items[:n]
    lines = []
    for it in items:
        dt = _ts(int(it.get("operation_date", 0)))
        incoming = it.get("incoming_sum", "0"); outgoing = it.get("outgoing_sum", "0")
        user = (it.get("data") or {}).get("username") or ""
        lbl = (it.get("label") or {}).get("title") or (it.get("operation_type") or "")
        add_user = user and (user not in lbl) and ("от " not in lbl) and ("кому " not in lbl)
        if incoming and str(incoming) != "0.00":
            lines.append(f"🟢 {dt} +{incoming} — {lbl}{(' от '+user) if add_user else ''}".strip())
        else:
            lines.append(f"🔴 {dt} -{outgoing} — {lbl}{(' кому '+user) if add_user else ''}".strip())
    return "\n".join(lines) if lines else "Пока нет операций."

class TransferState(StatesGroup):
    ident = State(); amount = State(); comment = State(); hold = State(); note = State()
class InvoiceState(StatesGroup):
    amount = State(); merchant_id = State(); payment_id = State(); comment = State(); note = State()
class PayoutState(StatesGroup):
    service_pick = State(); amount = State(); wallet = State(); include_fee = State(); extra = State()
class BumpState(StatesGroup):
    menu = State(); add = State(); del_ = State()

@rt.callback_query(F.data == "act:payout")
async def act_payout(cb: CallbackQuery, state: FSMContext):
    if not await guard(cb): return await state.clear()
    await state.set_state(PayoutState.service_pick)
    services = market_payout_services()
    if services["ok"]:
        systems = services["data"].get("systems", services["data"])
        lines, mapping = [], {}
        if isinstance(systems, dict):
            systems = list(systems.values())
        for i, s in enumerate(systems, 1):
            title = s.get("title") or s.get("system") or s.get("payment_system") or "сервис"
            code = next((s.get(k) for k in ("payment_system","system","code","slug","system_code") if s.get(k)), None) or (s.get("id") and str(s["id"])) or f"svc{i}"
            minv = s.get("min") or s.get("min_sum") or "?"
            maxv = s.get("max") or s.get("max_sum") or "?"
            lines.append(f"{i}. {title} — code: <code>{code}</code> • мин: {minv} • макс: {maxv}")
            mapping[str(i)] = s; mapping[str(str(code).lower())] = s; mapping[str(title).lower()] = s
        await state.update_data(_payout_map=mapping)
        txt = "🏦 <b>Сервисы вывода</b>\nПришли номер, code или название из списка.\n\n" + "\n".join(lines[:120])
        try:
            await cb.message.edit_text(txt, reply_markup=kb_form())
        except TelegramBadRequest:
            await cb.message.answer(txt, reply_markup=kb_form())
    else:
        await cb.message.answer(fmt_err("Сервисы вывода", services), reply_markup=kb_form())
    await cb.answer()

@rt.message(PayoutState.service_pick)
async def payout_pick(m: Message, state: FSMContext):
    if not await guard(m): return
    s = (m.text or "").strip().lower()
    svc = (await state.get_data()).get("_payout_map", {}).get(s)
    if not svc:
        await m.reply("⚠️ Не нашёл сервис. Введи номер из списка, code или точное название.", reply_markup=kb_form()); return
    code = next((svc.get(k) for k in ("payment_system","system","code","slug","system_code") if svc.get(k)), None)
    await state.update_data(_svc=svc, _ps_code=code, _svc_id=svc.get("id"))
    await state.set_state(PayoutState.amount)
    title = svc.get('title') or svc.get('system') or svc.get('payment_system') or 'сервис'
    await m.answer(f"✅ Выбран: <b>{title}</b> (code: <code>{code}</code>)\n\n💵 Введи сумму для вывода:", reply_markup=kb_form())

@rt.message(PayoutState.amount)
async def payout_amount(m: Message, state: FSMContext):
    if not await guard(m): return
    try:
        amount = float(m.text.replace(",", ".").strip()); assert amount > 0
    except Exception:
        await m.reply("⚠️ Неверная сумма.", reply_markup=kb_form()); return
    await state.update_data(_amount=amount)
    await state.set_state(PayoutState.wallet)
    await m.answer("🧾 Введи кошелёк <b>wallet</b> (только значение):", reply_markup=kb_form())

@rt.message(PayoutState.wallet)
async def payout_wallet(m: Message, state: FSMContext):
    if not await guard(m): return
    wallet = (m.text or "").strip()
    if not wallet:
        await m.reply("⚠️ Нужен кошелёк.", reply_markup=kb_form()); return
    await state.update_data(_wallet=wallet)
    await state.set_state(PayoutState.include_fee)
    await m.answer("Учесть комиссию? (да/нет):", reply_markup=kb_form())

@rt.message(PayoutState.include_fee)
async def payout_fee(m: Message, state: FSMContext):
    if not await guard(m): return
    ans = (m.text or "").strip().lower()
    include_fee = ans in {"да","yes","+","y","true","1"}
    await state.update_data(_include_fee=include_fee)
    await state.set_state(PayoutState.extra)
    await m.answer("Доп. параметры (обычно не нужны) — введи строку вида:\n<code>KEY=VAL;KEY2=VAL2</code>\nНапример: <code>COMMENT=Мойкоммент</code>\nЕсли не знаешь — пришли «-».", reply_markup=kb_form())

@rt.message(PayoutState.extra)
async def payout_extra(m: Message, state: FSMContext):
    if not await guard(m): return
    extra: Dict[str,str] = {}
    if (m.text or "").strip() not in {"-","—",""}:
        for part in m.text.split(";"):
            if "=" in part:
                k,v = part.split("=",1); k=k.strip(); v=v.strip()
                if k: extra[k]=v
    data = await state.get_data()
    await m.answer("⏳ Отправляю заявку на вывод…")
    if data.get("_ps_code"):
        resp = market_create_payout_v2(payment_system=data["_ps_code"], wallet=data["_wallet"], amount=data["_amount"], include_fee=data.get("_include_fee", False), extra=extra or None)
    else:
        reqs = {"WALLET": data["_wallet"]}; reqs.update(extra)
        resp = market_create_payout(int(data["_svc_id"]), float(data["_amount"]), reqs)
    await state.clear()
    if resp["ok"]:
        await m.answer("✅ Вывод создан.", reply_markup=kb_main())
    else:
        await m.answer(fmt_err("Вывод", resp), reply_markup=kb_main())

def parse_recipient(text: str) -> Tuple[Optional[int], Optional[str]]:
    s = text.strip()
    m = re.search(r"(?:lolz\.live|zelenka\.guru)/members/(\d+)", s)
    if m: return int(m.group(1)), None
    m = re.search(r"lzt\.market/(?:user|users)/(\d+)", s)
    if m: return int(m.group(1)), None
    if re.fullmatch(r"\d{1,12}", s): return int(s), None
    if re.fullmatch(r"@?[A-Za-z0-9_.-]{3,32}", s): return None, s.lstrip("@")
    return None, None

def parse_hold_option(s: str) -> Tuple[Optional[int], Optional[str], int, bool]:
    s = (s or "0").strip().lower()
    if s == "0": return None, None, 0, True
    m = re.fullmatch(r"(\d+)\s*(m|h|d|w|mo)", s)
    if not m: return None, None, 0, False
    val = max(1, int(m.group(1))); unit = m.group(2)
    if unit == "m":
        hours = max(1, math.ceil(val/60)); secs = hours*3600; return hours, "hour", secs, secs <= 30*86400
    if unit == "h":
        secs = val*3600; return val, "hour", secs, secs <= 30*86400
    if unit == "d":
        secs = val*86400; return val, "day", secs, secs <= 30*86400
    if unit == "w":
        secs = val*7*86400;return val, "week", secs, secs <= 30*86400
    if unit == "mo":
        secs = val*30*86400;return val, "month", secs, secs <= 30*86400
    return None, None, 0, False

@rt.callback_query(F.data == "act:transfer")
async def act_transfer(cb: CallbackQuery, state: FSMContext):
    if not await guard(cb): return await state.clear()
    await state.set_state(TransferState.ident)
    try:
        await cb.message.edit_text("💸 <b>Перевод средств</b>\n\n👤 Введи @username / ID / ссылку на профиль получателя:", reply_markup=kb_form())
    except TelegramBadRequest:
        await cb.message.answer("💸 <b>Перевод средств</b>\n\n👤 Введи @username / ID / ссылку на профиль получателя:", reply_markup=kb_form())
    await cb.answer()

@rt.message(TransferState.ident)
async def tr_ident(m: Message, state: FSMContext):
    if not await guard(m): return
    uid, uname = parse_recipient(m.text)
    if not (uid or uname):
        await m.reply("⚠️ Пришли ID, @username или ссылку на профиль.", reply_markup=kb_form()); return
    await state.update_data(recipient_id=uid, recipient_username=uname)
    await state.set_state(TransferState.amount)
    await m.answer("💵 Введи <b>сумму</b> (целое, например 125):", reply_markup=kb_form())

@rt.message(TransferState.amount)
async def tr_amount(m: Message, state: FSMContext):
    if not await guard(m): return
    try:
        amount = int(m.text.strip()); assert amount >= 1
    except Exception:
        await m.reply("⚠️ Введи целое число ≥ 1.", reply_markup=kb_form()); return
    await state.update_data(amount=amount)
    pct = 0; fee = market_fee(amount)
    if fee["ok"]:
        pct = int((fee["data"] or {}).get("commission_percentage", 0) or 0)
    total = amount * (100+pct) / 100
    await state.set_state(TransferState.comment)
    await m.answer("✍️ Комментарий к переводу (или «-» чтобы пропустить)\n" + f"Сумма: <b>{amount}</b> RUB • Комиссия: <b>{pct}%</b> • К списанию: <b>{total:.0f}</b> RUB", reply_markup=kb_form())

@rt.message(TransferState.comment)
async def tr_comment(m: Message, state: FSMContext):
    if not await guard(m): return
    comment = "" if m.text.strip() in {"-", "—"} else m.text.strip()
    await state.update_data(comment=comment); await state.set_state(TransferState.hold)
    await m.answer("⏳ Холд: 0 / 2h / 1d / 1w / 1mo (не больше 1 месяца):", reply_markup=kb_form())

@rt.message(TransferState.hold)
async def tr_hold(m: Message, state: FSMContext):
    if not await guard(m): return
    hv, ho, secs, ok = parse_hold_option(m.text)
    if not ok:
        await m.reply("⚠️ Неверный формат или > 1 месяца. Примеры: 0, 2h, 1d, 1w, 1mo.", reply_markup=kb_form()); return
    await state.update_data(hold_value=hv, hold_option=ho, hold_seconds=secs, hold_human=human_hold(hv, ho))
    data = await state.get_data()
    uid = data.get("recipient_id"); uname = data.get("recipient_username")
    who_line = f'<a href="{SITE_FORUM}/members/{uid}">{uid}</a>' if uid else ("@"+uname)
    hold_line = data.get("hold_human", "без удержания")
    if hv and int(data["amount"]) <= 10:
        hold_line += " ⚠️ (минимум > 10 ₽)"
    summary = f"📤 <b>Подтверждение перевода</b>\n" + f"Получатель: {who_line}\n" + f"Сумма: <b>{data['amount']}</b> RUB\n" + f"Комментарий: {data.get('comment') or '—'}\n" + f"Холд: <b>{hold_line}</b>"
    await m.answer(summary, disable_web_page_preview=True)
    await state.set_state(TransferState.note)
    await m.answer("🗒 Добавить секретную заметку к переводу? (или «-»):", reply_markup=kb_form())

@rt.message(TransferState.note)
async def tr_note(m: Message, state: FSMContext):
    if not await guard(m): return
    note = "" if m.text.strip() in {"-", "—"} else m.text.strip()
    await state.update_data(secret_note=note)
    await m.answer("✅ Ок. Выполняю перевод…")
    data = await state.get_data()
    resp = market_transfer(user_id=data.get("recipient_id"), username=data.get("recipient_username"), amount=data["amount"], comment=data.get("comment",""), hold_value=data.get("hold_value"), hold_option=data.get("hold_option"))
    if resp["ok"]:
        if note:
            notes = _load(NOTES_FILE, {"items":[]})
            notes["items"].append({"type":"transfer","created_at": int(time.time()),"amount": data["amount"], "to": data.get("recipient_id") or data.get("recipient_username"), "comment": data.get("comment",""),"note": note})
            _save(NOTES_FILE, notes)
        await m.answer("✅ Перевод отправлен.", reply_markup=kb_main())
        secs = data.get("hold_seconds", 0)
        if secs > 0:
            if secs > 3600:
                asyncio.create_task(remind_after(secs - 3600, f"⏳ Напоминание: через <b>1 час</b> холд по переводу {data['amount']} RUB снимется.", m.chat.id))
            else:
                mins = max(1, secs//60)
                asyncio.create_task(remind_after(0, f"⏳ Напоминание: холд снимется через ~<b>{mins} мин</b>.", m.chat.id))
            asyncio.create_task(remind_after(secs, f"✅ Холд по переводу {data['amount']} RUB <b>снят</b>.", m.chat.id))
    else:
        await m.answer(fmt_err("Перевод", resp), reply_markup=kb_main())
    await state.clear()

async def remind_after(delay_sec: int, text: str, chat_id: int):
    try:
        await asyncio.sleep(max(0, delay_sec))
        await bot.send_message(chat_id, text)
    except Exception:
        pass

def market_create_invoice(amount: float, merchant_id: int, payment_id: str,
                          comment: str, url_success: str, url_callback: str,
                          lifetime: int = 43200):
    lifetime = max(60, min(int(lifetime), 43200))
    body = {
        "currency": "rub",
        "amount": float(amount),
        "payment_id": str(payment_id),
        "comment": comment or "-",
        "url_success": url_success,
        "merchant_id": int(merchant_id),
        "lifetime": lifetime,
    }
    if url_callback:
        body["url_callback"] = url_callback
    return api_req("POST", f"{MARKET_BASE}/invoice", LZT_MARKET_TOKEN, json_=body)


@rt.callback_query(F.data == "act:invoice")
async def act_invoice(cb: CallbackQuery, state: FSMContext):
    if not await guard(cb):
        return await state.clear()
    await state.set_state(InvoiceState.amount)
    try:
        await cb.message.edit_text(
            "➕ <b>Создание инвойса</b>\n\n💵 Введи сумму (1–1_000_000 RUB):",
            reply_markup=kb_form()
        )
    except TelegramBadRequest:
        await cb.message.answer(
            "➕ <b>Создание инвойса</b>\n\n💵 Введи сумму (1–1_000_000 RUB):",
            reply_markup=kb_form()
        )
    await cb.answer()


@rt.message(InvoiceState.amount)
async def inv_amount(m: Message, state: FSMContext):
    if not await guard(m): return
    try:
        amount = float(m.text.replace(",", ".").strip())
        assert 1 <= amount <= 1_000_000
    except Exception:
        await m.reply("⚠️ Укажите сумму счёта от 1 ₽ до 1 000 000 ₽",
                      reply_markup=kb_form())
        return
    await state.update_data(amount=amount)
    await state.set_state(InvoiceState.merchant_id)
    await m.answer("🏪 Введи <b>merchant_id</b> продавца:", reply_markup=kb_form())


@rt.message(InvoiceState.merchant_id)
async def inv_merchant_id(m: Message, state: FSMContext):
    if not await guard(m): return
    if not re.fullmatch(r"\d+", m.text.strip()):
        await m.reply("⚠️ Введи числовой <b>merchant_id</b>.",
                      reply_markup=kb_form())
        return
    await state.update_data(merchant_id=int(m.text.strip()))
    await state.set_state(InvoiceState.payment_id)
    await m.answer("🧾 Введи <b>payment_id</b> (уникальный или «-»):",
                   reply_markup=kb_form())


def _pick_invoice_id(j: Any) -> Optional[str]:
    if isinstance(j, dict):
        for k in ("invoice_id", "id", "uuid"):
            if j.get(k): return str(j[k])
        d = j.get("data") or j.get("invoice") or {}
        if isinstance(d, dict):
            for k in ("invoice_id", "id", "uuid"):
                if d.get(k): return str(d[k])
    return None


@rt.message(InvoiceState.payment_id)
async def inv_payment_id(m: Message, state: FSMContext):
    if not await guard(m): return
    pid = m.text.strip()
    if pid in {"-", "—", ""}:
        pid = datetime.now().strftime("i%Y%m%d%H%M%S") + "-" + secrets.token_hex(3)
    if not re.fullmatch(r"[A-Za-z0-9_.:-]{1,64}", pid):
        await m.reply("⚠️ Нужен корректный <b>payment_id</b> (до 64 символов).",
                      reply_markup=kb_form())
        return
    pid = f"{pid}-{int(time.time())}"
    await state.update_data(payment_id=pid)
    await state.set_state(InvoiceState.comment)
    await m.answer("✍️ Комментарий к инвойсу (обязателен):", reply_markup=kb_form())


def _get_expire_ts(payload: Dict[str, Any]) -> Tuple[Optional[int], Optional[int]]:
    j = payload or {}
    d = j.get("data") or j.get("invoice") or j
    created = d.get("created_at") or d.get("created") or None
    expires = d.get("expires_at") or d.get("expired_at") or d.get("expires") or None
    ttl = d.get("ttl") or d.get("time_left") or d.get("lifetime")
    now = int(time.time())
    try: created = int(created)
    except: created = None
    try: expires = int(expires)
    except: expires = None
    if expires is None and ttl:
        try:
            expires = now + int(ttl)
            if created is None:
                created = now
        except: pass
    return created, expires


@rt.message(InvoiceState.comment)
async def inv_comment(m: Message, state: FSMContext):
    if not await guard(m): return
    comment = (m.text or "").strip()
    data = await state.get_data()
    if not comment:
        await m.reply("⚠️ Комментарий обязателен.", reply_markup=kb_form())
        return

    await m.answer("⏳ Создаю инвойс…")
    resp = market_create_invoice(
        amount=data["amount"],
        merchant_id=data["merchant_id"],
        payment_id=data["payment_id"],
        comment=comment,
        url_success=INVOICE_SUCCESS_URL,
        url_callback=INVOICE_CALLBACK_URL,
        lifetime=43200
    )

    if resp["ok"]:
        j = resp["data"]
        link = (j.get("link") or j.get("invoice_link")
                or (j.get("data", {}) or {}).get("link", "")) or ""
        inv_id = _pick_invoice_id(j) or "?"
        if not link and inv_id not in {"?", ""}:
            link = f"{SITE_MARKET}/invoice/{inv_id}"

        created_ts, expire_ts = _get_expire_ts(j)
        now_ts = int(time.time())
        is_expired = expire_ts and expire_ts <= now_ts

        await state.update_data(
            _last_invoice_id=inv_id, _comment=comment,
            amount=data["amount"], merchant_id=data["merchant_id"],
            payment_id=data["payment_id"]
        )

        status_line = "⚠️ <b>Инвойс уже истёк</b>\n" if is_expired else "✅ Инвойс создан\n"
        time_line = ""
        if created_ts:
            time_line += f"🕒 Создан: {_ts(created_ts)}\n"
        if expire_ts:
            time_line += f"⏳ Действует до: {_ts(expire_ts)}"
        else:
            time_line += "⏳ Срок действия: неизвестен"

        text = (f"{status_line}ID: <b>{inv_id}</b>\n"
                f"Сумма: <b>{data['amount']:.2f} RUB</b>\n"
                f"Merchant: <b>{data['merchant_id']}</b>\n"
                f"Payment ID: <code>{data['payment_id']}</code>\n"
                f"{time_line}")
        if link:
            text += f'\n🔗 <a href="{link}">Оплатить</a>'

        await state.set_state(InvoiceState.note)
        await m.answer(text, disable_web_page_preview=True)
        await m.answer("🗒 Добавить секретную заметку к инвойсу? (или «-»):",
                       reply_markup=kb_form())
    else:
        await state.clear()
        await m.answer(fmt_err("Инвойс", resp), reply_markup=kb_main())


@rt.message(InvoiceState.note)
async def inv_note(m: Message, state: FSMContext):
    if not await guard(m): return
    note = "" if m.text.strip() in {"-", "—"} else m.text.strip()
    data = await state.get_data()
    inv_id = data.get("_last_invoice_id")
    if note and inv_id:
        notes = _load(NOTES_FILE, {"items": []})
        notes["items"].append({
            "type": "invoice",
            "created_at": int(time.time()),
            "invoice_id": inv_id,
            "amount": data.get("amount"),
            "merchant_id": data.get("merchant_id"),
            "payment_id": data.get("payment_id"),
            "comment": data.get("_comment", ""),
            "note": note
        })
        _save(NOTES_FILE, notes)
        await m.answer("🗒 Заметка сохранена.", reply_markup=kb_main())
    else:
        await m.answer("Ок, без заметки.", reply_markup=kb_main())
    await state.clear()


def kb_notes() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="🗑 Очистить заметки", callback_data="notes:clear")
    kb.button(text="🏠 Меню", callback_data="go:menu")
    kb.adjust(1,1)
    return kb.as_markup()

@rt.callback_query(F.data == "act:notes")
async def act_notes(cb: CallbackQuery):
    if not await guard(cb): return
    items = _load(NOTES_FILE, {"items":[]}).get("items", [])
    if not items:
        await cb.message.answer("🗒 Пока нет секретных заметок.", reply_markup=kb_notes()); await cb.answer(); return
    items = sorted(items, key=lambda x: x.get("created_at", 0), reverse=True)[:30]
    lines = []
    for it in items:
        dt = _ts(int(it.get("created_at", 0)))
        if it.get("type") == "invoice":
            lines.append(f"🧾 [{dt}] Инвойс #{it.get('invoice_id')} • {it.get('amount')} RUB — {it.get('note')}")
        else:
            lines.append(f"💸 [{dt}] Перевод → {it.get('to')} • {it.get('amount')} RUB — {it.get('note')}")
    await cb.message.answer("🗒 <b>Секретные заметки</b>\n" + "\n".join(lines), reply_markup=kb_notes())
    await cb.answer()

@rt.callback_query(F.data == "notes:clear")
async def notes_clear(cb: CallbackQuery):
    if not await guard(cb): return
    _save(NOTES_FILE, {"items":[]})
    await cb.message.answer("🧹 Готово! Все заметки удалены.", reply_markup=kb_main())
    await cb.answer()

def parse_thread_id(text: str) -> Optional[int]:
    s = text.strip(); m = re.search(r"/threads/(\d+)", s)
    if m: return int(m.group(1))
    if re.fullmatch(r"\d+", s): return int(s)
    return None

def kb_bumps_menu() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="➕ Добавить тему", callback_data="b:add")
    kb.button(text="🗑 Удалить", callback_data="b:del")
    kb.button(text="📜 Список", callback_data="b:list")
    kb.button(text="⏫ Поднять сейчас", callback_data="b:bumpnow")
    kb.button(text="🏠 Меню", callback_data="go:menu")
    kb.adjust(2,2,1)
    return kb.as_markup()

@rt.callback_query(F.data == "act:autobump")
async def act_autobump(cb: CallbackQuery, state: FSMContext):
    if not await guard(cb): return await state.clear()
    cfg = _load(BUMPS_FILE, {"threads":[]})
    lines = []
    for th in cfg.get("threads", []):
        tid = th["thread_id"]; iv = th.get("interval_min",10); last = th.get("last_bump_ts",0)
        lines.append(f"• #{tid} каждые {iv} мин • последний: { _ts(last) if last else '—' }")
    text = "📌 <b>Автоподнятие</b>\n" + ("\n".join(lines) if lines else "Пока нет тем.")
    try:
        await cb.message.edit_text(text, reply_markup=kb_bumps_menu())
    except TelegramBadRequest:
        await cb.message.answer(text, reply_markup=kb_bumps_menu())
    await cb.answer()

@rt.callback_query(F.data == "b:list")
async def b_list(cb: CallbackQuery):
    if not await guard(cb): return
    cfg = _load(BUMPS_FILE, {"threads":[]})
    lines = []
    for th in cfg.get("threads", []):
        tid = th["thread_id"]; iv = th.get("interval_min",10); last = th.get("last_bump_ts",0)
        lines.append(f"• #{tid} каждые {iv} мин • последний: { _ts(last) if last else '—' }")
    await cb.message.answer("📜 <b>Список</b>\n" + ("\n".join(lines) if lines else "Пусто"))
    await cb.answer()

@rt.callback_query(F.data == "b:add")
async def b_add(cb: CallbackQuery, state: FSMContext):
    if not await guard(cb): return
    await state.set_state(BumpState.add)
    await cb.message.answer("🔗 Пришли ссылку на тему или ID (пример: <code>9070000 15</code> — каждые 15 мин).", reply_markup=kb_form())
    await cb.answer()

@rt.message(BumpState.add)
async def b_add_msg(m: Message, state: FSMContext):
    if not await guard(m): return
    parts = (m.text or "").strip().split()
    tid = parse_thread_id(parts[0]) if parts else None
    if not tid:
        await m.reply("⚠️ Дай ссылку/ID темы.", reply_markup=kb_form()); return

    interval = 10
    if len(parts) >= 2 and parts[1].isdigit():
        interval = max(5, int(parts[1]))

    cfg = _load(BUMPS_FILE, {"threads":[]})
    arr = cfg.get("threads", [])

    for th in arr:
        if int(th["thread_id"]) == int(tid):
            th["interval_min"] = interval
            now_ts = int(time.time())
            th["next_bump_ts"] = now_ts + interval * 60
            _save(BUMPS_FILE, cfg)
            await m.answer(f"✅ Обновил тему #{tid}: каждые {interval} мин.", reply_markup=kb_bumps_menu())
            return

    resp = thread_bump(int(tid))
    now_ts = int(time.time())
    if resp["ok"]:
        last_ts = now_ts
        next_ts = now_ts + interval * 60
        msg = "✅ Добавил тему и сразу поднял."
    else:
        last_ts = 0
        next_ts = now_ts + 60 
        msg = f"✅ Добавил тему; авто начнётся по расписанию (err {resp.get('status')})."

    arr.append({
        "thread_id": int(tid),
        "interval_min": interval,
        "last_bump_ts": last_ts,
        "next_bump_ts": next_ts
    })
    cfg["threads"] = arr
    _save(BUMPS_FILE, cfg)

    await m.answer(f"{msg} #{tid}: каждые {interval} мин.", reply_markup=kb_bumps_menu())
    await state.set_state(BumpState.menu)


@rt.callback_query(F.data == "b:del")
async def b_del(cb: CallbackQuery, state: FSMContext):
    if not await guard(cb): return
    await state.set_state(BumpState.del_)
    await cb.message.answer("🗑 Пришли ID темы для удаления:", reply_markup=kb_form()); await cb.answer()

@rt.message(BumpState.del_)
async def b_del_msg(m: Message, state: FSMContext):
    if not await guard(m): return
    tid = parse_thread_id(m.text or "")
    if not tid:
        await m.reply("⚠️ Нужен ID темы.", reply_markup=kb_form()); return
    cfg = _load(BUMPS_FILE, {"threads":[]})
    before = len(cfg.get("threads", []))
    cfg["threads"] = [x for x in cfg.get("threads", []) if int(x["thread_id"]) != int(tid)]
    _save(BUMPS_FILE, cfg)
    after = len(cfg["threads"])
    await m.answer("✅ Удалено." if after < before else "⚠️ Не найдено.", reply_markup=kb_bumps_menu())
    await state.set_state(BumpState.menu)

@rt.callback_query(F.data == "b:bumpnow")
async def b_bumpnow(cb: CallbackQuery):
    if not await guard(cb): return
    cfg = _load(BUMPS_FILE, {"threads":[]})
    threads = cfg.get("threads", [])
    if not threads:
        await cb.message.answer("Пока нет тем."); await cb.answer(); return

    results = []
    now_ts = int(time.time())

    for th in threads:
        tid = int(th["thread_id"])
        resp = thread_bump(tid)
        if resp["ok"]:
            th["last_bump_ts"] = now_ts
            iv = max(5, int(th.get("interval_min", 10)))
            th["next_bump_ts"] = now_ts + iv * 60
            results.append(f"⏫ #{tid} — ok")
        else:
            results.append(f"⏫ #{tid} — err {resp.get('status')}")

    cfg["threads"] = threads
    _save(BUMPS_FILE, cfg)

    await cb.message.answer("\n".join(results))
    await cb.answer()



def fmt_err(title: str, resp: Dict[str, Any]) -> str:
    status = resp.get('status'); human = ""
    try:
        obj = resp.get("error", {})
        if isinstance(obj, dict):
            arr = obj.get("errors") or obj.get("error") or obj.get("message")
            if isinstance(arr, list) and arr:
                human = str(arr[0])
            elif isinstance(arr, str):
                human = arr
        elif isinstance(obj, str):
            human = obj
    except Exception:
        pass
    hints = []
    if status == 401:
        hints.append("Токен не подошёл (401).")
    if status == 403 and "систем" in (human or "").lower():
        hints.append("Проверь выбранный payment_system/code и формат кошелька.")
    if status == 403 and "холдом" in (human or "").lower():
        hints.append("Холд возможен только для сумм > 10 ₽.")
    body = json.dumps(resp.get("error", resp), ensure_ascii=False, indent=2)[:3500]
    tip = ("\n".join("• " + h for h in hints) + ("\n" if hints else ""))
    return f"⚠️ <b>{title} — ошибка ({status})</b>\n{tip}<b>json</b>\n<pre>{body}</pre>"

async def notif_poller():
    await asyncio.sleep(2)
    while True:
        try:
            s = get_settings()
            if s.get("push_cards_enabled", True):
                resp = forum_notifications(limit=10)
                if resp["ok"]:
                    arr = resp["data"].get("notifications", [])[:10]
                    arr = sorted(arr, key=lambda x: x.get("notification_create_date", 0))
                    last_key = s.get("last_notif_key", "")
                    new_items = []
                    if not last_key and arr:
                        s["last_notif_key"] = _hash_notif(arr[-1]); _save(SETTINGS_FILE, s)
                    else:
                        seen = False
                        for it in arr:
                            if _hash_notif(it) == last_key:
                                new_items = []; seen = True
                            else:
                                new_items.append(it)
                        if not seen:
                            new_items = arr

                    allowed = set()
                    if s["notify_comments"]: allowed.add("comment")
                    if s["notify_mentions"]: allowed.add("mention")
                    if s["notify_likes"]: allowed.add("like")
                    if s["notify_payment_in"]: allowed.add("payment_in")
                    if s["notify_hold_released"]: allowed.add("hold_released")
                    if s["notify_profile_post"]: allowed.add("profile_post")
                    if s["notify_profile_comment"]: allowed.add("profile_comment")
                    if s["notify_payment_in"]:
                        allowed.update({"transfer_in", "transfer_in_hold"})

                    for it in new_items:
                        cid = it.get("notification_id")
                        content = None
                        if cid:
                            c_resp = forum_notification_content(int(cid))
                            if c_resp.get("ok"):
                                content = c_resp["data"]
                        parsed = parse_notif(it.get("notification_html", "") or "", content)
                        if (parsed.get("type") or "other") not in allowed:
                            continue
                        text, kb = render_notif_line(it, content)
                        if not text.strip():
                            continue
                        try:
                            await bot.send_message(ADMIN_USER_ID, text, reply_markup=kb, disable_web_page_preview=True)
                        except Exception:
                            pass
                    if arr:
                        s["last_notif_key"] = _hash_notif(arr[-1]); _save(SETTINGS_FILE, s)
            await asyncio.sleep(20)
        except asyncio.CancelledError:
            break
        except Exception:
            await asyncio.sleep(20)

BUMP_TICK_SEC = 30          
BUMP_JITTER_SEC = (7, 25)   

async def autobump_worker():
    await asyncio.sleep(2)
    while True:
        try:
            cfg = _load(BUMPS_FILE, {"threads": []})
            threads = cfg.get("threads", [])
            if not isinstance(threads, list):
                threads = []

            now = int(time.time())
            changed = False
            results = []

            for th in threads:
                try:
                    tid = int(th.get("thread_id"))
                except Exception:
                    continue
                iv_min = max(5, int(th.get("interval_min", 10))) 
                last_ts = int(th.get("last_bump_ts", 0))
                next_ts = int(th.get("next_bump_ts", 0))

                if next_ts <= 0:
                    next_ts = last_ts + iv_min * 60 if last_ts else now

                if now < next_ts:
                    continue

                resp = thread_bump(tid)
                if resp.get("ok"):
                    th["last_bump_ts"] = now
                    jitter = random.randint(*BUMP_JITTER_SEC)
                    th["next_bump_ts"] = now + iv_min * 60 + jitter
                    results.append(f"#{tid}: ok")
                else:
                    status = int(resp.get("status", 0) or 0)
                    backoff = min(iv_min * 60, 300) if status in (403, 429) else 60
                    th["next_bump_ts"] = now + backoff
                    results.append(f"#{tid}: err {status}")
                changed = True

            if changed:
                cfg["threads"] = threads
                _save(BUMPS_FILE, cfg)

            if results:
                try:
                    await bot.send_message(ADMIN_USER_ID, "⏫ Автоподнятие:\n" + "\n".join(results))
                except Exception:
                    pass

            await asyncio.sleep(BUMP_TICK_SEC)
        except asyncio.CancelledError:
            break
        except Exception:
            await asyncio.sleep(BUMP_TICK_SEC)


async def main():
    asyncio.create_task(notif_poller())
    asyncio.create_task(autobump_worker())  
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
