"""Автозагрузка объявлений о работе из Telegram и KakaoTalk в ленту JobBox.

Запускается по расписанию в GitHub Actions (.github/workflows/feed-ingest.yml),
можно и вручную. Telegram — только чтение под аккаунтом владельца.

Порядок для каждого нового сообщения:
  1. Нет телефона → пропуск без вызова модели.
  2. Тот же текст уже разбирали (другая группа / повторный пост) → поднять
     те же карточки в ленте (не чаще раза в час), модель не вызываем.
  3. Иначе модель OpenAI разбирает сообщение по правилам (prompt.md): пропустить,
     новая карточка или обновить существующую (кандидаты — карточки с тем же
     телефоном из базы) → запись через RPC ingest_upsert_ad.
  4. Журнал feed_ingest_log, состояние feed_ingest_state, отчёт в Telegram.

KakaoTalk: телефон владельца (tool/kakao_collector в jobboxv3) кладёт
сообщения открытого чата в таблицу kakao_inbox. Источник в sources.json —
запись с ключом "kakao"; message_id = KAKAO_ID_BASE + id строки очереди.
Kakao обрезает текст в уведомлении до 500 знаков (truncated): телефон часто
отрезан, поэтому такой текст сравниваем с НАЧАЛОМ прежних полных постов.

Переменные окружения:
  TG_API_ID, TG_API_HASH, TG_SESSION   — Telegram (StringSession, см. export_session.py)
  OPENAI_API_KEY                       — тот же ключ, что у parse-job-terms
  SUPABASE_URL, SUPABASE_SERVICE_KEY   — база (сервисный ключ: RPC доступны только ему)
  OPENAI_MODEL     (по умолчанию gpt-4.1-mini)
  INGEST_MAX       — не больше N разборов моделью за запуск (по умолчанию 120)
  INGEST_DRY_RUN=1 — ничего не писать в базу и не слать отчёт (проверка)
  INGEST_REPORT_TO — кому отчёт (по умолчанию «me» — Избранное владельца)
  INGEST_ONLY=kakao — только очередь Kakao, без Telegram (проверка)
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from difflib import SequenceMatcher
from pathlib import Path
from typing import Literal, Optional

import httpx
from pydantic import BaseModel, Field
from telethon import TelegramClient
from telethon.sessions import StringSession

HERE = Path(__file__).parent
KST = timezone(timedelta(hours=9))
PHONE_RE = re.compile(r'(?:\+?82[\s\-.]*|0)1\s*0[\s\-.]*\d{3,4}[\s\-.]*\d{4}')
BUMP_EVERY = timedelta(hours=1)
SAME_TEXT_WINDOW = timedelta(days=3)
# Повторный пост с правками: совпадение текста с прежним постом того же
# телефона не меньше 85% за последнюю неделю — поднимаем без модели.
SIMILAR_MIN = 0.85
SIMILAR_WINDOW = timedelta(days=7)
# Обрезанный уведомлением текст Kakao: совпадение с началом прежнего поста.
PREFIX_MIN = 0.9
# Номера сообщений Kakao в feed_ingest_log: ручные выгрузки заняли 0–2xxx.
KAKAO_ID_BASE = 100_000

DRY = os.environ.get('INGEST_DRY_RUN') == '1'
ONLY = os.environ.get('INGEST_ONLY') or ''
MODEL = os.environ.get('OPENAI_MODEL') or 'gpt-4.1-mini'
MAX_PARSE = int(os.environ.get('INGEST_MAX') or '120')


# ---------- что возвращает модель ----------

City = Literal['ansan', 'hwaseong', 'incheon', 'yongin', 'pyeongtaek', 'other']


class Contact(BaseModel):
    phone: str = Field(description='Только цифры, 010XXXXXXXX')
    name: str
    note: Optional[str] = None


class Ad(BaseModel):
    title: str
    city: City
    place: str
    work: str
    schedule: str
    pay: str
    audience: Literal['women', 'men', 'any']
    age: Optional[str] = None
    visas: Optional[str] = None
    transport: Optional[str] = None
    conditions: Optional[str] = None
    rhythm: Literal['daily', 'weekly', 'monthly', 'mixed', 'delayed', 'unspecified']
    has_shuttle: bool
    needs_car: bool
    urgent: bool
    hourly_rate: Optional[int] = Field(
        default=None, description='Ставка за час основного времени, вон; null — не указана')
    shifts: list[Literal['day', 'night']] = Field(
        default_factory=list, description='Смены: day, night, обе; пусто — не указано')
    job_kind: Literal['factory', 'warehouse', 'construction', 'kitchen', 'beauty',
                      'driver', 'cleaning', 'care', 'office', 'service', 'other'] = Field(
        default='factory', description='Вид работы — фильтр ленты')
    contacts: list[Contact]


class Item(BaseModel):
    action: Literal['new', 'update']
    update_slug: Optional[str] = Field(
        default=None, description='Для update — slug из списка существующих карточек')
    slug_hint: str = Field(description='Латиница kebab-case: имя-место-суть, для new')
    ad: Ad


class Decision(BaseModel):
    skip_reason: Optional[str] = Field(
        default=None, description='Если items пуст — почему пропущено')
    items: list[Item]


# ---------- Supabase (REST, сервисный ключ) ----------

class Db:
    def __init__(self) -> None:
        url = os.environ['SUPABASE_URL'].rstrip('/')
        key = os.environ['SUPABASE_SERVICE_KEY']
        headers = {'apikey': key, 'Content-Type': 'application/json'}
        # Новый секретный ключ (sb_secret_…) — только в apikey: он не JWT.
        # Старый service_role (JWT) — ещё и в Authorization.
        if not key.startswith('sb_'):
            headers['Authorization'] = f'Bearer {key}'
        self.http = httpx.Client(base_url=f'{url}/rest/v1', headers=headers, timeout=30)

    def rpc(self, fn: str, args: dict):
        r = self.http.post(f'/rpc/{fn}', json=args)
        r.raise_for_status()
        return r.json()

    def state(self) -> dict[str, int]:
        r = self.http.get('/feed_ingest_state', params={'select': 'source,last_message_id'})
        r.raise_for_status()
        return {row['source']: row['last_message_id'] for row in r.json()}

    def save_state(self, source: str, last_id: int) -> None:
        if DRY:
            return
        r = self.http.post('/feed_ingest_state',
                           params={'on_conflict': 'source'},
                           headers={'Prefer': 'resolution=merge-duplicates'},
                           json={'source': source, 'last_message_id': last_id,
                                 'updated_at': datetime.now(timezone.utc).isoformat()})
        r.raise_for_status()

    def seen(self, text_hash: str) -> Optional[dict]:
        since = (datetime.now(timezone.utc) - SAME_TEXT_WINDOW).isoformat()
        r = self.http.get('/feed_ingest_log', params={
            'select': 'decision,slugs,created_at',
            'text_hash': f'eq.{text_hash}',
            'decision': 'in.(new,update,bump,skip)',
            'created_at': f'gte.{since}',
            'order': 'created_at.desc', 'limit': '1'})
        r.raise_for_status()
        rows = r.json()
        return rows[0] if rows else None

    def similar(self, phones: list[str], norm: str, prefix: bool = False) -> Optional[dict]:
        """Самый похожий прежний пост с тем же телефоном (за неделю), если
        совпадает не меньше чем на SIMILAR_MIN. Возвращает строку журнала + ratio.
        prefix — текст обрезан: сравнивать с началом прежнего поста."""
        since = (datetime.now(timezone.utc) - SIMILAR_WINDOW).isoformat()
        r = self.http.get('/feed_ingest_log', params={
            'select': 'decision,slugs,text_norm',
            'phones': f'ov.{{{",".join(phones)}}}',
            'decision': 'in.(new,update,bump)',
            'text_norm': 'not.is.null',
            'created_at': f'gte.{since}',
            'order': 'created_at.desc', 'limit': '40'})
        r.raise_for_status()
        return best_match(r.json(), norm, prefix, PREFIX_MIN if prefix else SIMILAR_MIN)

    def prefix_match(self, source: str, norm: str) -> Optional[dict]:
        """Обрезанный текст без телефона: прежний пост того же источника
        (за неделю), начало которого совпадает не меньше чем на PREFIX_MIN."""
        since = (datetime.now(timezone.utc) - SIMILAR_WINDOW).isoformat()
        r = self.http.get('/feed_ingest_log', params={
            'select': 'decision,slugs,text_norm',
            'source': f'eq.{source}',
            'decision': 'in.(new,update,bump)',
            'text_norm': 'not.is.null',
            'created_at': f'gte.{since}',
            'order': 'created_at.desc', 'limit': '500'})
        r.raise_for_status()
        return best_match(r.json(), norm, True, PREFIX_MIN)

    def kakao_inbox(self, source: str, after_id: int) -> list[dict]:
        r = self.http.get('/kakao_inbox', params={
            'select': 'id,sender,posted_at,text,truncated',
            'source': f'eq.{source}', 'id': f'gt.{after_id}',
            'order': 'id', 'limit': '300'})
        r.raise_for_status()
        return r.json()

    def last_bump(self, slugs: list[str]) -> Optional[datetime]:
        r = self.http.get('/feed_ingest_log', params={
            'select': 'created_at', 'slugs': f'ov.{{{",".join(slugs)}}}',
            'decision': 'in.(new,update,bump)',
            'order': 'created_at.desc', 'limit': '1'})
        r.raise_for_status()
        rows = r.json()
        return datetime.fromisoformat(rows[0]['created_at']) if rows else None

    def log(self, **row) -> None:
        if DRY:
            return
        r = self.http.post('/feed_ingest_log',
                           params={'on_conflict': 'source,message_id'},
                           headers={'Prefer': 'resolution=ignore-duplicates'},
                           json=row)
        r.raise_for_status()

    def active_ads(self) -> Optional[int]:
        """Сколько объявлений сейчас в ленте (активных, не вакансий найма)."""
        try:
            r = self.http.head('/job_listings', params={
                'select': 'id', 'is_active': 'eq.true', 'in_contour': 'is.false'},
                headers={'Prefer': 'count=exact', 'Range': '0-0'})
            total = r.headers.get('content-range', '').rsplit('/', 1)[-1]
            return int(total) if total.isdigit() else None
        except Exception:
            return None

    def slug_taken(self, slug: str) -> bool:
        r = self.http.get('/job_listings', params={'select': 'slug', 'slug': f'eq.{slug}'})
        r.raise_for_status()
        return bool(r.json())


def best_match(rows: list[dict], norm: str, prefix: bool, need: float) -> Optional[dict]:
    """Лучшее совпадение текста со строками журнала; prefix — с их началом."""
    if prefix and len(norm) < 80:
        return None  # слишком мало текста для сравнения
    best = None
    for row in rows:
        if not row['slugs']:
            continue
        other = row['text_norm'][:len(norm)] if prefix else row['text_norm']
        ratio = SequenceMatcher(None, norm, other, autojunk=False).ratio()
        if ratio >= need and (best is None or ratio > best['ratio']):
            best = {**row, 'ratio': ratio}
    return best


# ---------- разбор ----------

def phones_of(text: str) -> list[str]:
    out = []
    for m in PHONE_RE.findall(text):
        d = re.sub(r'\D', '', m)
        if d.startswith('82'):
            d = '0' + d[2:]
        if re.fullmatch(r'01\d{8,9}', d) and d not in out:
            out.append(d)
    return out


def norm_text(text: str) -> str:
    """Текст для сравнения похожих постов: строчные буквы и цифры, до 2000 знаков."""
    return re.sub(r'[\W_]+', ' ', text.lower()).strip()[:2000]


def text_hash(text: str) -> str:
    norm = re.sub(r'[\W_]+', '', text.lower())
    return hashlib.sha1(norm.encode()).hexdigest()


# Города вне нашего региона (prompt.md, «Далёкий регион»). Карточка с таким
# местом не попадает в ленту, даже если модель её пропустила. Если в месте
# есть и наш город («Сеул и Инчхон», «развоз из Ансана») — оставляем.
FAR_RE = re.compile(
    r'Пусан|Кимх[эе]|Чиньён|Кодже|Ульсан|Янсан|Тэгу|Кёнсан|Куми|Кёнджу|Пхохан'
    r'|Мокпо|Синан|Х[эе]нам|Ёсу|Кванъян|Наджу|Кванджу|Сунчхон|Каннын|Донхе'
    r'|Тонхе|Самчхок|Сокчхо|Вонджу|Чхунджу|Чечхон|Намъянджу|Намьянджу|Ыйджонбу'
    r'|Пхочхон|Пачжу|Паджу|Сеул|Ичхон|Ёджу|Чеджу|Сосан|Танджин|Тхэан|Иксан'
    r'|Кунсан|Чонджу|Ёнджу|Андон|Ульджин|Чхунчхон', re.I)
OURS_RE = re.compile(
    r'Ансан|Шива|Сихын|Хвасон|Хвасонг|Инчхон|Инчон|Кимпо|Кимпхо|Пучхон|Сувон'
    r'|Ёнин|Йонин|Пх?ёнт[эе]?к|Осан|Посын|Паран|Ансон|Нери|Чхонан|Чонан|Асан'
    r'|Чхонджу|Тэджон|Ымсон|Кунпхо|Кунпо|Анян|Ыйван', re.I)


def is_far(place: str, title: str = '') -> bool:
    s = f'{place} {title}'
    return bool(FAR_RE.search(s)) and not OURS_RE.search(s)


def slugify(hint: str, h: str, prefix: str = 'tg') -> str:
    s = re.sub(r'[^a-z0-9]+', '-', hint.lower()).strip('-')[:48] or 'ad'
    return f'{prefix}-{s}-{h[:6]}'


def _nullable(t: str) -> dict:
    return {'type': [t, 'null']}


# Строгая JSON-схема ответа (structured outputs OpenAI: все поля в required,
# необязательные — через null). Совпадает с моделью Decision выше.
DECISION_SCHEMA = {
    'type': 'object', 'additionalProperties': False,
    'required': ['skip_reason', 'items'],
    'properties': {
        'skip_reason': _nullable('string'),
        'items': {'type': 'array', 'items': {
            'type': 'object', 'additionalProperties': False,
            'required': ['action', 'update_slug', 'slug_hint', 'ad'],
            'properties': {
                'action': {'type': 'string', 'enum': ['new', 'update']},
                'update_slug': _nullable('string'),
                'slug_hint': {'type': 'string'},
                'ad': {
                    'type': 'object', 'additionalProperties': False,
                    'required': ['title', 'city', 'place', 'work', 'schedule', 'pay',
                                 'audience', 'age', 'visas', 'transport', 'conditions',
                                 'rhythm', 'has_shuttle', 'needs_car', 'urgent',
                                 'hourly_rate', 'shifts', 'job_kind', 'contacts'],
                    'properties': {
                        'title': {'type': 'string'},
                        'city': {'type': 'string', 'enum': ['ansan', 'hwaseong', 'incheon',
                                                            'yongin', 'pyeongtaek', 'other']},
                        'place': {'type': 'string'},
                        'work': {'type': 'string'},
                        'schedule': {'type': 'string'},
                        'pay': {'type': 'string'},
                        'audience': {'type': 'string', 'enum': ['women', 'men', 'any']},
                        'age': _nullable('string'),
                        'visas': _nullable('string'),
                        'transport': _nullable('string'),
                        'conditions': _nullable('string'),
                        'rhythm': {'type': 'string', 'enum': ['daily', 'weekly', 'monthly',
                                                              'mixed', 'delayed', 'unspecified']},
                        'has_shuttle': {'type': 'boolean'},
                        'needs_car': {'type': 'boolean'},
                        'urgent': {'type': 'boolean'},
                        'hourly_rate': _nullable('integer'),
                        'shifts': {'type': 'array', 'items': {
                            'type': 'string', 'enum': ['day', 'night']}},
                        'job_kind': {'type': 'string', 'enum': ['factory', 'warehouse', 'construction', 'kitchen', 'beauty', 'driver', 'cleaning', 'care', 'office', 'service', 'other']},
                        'contacts': {'type': 'array', 'items': {
                            'type': 'object', 'additionalProperties': False,
                            'required': ['phone', 'name', 'note'],
                            'properties': {'phone': {'type': 'string'},
                                           'name': {'type': 'string'},
                                           'note': _nullable('string')}}},
                    },
                },
            },
        }},
    },
}


class Parser:
    """Разбор через OpenAI Responses API — тот же ключ и тот же вид запроса,
    что у функции supabase/functions/parse-job-terms."""

    def __init__(self) -> None:
        self.http = httpx.Client(
            base_url='https://api.openai.com/v1',
            headers={'Authorization': f'Bearer {os.environ["OPENAI_API_KEY"]}',
                     'Content-Type': 'application/json'},
            timeout=120)
        self.instructions = (HERE / 'prompt.md').read_text(encoding='utf-8')
        self.calls = 0
        self.tokens_in = 0
        self.tokens_out = 0

    def decide(self, msg_text: str, sender: str, when: datetime,
               candidates: list[dict]) -> Decision:
        cand = '\n'.join(
            f'- slug={c["slug"]} | {c["title"]} | {c.get("place") or ""} | '
            f'{(c.get("work_description") or "")[:160]} | '
            f'{"активна" if c["is_active"] else "снята"}'
            for c in candidates) or '(нет)'
        now = datetime.now(KST)
        user = (
            f'Сейчас: {now:%d.%m.%Y %H:%M} по Корее.\n'
            f'Сообщение от «{sender}», {when.astimezone(KST):%d.%m.%Y %H:%M}:\n'
            f'<message>\n{msg_text}\n</message>\n\n'
            f'Существующие карточки с этими телефонами:\n{cand}')
        r = self.http.post('/responses', json={
            'model': MODEL,
            'instructions': self.instructions,
            'input': user,
            'temperature': 0,
            'text': {'format': {'type': 'json_schema', 'name': 'job_ad_decision',
                                'strict': True, 'schema': DECISION_SCHEMA}},
        })
        if r.status_code != 200:
            raise RuntimeError(f'OpenAI {r.status_code}: {r.text[:300]}')
        data = r.json()
        self.calls += 1
        usage = data.get('usage') or {}
        self.tokens_in += usage.get('input_tokens', 0)
        self.tokens_out += usage.get('output_tokens', 0)
        for item in data.get('output') or []:
            for part in item.get('content') or []:
                if part.get('type') == 'refusal':
                    return Decision(skip_reason='модель отказалась разбирать', items=[])
                if part.get('type') == 'output_text':
                    return Decision.model_validate_json(part['text'])
        raise RuntimeError('OpenAI: пустой ответ')


def to_row(item: Item, slug: str) -> dict:
    a = item.ad
    contacts = []
    for c in a.contacts:
        d = re.sub(r'\D', '', c.phone)
        if re.fullmatch(r'01\d{8,9}', d):
            contacts.append({k: v for k, v in
                             {'phone': d, 'name': c.name, 'note': c.note}.items() if v})
    if not contacts:
        raise ValueError('нет корректного телефона')
    return {
        'slug': slug, 'title': a.title, 'city': a.city, 'place': a.place,
        'work': a.work, 'schedule': a.schedule, 'pay': a.pay,
        'audience': a.audience, 'age': a.age, 'visas': a.visas,
        'transport': a.transport, 'conditions': a.conditions, 'rhythm': a.rhythm,
        'has_shuttle': a.has_shuttle, 'needs_car': a.needs_car, 'urgent': a.urgent,
        # Ставка и смена — для фильтров ленты; неправдоподобную ставку
        # ingest_upsert_ad запишет как не указанную.
        'hourly_rate': a.hourly_rate,
        'shifts': sorted(set(a.shifts)),
        # Вид работы — фильтр «Вид работы» / «Не завод» в ленте.
        'job_kind': a.job_kind,
        'contacts': contacts,
        'posted_at': datetime.now(timezone.utc).isoformat(),
    }


# ---------- источники ----------

@dataclass
class Msg:
    key: str            # источник: чат Telegram (/тема) или kakao/…
    id: int             # message_id в feed_ingest_log
    date: datetime
    text: str
    sender: str
    truncated: bool = False

    @property
    def slug_prefix(self) -> str:
        return 'kk' if self.key.startswith('kakao/') else 'tg'


def fetch_kakao(db: 'Db', sources, state) -> list[Msg]:
    """Неразобранные строки очереди kakao_inbox (с телефона владельца)."""
    out = []
    for src in sources:
        if not src.get('kakao'):
            continue
        key = src['source']
        last = state.get(key, 0)
        rows = db.kakao_inbox(key, max(last - KAKAO_ID_BASE, 0))
        for row in rows:
            out.append(Msg(key, KAKAO_ID_BASE + row['id'],
                           datetime.fromisoformat(row['posted_at']),
                           (row['text'] or '').strip(), row['sender'] or 'Без имени',
                           bool(row['truncated'])))
        state[key] = max([last] + [KAKAO_ID_BASE + row['id'] for row in rows])
    return out


# ---------- Telegram ----------

def sender_name(msg) -> str:
    if getattr(msg, 'post_author', None):
        return msg.post_author
    s = msg.sender
    if s is None:
        return 'Без имени'
    name = ' '.join(x for x in [getattr(s, 'first_name', None), getattr(s, 'last_name', None)] if x)
    return name or getattr(s, 'title', None) or getattr(s, 'username', None) or 'Без имени'


async def fetch(tg, sources, state) -> list[Msg]:
    """Новые сообщения всех чатов Telegram."""
    out = []
    since = datetime.now(timezone.utc) - timedelta(hours=2)
    for src in sources:
        if src.get('kakao'):
            continue
        key = src['chat'] + (f'/{src["topic_id"]}' if src.get('topic_id') else '')
        entity = await tg.get_entity(src['chat'])
        last = state.get(key, 0)
        kw = {'reply_to': int(src['topic_id'])} if src.get('topic_id') else {}
        msgs = []
        async for m in tg.iter_messages(entity, min_id=last, limit=300, **kw):
            if not last and m.date < since:
                break
            if not m.action:
                msgs.append(m)
        for m in msgs:
            out.append(Msg(key, m.id, m.date, (m.message or '').strip(), sender_name(m)))
        state[key] = max([last] + [m.id for m in msgs])
    return out


# ---------- главный проход ----------

async def run() -> int:
    sources = json.loads((HERE / 'sources.json').read_text(encoding='utf-8'))
    db = Db()
    parser = Parser()
    state = db.state()
    start_state = dict(state)

    tg = None
    tg_broken = False
    if ONLY != 'kakao':
        tg = TelegramClient(StringSession(os.environ['TG_SESSION']),
                            int(os.environ['TG_API_ID']), os.environ['TG_API_HASH'],
                            device_model='JobBox ingest', app_version='1.0')
        await tg.connect()
        if not await tg.is_user_authorized():
            # Kakao разбираем всё равно — Telegram для него не нужен.
            print('Telegram: сессия недействительна — нужен новый TG_SESSION', file=sys.stderr)
            await tg.disconnect()
            tg, tg_broken = None, True

    stats = {'messages': 0, 'no_phone': 0, 'duplicate': 0, 'bump': 0,
             'skip': 0, 'new': 0, 'update': 0, 'error': 0}
    new_titles, upd_titles, skipped, errors = [], [], [], []
    done_ids: dict[str, int] = {}

    try:
        batch = (await fetch(tg, sources, state)) if tg else []
        batch += fetch_kakao(db, sources, state)
        batch.sort(key=lambda x: x.date)
        for m in batch:
            key = m.key
            stats['messages'] += 1
            text = m.text
            h = text_hash(text) if text else 'empty'
            base = {'source': key, 'message_id': m.id,
                    'posted_at': m.date.isoformat(), 'text_hash': h}
            phones = phones_of(text)
            cut = '; текст обрезан уведомлением' if m.truncated else ''
            if not phones and not m.truncated:
                stats['no_phone'] += 1
                db.log(**base, decision='skip', note='нет телефона')
                done_ids[key] = max(done_ids.get(key, 0), m.id)
                continue

            norm = norm_text(text)
            base.update(phones=phones, text_norm=norm)
            # Тот же текст — или почти тот же пост с тем же телефоном
            # (рекрутер поменял время, «срочно», цену): поднять, без модели.
            # Обрезанный текст Kakao — сравнить с началом прежних постов.
            prev = db.seen(h)
            similar = None
            if not prev and phones:
                prev = similar = db.similar(phones, norm, prefix=m.truncated)
            if not prev and m.truncated:
                prev = similar = db.prefix_match(key, norm)
            if not prev and not phones:
                stats['no_phone'] += 1
                db.log(**base, decision='skip', note='нет телефона' + cut)
                done_ids[key] = max(done_ids.get(key, 0), m.id)
                continue
            if prev:
                why = (f'похоже на прежний пост на {round(similar["ratio"] * 100)}%{cut}'
                       if similar else None)
                if prev['decision'] == 'skip' or not prev['slugs']:
                    stats['duplicate'] += 1
                    db.log(**base, decision='duplicate', note='тот же текст пропущен ранее')
                else:
                    last = db.last_bump(prev['slugs'])
                    if last and datetime.now(timezone.utc) - last < BUMP_EVERY:
                        stats['duplicate'] += 1
                        db.log(**base, decision='duplicate', slugs=prev['slugs'],
                               note=f'поднято меньше часа назад{"; " + why if why else ""}')
                    else:
                        if not DRY:
                            db.rpc('ingest_bump', {'p_slugs': prev['slugs']})
                        stats['bump'] += 1
                        db.log(**base, decision='bump', slugs=prev['slugs'], note=why)
                done_ids[key] = max(done_ids.get(key, 0), m.id)
                continue

            if parser.calls >= MAX_PARSE:
                break  # остальное — в следующий запуск (состояние не двигаем)

            try:
                cands = db.rpc('ingest_find_by_phones', {'p_phones': phones})
                dec = parser.decide(text, m.sender, m.date, cands)
                # Далёкие города отсекаем сами: модель иногда пропускает их.
                far = [it for it in dec.items if is_far(it.ad.place, it.ad.title)]
                if far:
                    dec.items = [it for it in dec.items if it not in far]
                    if not dec.items:
                        dec.skip_reason = f'далёкий регион: {far[0].ad.place}'[:200]
                if not dec.items:
                    stats['skip'] += 1
                    skipped.append(dec.skip_reason or '—')
                    db.log(**base, decision='skip', note=dec.skip_reason)
                else:
                    slugs, kinds = [], set()
                    known = {c['slug'] for c in cands}
                    for it in dec.items:
                        if it.action == 'update' and it.update_slug in known:
                            slug = it.update_slug
                            kinds.add('update')
                            upd_titles.append(it.ad.title)
                        else:
                            slug = slugify(it.slug_hint, h, m.slug_prefix)
                            n = 2
                            while slug in slugs or (not DRY and db.slug_taken(slug)):
                                slug = f'{slugify(it.slug_hint, h, m.slug_prefix)}-{n}'
                                n += 1
                            kinds.add('new')
                            new_titles.append(f'{it.ad.title} ({it.ad.place})')
                        row = to_row(it, slug)
                        if not DRY:
                            db.rpc('ingest_upsert_ad', {'p': row})
                        slugs.append(slug)
                    kind = 'new' if 'new' in kinds else 'update'
                    stats[kind] += 1
                    db.log(**base, decision=kind, slugs=slugs, note=cut[2:] or None)
            except Exception as e:  # одно сообщение не должно ронять весь проход
                stats['error'] += 1
                errors.append(f'{key}#{m.id}: {type(e).__name__}: {str(e)[:200]}')
                db.log(**base, decision='error', note=str(e)[:500])
            done_ids[key] = max(done_ids.get(key, 0), m.id)

        # Состояние: до последнего обработанного по каждому источнику.
        for key in state:
            target = done_ids.get(key, start_state.get(key, 0))
            if parser.calls < MAX_PARSE:
                target = state[key]  # всё прочитанное разобрано
            if target != start_state.get(key):
                db.save_state(key, target)

        # Репозиторий публичный: в лог GitHub — только цифры, без текстов.
        # Подробности по каждому сообщению — в базе (feed_ingest_log).
        summary = make_summary(stats, len(new_titles), len(upd_titles), errors,
                               None if DRY else db.active_ads())
        print(summary)
        if parser.calls:
            print(f'OpenAI ({MODEL}): {parser.calls} разборов, '
                  f'токенов {parser.tokens_in}/{parser.tokens_out}')
        if tg and not DRY and (stats['new'] or stats['update'] or stats['error']):
            await tg.send_message(os.environ.get('INGEST_REPORT_TO', 'me'), summary,
                                  silent=not stats['error'])
    finally:
        if tg:
            await tg.disconnect()
    if tg_broken:
        return 2
    return 1 if stats['error'] and not (stats['new'] or stats['update']) else 0


def make_summary(stats, new, updated, errors, active) -> str:
    """Короткий отчёт в Telegram: только цифры."""
    now = datetime.now(KST)
    repeats = updated + stats['bump'] + stats['duplicate']
    lines = [
        f'JobBox {now:%H:%M}',
        f'Сообщений: {stats["messages"]}',
        f'Новых: {new}',
        f'Повторов: {repeats}',
        f'Не работа: {stats["skip"] + stats["no_phone"]}',
    ]
    if active is not None:
        lines.append(f'В ленте: {active}')
    if errors:
        lines.append(f'Ошибок: {len(errors)} — {errors[0][:120]}')
    return '\n'.join(lines)


if __name__ == '__main__':
    sys.stdout.reconfigure(encoding='utf-8')
    sys.exit(asyncio.run(run()))
