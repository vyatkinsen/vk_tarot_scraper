from __future__ import annotations
import json
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, List, Set

import requests
from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import KafkaError
from tqdm import tqdm

load_dotenv()

ACCESS_TOKEN = os.getenv("VK_TOKEN")
VK_API_URL = os.getenv("VK_API_URL")
API_VERSION = os.getenv("API_VERSION")

GROUPS_LIMIT = int(os.getenv("GROUPS_LIMIT"))
PER_REQUEST_API = int(os.getenv("PER_REQUEST_API"))
DELAY = float(os.getenv("DELAY"))

KEYWORDS = [kw.strip() for kw in os.getenv("KEYWORDS", "").split(",") if kw.strip()]
FIELDS = os.getenv("FIELDS", "")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "vk_users")

MAX_KAFKA_MSG_BYTES = int(os.getenv("MAX_KAFKA_MSG_BYTES", "950000"))

PRODUCER = KafkaProducer(
    bootstrap_servers=[s.strip() for s in KAFKA_BOOTSTRAP_SERVERS.split(",") if s.strip()],
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    linger_ms=10,
    retries=5,
)

SESSION = requests.Session()


def vk_request(method: str, params: Dict[str, Any]) -> Dict[str, Any]:
    base = {"access_token": ACCESS_TOKEN, "v": API_VERSION}
    resp = SESSION.get(VK_API_URL + method, params=base | params, timeout=30)
    time.sleep(DELAY)
    data: Dict[str, Any] = resp.json()
    if "error" in data:
        raise RuntimeError(
            f"VK API error {data['error']['error_code']}: {data['error']['error_msg']}"
        )
    return data["response"]


def _chunks(lst: List[Any], size: int) -> Iterator[List[Any]]:
    for i in range(0, len(lst), size):
        yield lst[i : i + size]


def size_limited_chunks(profiles: List[Dict[str, Any]], max_bytes: int) -> Iterator[List[Dict[str, Any]]]:
    buf: List[Dict[str, Any]] = []
    cur_size = 2
    for prof in profiles:
        p_json = json.dumps(prof, ensure_ascii=False)
        p_size = len(p_json.encode()) + 1
        if p_size > max_bytes - 2:
            raise ValueError("Single profile exceeds max message size")
        if cur_size + p_size > max_bytes:
            yield buf
            buf = []
            cur_size = 2
        buf.append(prof)
        cur_size += p_size
    if buf:
        yield buf


def search_groups(limit: int) -> List[int]:
    found: Set[int] = set()
    print("Поиск групп начался")
    for kw in KEYWORDS:
        offset = 0
        while len(found) < limit:
            r = vk_request("groups.search", {"q": kw, "type": "group", "count": 1000, "offset": offset})
            for g in r.get("items", []):
                if g.get("is_closed") == 0:
                    found.add(g["id"])
                    if len(found) >= limit:
                        break
            offset += len(r.get("items", []))
            if offset >= r.get("count", 0):
                break
    print(f"Найдено групп: {len(found)}")
    return list(found)[:limit]


def safe_total(group_id: int) -> int | None:
    try:
        return vk_request("groups.getMembers", {"group_id": group_id, "count": 0})["count"]
    except RuntimeError as e:
        if "error 15" in str(e):
            return None
        raise


def fetch_profiles(group_id: int, total: int) -> List[Dict[str, Any]]:
    profiles: List[Dict[str, Any]] = []
    base: Dict[str, Any] = {"group_id": group_id, "count": PER_REQUEST_API}
    if FIELDS:
        base["fields"] = FIELDS
    with tqdm(total=total, desc=f"Группа {group_id}") as bar:
        for offset in range(0, total, PER_REQUEST_API):
            try:
                resp = vk_request("groups.getMembers", base | {"offset": offset})
                got = resp["items"]
                profiles.extend(got)
                bar.update(len(got))
            except RuntimeError as e:
                if "error 15" in str(e):
                    bar.set_description("⚠️ доступ закрыт")
                    break
                raise
    return profiles


def send_chunks(group_id: int, profiles: List[Dict[str, Any]], ts_iso: str) -> None:
    if not profiles:
        print("⚠️ Нет профилей для отправки")
        return

    chunks = size_limited_chunks(profiles, MAX_KAFKA_MSG_BYTES)
    count = 0
    for idx, part in enumerate(chunks):
        future = PRODUCER.send(
            KAFKA_TOPIC,
            {
                "generated_at": ts_iso,
                "group_id": group_id,
                "chunk_index": idx,
                "profiles": part,
            },
        )
        try:
            future.get(timeout=30)
        except KafkaError as exc:
            print(f"      ✖ chunk {idx}: {exc}")
            raise
        count = idx + 1
    PRODUCER.flush()
    print(f"✅ Отправлено {count} сообщений (всего {len(profiles)} профилей)")


def main() -> None:
    if not ACCESS_TOKEN:
        print("❌ Нет VK access_token", file=sys.stderr)
        sys.exit(1)

    ts_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")

    try:
        groups = search_groups(GROUPS_LIMIT)
        if not groups:
            print("❗️ Группы не найдены — выходим")
            return

        for idx, gid in enumerate(groups, 1):
            print(f"\n[{idx}/{len(groups)}] группа {gid}")
            total = safe_total(gid)
            if total is None:
                print("⚠️ Список участников скрыт — пропуск")
                continue

            print(f"    Всего участников: {total}")
            profiles = fetch_profiles(gid, total)
            print(f"    Получено профилей: {len(profiles)}")

            send_chunks(gid, profiles, ts_iso)

    except KeyboardInterrupt:
        print("\n⏹️  Остановлено пользователем")
    except Exception as exc:
        print("\n❌ Ошибка:", exc, file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
