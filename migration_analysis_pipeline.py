# -*- coding: utf-8 -*-
"""
Версия analysis_pipeline.py для облачного деплоя.
Отличие от оригинала: импортирует из migration_database вместо database.
"""
from __future__ import annotations

import base64
import io
import json
import re
import os
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import requests

try:
    from PIL import Image, UnidentifiedImageError
except ImportError:
    Image = None  # type: ignore[misc, assignment]
    UnidentifiedImageError = ValueError  # type: ignore[misc, assignment]

try:
    import cairosvg as _cairosvg  # type: ignore[import-untyped]
except Exception:
    _cairosvg = None  # type: ignore[assignment]
from rapidfuzz import fuzz, process

from migration_database import (
    get_curriculum_topics,
    get_curriculum_topic,
    get_curriculum_subsections,
    get_topics_by_subsection,
    get_analysis_prompt,
    get_analysis_prompt_versions,
    get_analysis_prompt_version_by_id,
    save_analysis_prompt_version,
    read_default_analysis_prompt_file,
    read_analysis_json_schema_file,
    read_analysis_field_rules_file,
    get_task_rowid,
    clear_task_analysis_links,
    upsert_content_element,
    upsert_skill,
    add_task_content_element,
    add_task_skill_step,
    increment_prerequisite,
    save_task_analysis,
)

USD_PER_MTOK_INPUT = 3.0   # Claude Sonnet direct
USD_PER_MTOK_OUTPUT = 15.0
RUB_PER_USD = 90.0

# Pricing for OpenRouter models: (input $/M, output $/M)
OPENROUTER_MODEL_PRICING: Dict[str, Tuple[float, float]] = {
    "google/gemini-2.5-flash":                     (0.30,  2.50),
    "google/gemini-2.5-flash-lite":                (0.10,  0.40),
    "anthropic/claude-haiku-4.5":                  (1.00,  5.00),
    "deepseek/deepseek-chat-v3.1":                 (0.27,  1.10),
    "qwen/qwen3-235b-a22b-2507":                   (0.14,  0.39),
    "qwen/qwen2.5-vl-72b-instruct":                (0.80,  0.80),
    "qwen/qwen3-vl-235b-a22b-instruct":            (0.20,  0.88),
    "openai/gpt-4o-mini":                          (0.15,  0.60),
    "openai/gpt-4.1-nano":                         (0.10,  0.40),
    "mistralai/mistral-small-3.1-24b-instruct":    (0.05,  0.15),
    "microsoft/phi-4":                             (0.07,  0.14),
    "meta-llama/llama-3.3-70b-instruct":           (0.10,  0.25),
}
# Direct Anthropic models
ANTHROPIC_MODEL_PRICING: Dict[str, Tuple[float, float]] = {
    "claude-sonnet-4-20250514": (3.00, 15.00),
    "claude-sonnet-4-0":        (3.00, 15.00),
    "claude-haiku-4-5":         (0.80,  4.00),
    "claude-opus-4-5":         (15.00, 75.00),
}


def model_pricing(model: str, provider: str = "anthropic") -> Tuple[float, float]:
    """Returns (input_price, output_price) in USD per 1M tokens."""
    if provider == "openrouter":
        return OPENROUTER_MODEL_PRICING.get(model, (1.0, 5.0))
    return ANTHROPIC_MODEL_PRICING.get(model, (USD_PER_MTOK_INPUT, USD_PER_MTOK_OUTPUT))


def _proxies(proxy_url: Optional[str]) -> Optional[dict]:
    url = (proxy_url or "").strip()
    if not url:
        url = os.environ.get("ANTHROPIC_HTTPS_PROXY", "").strip() or os.environ.get(
            "HTTPS_PROXY", ""
        ).strip()
    if not url:
        return None
    return {"http": url, "https": url}


JSON_OUTPUT_STRICT_RU = """ЖЁСТКОЕ ТРЕБОВАНИЕ К ФОРМАТУ (нарушение = неверный ответ):
- В выводе должен быть ТОЛЬКО один валидный JSON-объект: от первой «{» до парной «}».
- ЗАПРЕЩЕНО: любой текст до или после JSON; вступления («Вот JSON», «Результат»); markdown-заголовки; блоки ``` или ```json```.
- Первый непробельный символ всего ответа — «{», последний значимый — «}».
- Стандарт JSON: ключи и строки в двойных кавычках; без комментариев // /* */; без хвостовых запятых."""


def extract_json_object(text: str) -> dict:
    if not text:
        raise ValueError("Пустой ответ модели")
    s = text.strip()
    if "```json" in s:
        s = s.split("```json", 1)[1].split("```", 1)[0].strip()
    elif "```" in s:
        parts = s.split("```", 2)
        if len(parts) >= 2:
            s = parts[1].strip()
            if s.lower().startswith("json"):
                s = s[4:].lstrip()
    start = s.find("{")
    if start < 0:
        raise ValueError("JSON не найден в ответе")
    depth = 0
    for i in range(start, len(s)):
        if s[i] == "{":
            depth += 1
        elif s[i] == "}":
            depth -= 1
            if depth == 0:
                return json.loads(s[start : i + 1])
    raise ValueError("Незавершённый JSON")


_ALLOWED_STAGES = frozenset({"condition", "solution"})
_ALLOWED_IMPORTANCE = frozenset({"key", "auxiliary"})
_ALLOWED_WIDGETS = frozenset(
    {
        "короткий ответ",
        "выбор из списка",
        "заполнение пропусков",
        "сортировка по группам",
        "установить соответствие",
        "упорядочивание",
        "развёрнутый ответ",
    }
)
_ALLOWED_BLOOM = frozenset(
    {
        "запоминание",
        "понимание",
        "применение",
        "анализ",
        "оценка",
        "создание",
    }
)


def _as_str(x) -> str:
    if x is None:
        return ""
    if isinstance(x, bool):
        return "true" if x else "false"
    if isinstance(x, (int, float)):
        return str(x)
    return str(x).strip()


def _coerce_int(val):
    if val is None:
        return None
    if isinstance(val, bool):
        return None
    if isinstance(val, int):
        return val
    if isinstance(val, float) and val == int(val):
        return int(val)
    s = str(val).strip()
    if s.isdigit() or (s.startswith("-") and s[1:].isdigit()):
        try:
            return int(s)
        except ValueError:
            return None
    return None


def _coerce_int_list(items) -> List[int]:
    out: List[int] = []
    if not items:
        return out
    if not isinstance(items, list):
        return out
    for x in items:
        n = _coerce_int(x)
        if n is not None:
            out.append(n)
    return out


TASK_MARKUP_REF_KEYS = (
    "subject",
    "section",
    "topic",
    "grade_level",
    "curriculum_standard",
    "source",
    "content_elements",
    "task_type_by_action",
    "task_type_by_widget",
    "answer_format",
    "text_length",
    "non_text_elements",
    "non_subject_context",
    "requires_external_info",
    "external_info_sources",
    "hidden_assumptions",
    "variable_numbers",
    "variable_expressions",
    "structural_elements",
    "educational_actions",
    "key_features",
    "bloom_level",
)


def task_markup_slice_from_raw(raw: dict) -> dict:
    return {k: raw.get(k) for k in TASK_MARKUP_REF_KEYS}


def normalize_analysis_raw(raw: dict) -> Tuple[dict, List[str]]:
    fixes: List[str] = []
    if not isinstance(raw, dict):
        fixes.append("корень ответа не объект — обработка невозможна без исправления моделью")
        return {}, fixes

    out = dict(raw)

    # Normalize subsection_choice (new flow)
    sc = out.get("subsection_choice")
    if sc is not None:
        out["subsection_choice"] = _as_str(sc)

    # Normalize topic_id (legacy flow / second pass)
    tid = out.get("topic_id")
    ci = _coerce_int(tid)
    if ci is not None and ci != tid:
        out["topic_id"] = ci
        fixes.append("topic_id приведён к целому числу")
    elif tid is not None and ci is None:
        fixes.append("topic_id не удалось привести к int")

    ce = out.get("content_elements")
    if ce is None:
        out["content_elements"] = []
        fixes.append("content_elements отсутствовало — подставлен []")
    elif not isinstance(ce, list):
        out["content_elements"] = []
        fixes.append("content_elements не массив — заменено на []")
    else:
        new_ce: List[dict] = []
        for i, el in enumerate(ce):
            if isinstance(el, str):
                new_ce.append({"name": el.strip(), "stage": "condition", "importance": "auxiliary"})
                fixes.append(f"content_elements[{i}]: строка развёрнута в объект")
                continue
            if not isinstance(el, dict):
                continue
            name = _as_str(el.get("name") or el.get("label") or el.get("название"))
            stage = _as_str(el.get("stage")).lower() or "condition"
            if stage not in _ALLOWED_STAGES:
                stage = "condition"
                fixes.append(f"content_elements[{i}]: неизвестный stage — condition")
            imp = _as_str(el.get("importance")).lower() or "auxiliary"
            if imp not in _ALLOWED_IMPORTANCE:
                imp = "auxiliary"
                fixes.append(f"content_elements[{i}]: неизвестная importance — auxiliary")
            merged = dict(el)
            merged["name"] = name
            merged["stage"] = stage
            merged["importance"] = imp
            new_ce.append(merged)
        out["content_elements"] = new_ce

    ea = out.get("educational_actions")
    if ea is None:
        out["educational_actions"] = []
        fixes.append("educational_actions отсутствовало — []")
    elif not isinstance(ea, list):
        out["educational_actions"] = []
        fixes.append("educational_actions не массив — []")
    else:
        new_ea: List[dict] = []
        for i, ed in enumerate(ea):
            if isinstance(ed, str):
                new_ea.append({"action": ed.strip(), "prerequisite": []})
                fixes.append(f"educational_actions[{i}]: строка → объект с action")
                continue
            if not isinstance(ed, dict):
                continue
            act = _as_str(ed.get("action") or ed.get("действие"))
            pre = _coerce_int_list(ed.get("prerequisite") or ed.get("prerequisites") or [])
            merged = dict(ed)
            merged["action"] = act
            merged["prerequisite"] = pre
            new_ea.append(merged)
        out["educational_actions"] = new_ea

    tta = out.get("task_type_by_action")
    if tta is None:
        out["task_type_by_action"] = []
        fixes.append("task_type_by_action отсутствовало — []")
    elif isinstance(tta, str):
        out["task_type_by_action"] = [tta] if tta.strip() else []
        fixes.append("task_type_by_action: одна строка → массив из одного элемента")
    elif isinstance(tta, list):
        out["task_type_by_action"] = [_as_str(x) for x in tta if _as_str(x)]
    else:
        out["task_type_by_action"] = []
        fixes.append("task_type_by_action: неверный тип — []")

    ttw = out.get("task_type_by_widget")
    if ttw is None:
        out["task_type_by_widget"] = "короткий ответ"
        fixes.append("task_type_by_widget отсутствовало — «короткий ответ»")
    elif isinstance(ttw, list) and ttw:
        out["task_type_by_widget"] = _as_str(ttw[0])
        fixes.append("task_type_by_widget: взят первый элемент списка")
    else:
        out["task_type_by_widget"] = _as_str(ttw)
    w = out["task_type_by_widget"]
    if w not in _ALLOWED_WIDGETS:
        lower_map = {a.lower(): a for a in _ALLOWED_WIDGETS}
        wl = w.lower()
        if wl in lower_map:
            out["task_type_by_widget"] = lower_map[wl]
            fixes.append("task_type_by_widget: нормализован регистр/форма")
        else:
            out["task_type_by_widget"] = "короткий ответ"
            fixes.append("task_type_by_widget: не из списка допустимых — «короткий ответ»")

    af = out.get("answer_format")
    if af is None:
        out["answer_format"] = ""
    else:
        out["answer_format"] = _as_str(af)

    ss = out.get("solution_steps")
    if ss is None:
        out["solution_steps"] = []
        fixes.append("solution_steps отсутствовало — []")
    elif not isinstance(ss, list):
        out["solution_steps"] = [_as_str(ss)] if _as_str(ss) else []
        fixes.append("solution_steps: скаляр обёрнут в массив")
    else:
        out["solution_steps"] = [_as_str(x) for x in ss]

    fa = out.get("final_answer")
    if fa is None:
        out["final_answer"] = ""
    else:
        out["final_answer"] = _as_str(fa)

    bl = out.get("bloom_level")
    if bl is None:
        out["bloom_level"] = "применение"
        fixes.append("bloom_level отсутствовало — «применение»")
    else:
        out["bloom_level"] = _as_str(bl)
    b = out["bloom_level"]
    if b not in _ALLOWED_BLOOM:
        lb = {x.lower(): x for x in _ALLOWED_BLOOM}
        low = b.lower()
        if low in lb:
            out["bloom_level"] = lb[low]
            fixes.append("bloom_level: нормализован регистр")
        else:
            out["bloom_level"] = "применение"
            fixes.append("bloom_level: не из списка — «применение»")

    for _meta_key in ("subject", "section", "topic", "grade_level", "curriculum_standard", "source"):
        if out.get(_meta_key) is None:
            out[_meta_key] = ""
        else:
            out[_meta_key] = _as_str(out.get(_meta_key))

    tl = out.get("text_length")
    if tl is None:
        out["text_length"] = 0
    else:
        try:
            out["text_length"] = int(float(tl))
        except (TypeError, ValueError):
            out["text_length"] = 0
            fixes.append("text_length не число — 0")

    def _norm_obj_list(key: str) -> None:
        v = out.get(key)
        if v is None:
            out[key] = []
        elif not isinstance(v, list):
            out[key] = []
            fixes.append(f"{key} не массив — []")
        else:
            out[key] = [x for x in v if isinstance(x, dict)]

    _norm_obj_list("non_text_elements")
    _norm_obj_list("variable_numbers")
    _norm_obj_list("variable_expressions")
    _norm_obj_list("structural_elements")

    nsc = out.get("non_subject_context")
    if nsc is None or not isinstance(nsc, list) or len(nsc) == 0:
        out["non_subject_context"] = [{"present": False, "other_subject": None, "plot": None, "replaceable": False}]
        if nsc is not None and not isinstance(nsc, list):
            fixes.append("non_subject_context не массив — объект по умолчанию")
    else:
        norm_nsc: List[dict] = []
        for it in nsc:
            if not isinstance(it, dict):
                continue
            d = dict(it)
            d["present"] = bool(d.get("present", False))
            os_ = d.get("other_subject")
            d["other_subject"] = None if os_ is None else (_as_str(os_) or None)
            pl = d.get("plot")
            d["plot"] = None if pl is None else (_as_str(pl) or None)
            d["replaceable"] = bool(d.get("replaceable", False))
            norm_nsc.append(d)
        out["non_subject_context"] = norm_nsc or [{"present": False, "other_subject": None, "plot": None, "replaceable": False}]

    rei = out.get("requires_external_info")
    if rei is None:
        out["requires_external_info"] = False
    else:
        out["requires_external_info"] = bool(rei)

    def _norm_str_list_key(key: str) -> None:
        v = out.get(key)
        if v is None:
            out[key] = []
        elif not isinstance(v, list):
            s = _as_str(v)
            out[key] = [s] if s else []
            fixes.append(f"{key}: скаляр обёрнут в массив")
        else:
            out[key] = [_as_str(x) for x in v if _as_str(x)]

    _norm_str_list_key("external_info_sources")
    _norm_str_list_key("hidden_assumptions")
    _norm_str_list_key("key_features")

    return out, fixes


def validate_analysis_raw(raw: dict, mode: str = "subsection_flow") -> List[str]:
    errs: List[str] = []
    if not isinstance(raw, dict):
        return ["корень не объект"]
    if mode == "subsection_flow":
        sc = raw.get("subsection_choice")
        if not sc or not isinstance(sc, str) or not sc.strip():
            errs.append("отсутствует или пустой subsection_choice")
    else:
        if raw.get("topic_id") is None:
            errs.append("отсутствует или пустой topic_id")
        elif not isinstance(raw.get("topic_id"), int):
            errs.append("topic_id должен быть целым числом")
    for key, label in (
        ("content_elements", "content_elements"),
        ("educational_actions", "educational_actions"),
        ("task_type_by_action", "task_type_by_action"),
        ("solution_steps", "solution_steps"),
    ):
        if not isinstance(raw.get(key), list):
            errs.append(f"{label} должен быть массивом")
    if not isinstance(raw.get("task_type_by_widget"), str):
        errs.append("task_type_by_widget должен быть строкой")
    if not isinstance(raw.get("answer_format"), str):
        errs.append("answer_format должен быть строкой")
    if not isinstance(raw.get("final_answer"), str):
        errs.append("final_answer должен быть строкой")
    if not isinstance(raw.get("bloom_level"), str):
        errs.append("bloom_level должен быть строкой")
    for i, el in enumerate(raw.get("content_elements") or []):
        if not isinstance(el, dict):
            errs.append(f"content_elements[{i}] должен быть объектом")
        elif not _as_str(el.get("name")):
            errs.append(f"content_elements[{i}]: нужен непустой name")
    for i, ed in enumerate(raw.get("educational_actions") or []):
        if not isinstance(ed, dict):
            errs.append(f"educational_actions[{i}] должен быть объектом")
        elif not _as_str(ed.get("action")):
            errs.append(f"educational_actions[{i}]: нужен непустой action")
        elif not isinstance(ed.get("prerequisite"), list):
            errs.append(f"educational_actions[{i}]: prerequisite должен быть массивом")
    for key in ("non_text_elements", "variable_numbers", "variable_expressions", "structural_elements"):
        v = raw.get(key)
        if v is None:
            continue
        if not isinstance(v, list):
            errs.append(f"{key} должен быть массивом")
        else:
            for j, it in enumerate(v):
                if not isinstance(it, dict):
                    errs.append(f"{key}[{j}] должен быть объектом")
    nsc = raw.get("non_subject_context")
    if nsc is not None:
        if not isinstance(nsc, list):
            errs.append("non_subject_context должен быть массивом")
        else:
            for j, it in enumerate(nsc):
                if not isinstance(it, dict):
                    errs.append(f"non_subject_context[{j}] должен быть объектом")
    if raw.get("requires_external_info") is not None and not isinstance(raw.get("requires_external_info"), bool):
        errs.append("requires_external_info должен быть boolean")
    for key in ("external_info_sources", "hidden_assumptions", "key_features"):
        v = raw.get(key)
        if v is None:
            continue
        if not isinstance(v, list):
            errs.append(f"{key} должен быть массивом")
    if raw.get("text_length") is not None and not isinstance(raw.get("text_length"), int):
        errs.append("text_length должен быть целым числом")
    return errs


_ANALYSIS_JSON_SCHEMA_FALLBACK = """Один корневой JSON-объект. Ключи — snake_case."""
_ANALYSIS_FIELD_RULES_FALLBACK = """Структура ответа — как в блоке «чёткий шаблон корня»."""


def get_analysis_json_schema_text() -> str:
    t = read_analysis_json_schema_file()
    if t and t.strip():
        return t.strip()
    return _ANALYSIS_JSON_SCHEMA_FALLBACK.strip()


def get_analysis_field_rules_text() -> str:
    t = read_analysis_field_rules_file()
    if t and t.strip():
        return t.strip()
    return _ANALYSIS_FIELD_RULES_FALLBACK.strip()


ANALYSIS_PLACEHOLDERS_HELP = """Подстановки сервера (оставляйте имя в фигурных скобках ровно как указано):

{subsections_json} — JSON-массив подразделов из справочника (новый поток: первый запрос).
{topics_json} — JSON-массив тем из справочника (устаревший поток: весь список).
{meta_json} — JSON с полями импорта.
{kes} — строка КЭС задания или «—».
{exam_type}, {task_number}, {answer_type}, {answer_format_import}, {subject_import} — отдельные поля.
{json_output_strict} — жёсткие правила формата.
{analysis_json_schema} — описание структуры JSON-ответа.
{analysis_field_rules} — расшифровка полей.
{analysis_methodology} — блок методики из кода."""


ANALYSIS_METHODOLOGY_TASK_ANALYZER_RU = """
══════════════════════════════════════════════════════════════════════════════
КАК АНАЛИЗИРОВАТЬ УСЛОВИЕ И РЕШЕНИЕ И ЧТО СОХРАНЯТЬ В ПОЛЯХ (методика task_analyzer)
══════════════════════════════════════════════════════════════════════════════
""".strip()


_DEFAULT_TEMPLATE_SHELL_HEAD = """Роль: ты — педагогический ассистент-аналитик учебных заданий по математике.

Входные переменные:
* meta_json: {meta_json}
* kes: {kes}
* topics_json: см. блок в конце промпта.

"""

_DEFAULT_TEMPLATE_SHELL_TAIL = """

Требования к выводу:
{json_output_strict}

Структура JSON ответа:
{analysis_json_schema}

Правила полей:
{analysis_field_rules}

СПРАВОЧНИК ТЕМ:
{topics_json}

СЛУЖЕБНЫЕ ДАННЫЕ: {meta_json}
КЭС: {kes}

Итог: весь ответ — только один JSON-объект, без markdown, первый символ «{», последний «}».
"""


def load_default_analysis_system_template() -> str:
    raw = read_default_analysis_prompt_file()
    if raw and raw.strip():
        return raw.strip()
    return _DEFAULT_TEMPLATE_SHELL_HEAD + ANALYSIS_METHODOLOGY_TASK_ANALYZER_RU + _DEFAULT_TEMPLATE_SHELL_TAIL


def repair_analysis_json_via_llm(
    api_key: str,
    model: str,
    proxy_url: Optional[str],
    *,
    raw_model_text: str,
    reason: str,
    partial: Optional[dict] = None,
    provider: str = "anthropic",
) -> Tuple[str, Dict[str, int]]:
    system = "\n\n".join([
        JSON_OUTPUT_STRICT_RU,
        "Ты исправляешь ответ предыдущей модели. Верни ровно один JSON-объект по схеме ниже.",
        get_analysis_json_schema_text(),
        "Сохрани смысл разметки; заполни отсутствующие обязательные поля разумными значениями.",
    ])
    parts = [f"Причина исправления: {reason}\n"]
    if partial is not None:
        parts.append("Частично восстановленный JSON:\n" + json.dumps(partial, ensure_ascii=False)[:10000])
    else:
        parts.append("Сырой ответ модели:\n" + (raw_model_text or "")[:12000])
    user = "\n".join(parts)
    return call_ai(api_key, model,
        [{"role": "user", "content": [{"type": "text", "text": user}]}],
        system, max_tokens=8192, proxy_url=proxy_url, provider=provider)


def normalize_merge_mapping(mapping: dict) -> Tuple[dict, List[str]]:
    fixes: List[str] = []
    if not isinstance(mapping, dict):
        return {"content_results": [], "skill_results": []}, ["mapping не объект"]
    out = dict(mapping)
    for key, idx_key in (("content_results", "local_i"), ("skill_results", "action_index")):
        rows = out.get(key)
        if rows is None:
            out[key] = []
            fixes.append(f"{key} отсутствовало — []")
        elif not isinstance(rows, list):
            out[key] = []
            fixes.append(f"{key} не массив — []")
        else:
            new_rows: List[dict] = []
            for i, row in enumerate(rows):
                if not isinstance(row, dict):
                    continue
                r = dict(row)
                ik = r.get(idx_key)
                ci = _coerce_int(ik)
                if ci is not None:
                    r[idx_key] = ci
                elif ik is not None:
                    fixes.append(f"{key}[{i}]: {idx_key} не число — строка пропущена")
                    continue
                else:
                    continue
                ex = r.get("existing_id")
                if ex is None or (isinstance(ex, str) and ex.lower() == "null"):
                    r["existing_id"] = None
                else:
                    ei = _coerce_int(ex)
                    r["existing_id"] = ei
                    if ei is None and ex is not None:
                        fixes.append(f"{key}[{i}]: existing_id не int — обнулено")
                        r["existing_id"] = None
                new_rows.append(r)
            out[key] = new_rows
    return out, fixes


MERGE_JSON_SCHEMA_TEXT = """Корневой объект:
{
  "content_results": [{"local_i": <int>, "existing_id": <int или null>}],
  "skill_results": [{"action_index": <int>, "existing_id": <int или null>}]
}"""


MERGE_WITH_TOPIC_JSON_SCHEMA_TEXT = """Корневой объект:
{
  "topic_id": <int из списка тем подраздела>,
  "content_results": [{"local_i": <int>, "existing_id": <int или null>}],
  "skill_results": [{"action_index": <int>, "existing_id": <int или null>}]
}"""


def repair_merge_mapping_via_llm(
    api_key: str,
    model: str,
    proxy_url: Optional[str],
    *,
    failed_text: str,
    reason: str,
    payload_user: str,
    provider: str = "anthropic",
) -> Tuple[str, Dict[str, int]]:
    system = "\n\n".join([
        JSON_OUTPUT_STRICT_RU,
        "Ты исправляешь ответ сопоставления со справочником.",
        MERGE_JSON_SCHEMA_TEXT,
        "Верни только один JSON-объект, без текста вокруг.",
    ])
    user = f"{reason}\n\nЗапрос был:\n{payload_user[:8000]}\n\nНекорректный ответ:\n{failed_text[:6000]}"
    return call_ai(api_key, model,
        [{"role": "user", "content": [{"type": "text", "text": user}]}],
        system, max_tokens=4096, proxy_url=proxy_url, provider=provider)


def normalize_merge_with_topic_mapping(mapping: dict) -> Tuple[dict, List[str]]:
    """Extends normalize_merge_mapping to also handle topic_id."""
    fixes: List[str] = []
    if not isinstance(mapping, dict):
        return {"topic_id": None, "content_results": [], "skill_results": []}, ["mapping не объект"]
    out, base_fixes = normalize_merge_mapping(mapping)
    fixes.extend(base_fixes)
    tid = out.get("topic_id")
    ci = _coerce_int(tid)
    if ci is not None:
        out["topic_id"] = ci
    else:
        out["topic_id"] = None
        if tid is not None:
            fixes.append("topic_id не удалось привести к int — обнулено")
    return out, fixes


def _get_topics_for_subsection_choice(
    all_topics: List[dict], subsection_choice: str, section_choice: str = ""
) -> List[dict]:
    """Finds topics matching subsection_choice returned by the model."""
    subsection_choice = (subsection_choice or "").strip()
    section_choice = (section_choice or "").strip()
    if not subsection_choice:
        return []
    # Exact match on subsection field
    matches = [t for t in all_topics if (t.get("subsection") or "").strip() == subsection_choice]
    if matches:
        return matches
    # Subsection is empty → model may have returned the section name
    matches = [t for t in all_topics if (t.get("section") or "").strip() == subsection_choice]
    if matches:
        return matches
    # Fuzzy fallback
    subs_set = list({(t.get("subsection") or t.get("section") or "").strip() for t in all_topics})
    subs_set = [s for s in subs_set if s]
    if subs_set:
        best, score, _ = process.extractOne(subsection_choice, subs_set, scorer=fuzz.token_sort_ratio)
        if score >= 75:
            matches = [
                t for t in all_topics
                if (t.get("subsection") or t.get("section") or "").strip() == best
            ]
    return matches


def merge_catalogs_with_topic_llm(
    raw: dict,
    subsection_choice: str,
    section_choice: str,
    subsection_topics: List[dict],
    api_key: str,
    model: str,
    proxy_url: Optional[str],
    task_body_text: str = "",
    task_images: Optional[dict] = None,
    provider: str = "anthropic",
) -> dict:
    """Second pass: picks topic_id from subsection topics AND matches skills/elements."""
    from migration_database import get_conn

    conn = get_conn()
    ce_rows = conn.execute("SELECT id, label_display FROM content_element_defs").fetchall()
    sk_rows = conn.execute("SELECT id, label_display FROM skill_defs").fetchall()
    conn.close()

    ce_choices = {r[1]: r[0] for r in ce_rows}
    sk_choices = {r[1]: r[0] for r in sk_rows}
    ce_list = list(ce_choices.keys())
    sk_list = list(sk_choices.keys())

    content_items: List[dict] = []
    for li, el in enumerate(raw.get("content_elements") or []):
        name = (el.get("name") or "").strip()
        if not name:
            continue
        cand = []
        if ce_list:
            for m, score, _ in process.extract(name, ce_list, scorer=fuzz.token_sort_ratio, limit=5):
                if score >= 55:
                    cand.append({"id": ce_choices[m], "label": m, "score": score})
        content_items.append({"local_i": li, "text": name, "stage": el.get("stage"),
                               "importance": el.get("importance"), "candidates": cand})

    skill_items: List[dict] = []
    for ai, ed in enumerate(raw.get("educational_actions") or []):
        act = (ed.get("action") or "").strip()
        if not act:
            continue
        cand = []
        if sk_list:
            for m, score, _ in process.extract(act, sk_list, scorer=fuzz.token_sort_ratio, limit=5):
                if score >= 55:
                    cand.append({"id": sk_choices[m], "label": m, "score": score})
        skill_items.append({"action_index": ai, "text": act,
                            "prerequisite": ed.get("prerequisite") or [], "candidates": cand})

    topics_payload = _topics_payload_for_prompt(subsection_topics)

    user_data = {
        "instructions": (
            f"{JSON_OUTPUT_STRICT_RU}\n"
            f"Выбери РОВНО ОДНУ тему из списка topics (topic_id — целое число из списка, не выдумывай). "
            f"Для каждого элемента content: если смысл совпадает с кандидатом из candidates — укажи его existing_id, иначе null. "
            f"Для каждого навыка skills: аналогично. "
            f"Строгий вид ответа:\n{MERGE_WITH_TOPIC_JSON_SCHEMA_TEXT}"
        ),
        "topics": topics_payload,
        "subsection_context": {"subsection": subsection_choice, "section": section_choice},
        "content": content_items,
        "skills": skill_items,
    }
    user_text = json.dumps(user_data, ensure_ascii=False)

    user_parts: List[dict] = []
    if task_body_text:
        user_parts.extend(build_message_content(
            f"Условие задачи (для контекста выбора темы):\n{task_body_text}",
            task_images or {},
        ))
    user_parts.append({"type": "text", "text": "\n\n" + user_text})

    sys = "\n\n".join([
        JSON_OUTPUT_STRICT_RU,
        "Ты выбираешь тему из подраздела и сопоставляешь формулировки с элементами справочника.",
        MERGE_WITH_TOPIC_JSON_SCHEMA_TEXT,
        "Ответ — только один JSON-объект.",
    ])

    text, u = call_ai(api_key, model,
        [{"role": "user", "content": user_parts}],
        sys, max_tokens=4096, proxy_url=proxy_url, provider=provider)

    mapping: dict = {}
    try:
        mapping = extract_json_object(text)
    except Exception as parse_ex:
        try:
            text2, u2 = repair_merge_mapping_via_llm(api_key, model, proxy_url,
                failed_text=text, reason=f"парсинг merge+topic JSON: {parse_ex}", payload_user=user_text,
                provider=provider)
            u["input_tokens"] += u2["input_tokens"]
            u["output_tokens"] += u2["output_tokens"]
            mapping = extract_json_object(text2)
        except Exception:
            mapping = {}

    mapping, _ = normalize_merge_with_topic_mapping(mapping)

    topic_id_from_merge = mapping.get("topic_id")
    valid_topic_ids = {t["id"] for t in subsection_topics}
    if topic_id_from_merge not in valid_topic_ids:
        topic_id_from_merge = subsection_topics[0]["id"] if subsection_topics else None

    cr: Dict[int, Optional[int]] = {}
    sr: Dict[int, Optional[int]] = {}
    for row in mapping.get("content_results") or []:
        if row.get("local_i") is not None:
            cr[int(row["local_i"])] = row.get("existing_id")
    for row in mapping.get("skill_results") or []:
        if row.get("action_index") is not None:
            sr[int(row["action_index"])] = row.get("existing_id")

    normalized_content = []
    for it in content_items:
        li = it["local_i"]
        existing = cr.get(li)
        try:
            eid = int(existing) if existing is not None and str(existing).lower() != "null" else None
        except (TypeError, ValueError):
            eid = None
        normalized_content.append({"existing_id": eid, "label": it["text"],
                                    "importance": it.get("importance"), "stage": it.get("stage")})

    skill_steps_out: List[dict] = []
    for it in skill_items:
        ai = it["action_index"]
        existing = sr.get(ai)
        try:
            eid = int(existing) if existing is not None and str(existing).lower() != "null" else None
        except (TypeError, ValueError):
            eid = None
        skill_steps_out.append({
            "order": ai, "existing_skill_id": eid, "label": it["text"],
            "prereq_indices": it.get("prerequisite") or [],
        })
    skill_steps_out.sort(key=lambda x: x["order"])

    return {
        "topic_id": topic_id_from_merge,
        "normalized": {
            "content_elements": normalized_content,
            "skill_steps": skill_steps_out,
        },
        "usage": u,
    }


_ANTHROPIC_IMAGE_MIME_ALLOWED = frozenset({"image/jpeg", "image/png", "image/gif", "image/webp"})


def _normalize_image_mime(mime: str) -> str:
    m = (mime or "").strip().lower().split(";", 1)[0].strip()
    if m in ("image/jpg", "image/pjpeg", "image/x-jpeg"):
        return "image/jpeg"
    return m or "image/png"


def _image_bytes_to_png_b64(raw: bytes) -> Optional[str]:
    if Image is None:
        return None
    try:
        im = Image.open(io.BytesIO(raw))
        im.load()
    except (UnidentifiedImageError, OSError, ValueError):
        return None
    if im.mode == "RGBA":
        pass
    elif im.mode == "LA":
        im = im.convert("RGBA")
    elif im.mode == "P" and "transparency" in im.info:
        im = im.convert("RGBA")
    elif im.mode == "P":
        im = im.convert("RGB")
    elif im.mode in ("L", "1"):
        im = im.convert("RGB")
    elif im.mode != "RGB":
        im = im.convert("RGB")
    buf = io.BytesIO()
    im.save(buf, format="PNG", optimize=True)
    return base64.b64encode(buf.getvalue()).decode("ascii")


def _svg_bytes_to_png_b64(raw: bytes, width: Optional[int] = None, height: Optional[int] = None) -> Optional[str]:
    """Конвертирует SVG-байты в PNG base64. Размер берётся из width/height (если заданы)."""
    if _cairosvg is None:
        return None
    try:
        kwargs: dict = {}
        if width:
            kwargs["output_width"] = int(width)
        if height:
            kwargs["output_height"] = int(height)
        png_bytes = _cairosvg.svg2png(bytestring=raw, **kwargs)
        return base64.b64encode(png_bytes).decode("ascii")
    except Exception:
        return None


def _prepare_image_for_anthropic(
    mime: str, b64: str, width: Optional[int] = None, height: Optional[int] = None
) -> Optional[Tuple[str, str]]:
    m = _normalize_image_mime(mime)
    # SVG обрабатываем отдельно через cairosvg
    if m in ("image/svg+xml", "image/svg"):
        try:
            raw = base64.b64decode(b64, validate=False)
        except (ValueError, TypeError):
            return None
        png_b64 = _svg_bytes_to_png_b64(raw, width=width, height=height)
        return ("image/png", png_b64) if png_b64 else None
    if m in _ANTHROPIC_IMAGE_MIME_ALLOWED:
        return m, b64
    try:
        raw = base64.b64decode(b64, validate=False)
    except (ValueError, TypeError):
        return None
    png_b64 = _image_bytes_to_png_b64(raw)
    if png_b64:
        return "image/png", png_b64
    return None


def _lookup_image_payload(images: dict, fn: str) -> Optional[dict]:
    if not fn or not isinstance(images, dict):
        return None
    if fn in images and isinstance(images[fn], dict):
        return images[fn]
    norm = fn.replace("\\", "/")
    base = os.path.basename(norm)
    if base in images and isinstance(images[base], dict):
        return images[base]
    norm_lower = norm.lower()
    base_lower = base.lower()
    found: Optional[dict] = None
    for k, v in images.items():
        if not isinstance(v, dict):
            continue
        kl = str(k).replace("\\", "/").lower()
        if kl == norm_lower:
            return v
        if kl.endswith("/" + base_lower) or os.path.basename(kl) == base_lower:
            found = v
    return found


def collect_images_for_analysis_task(task: dict) -> dict:
    out: dict = {}
    for name, data in (task.get("images") or {}).items():
        if isinstance(data, dict) and data.get("data"):
            out[str(name)] = data
    for name, data in (task.get("attachments") or {}).items():
        if not isinstance(data, dict) or not data.get("data"):
            continue
        mime = (data.get("mime") or "").lower()
        nl = str(name).lower()
        if mime.startswith("image/") or nl.endswith(
            (".png", ".jpeg", ".jpg", ".gif", ".webp", ".bmp", ".svg", ".tif", ".tiff")
        ):
            key = str(name)
            if key not in out:
                out[key] = data
    return out


def _split_text_and_images(body: str, images: dict) -> Tuple[str, List[Tuple[str, str, str]], List[str]]:
    if not body:
        return "", [], []
    found: List[Tuple[str, str, str]] = []
    missing: List[str] = []
    pattern = re.compile(r"\[img(?:_inline)?:([^\]]+)\]")

    def repl(m):
        fn = m.group(1).strip()
        data = _lookup_image_payload(images or {}, fn)
        if data and data.get("data"):
            mime = data.get("mime") or "image/png"
            found.append((mime, data["data"], fn))
            return f"[изображение {len(found)}: {fn}]"
        if fn not in missing:
            missing.append(fn)
        return f"[рисунок не передан в API — нет файла «{fn}»]"

    text = pattern.sub(repl, body)
    return text, found, missing


def build_message_content(text: str, images: dict, max_images: int = 10) -> List[dict]:
    clean, imgs, missing = _split_text_and_images(text, images)
    conv_notes: List[str] = []
    lead: List[str] = []
    if missing:
        lead.append(
            "ВНИМАНИЕ: для вставок изображений нет бинарных данных: "
            + ", ".join(f"«{x}»" for x in missing)
        )
    image_blocks: List[dict] = []
    for mime, b64, fn in imgs[:max_images]:
        prepared = _prepare_image_for_anthropic(mime, b64)
        if prepared is None:
            conv_notes.append(f"[«{fn}» не отправлен — неподдерживаемый формат]")
            continue
        out_mime, out_b64 = prepared
        image_blocks.append({"type": "image", "source": {"type": "base64", "media_type": out_mime, "data": out_b64}})
    if conv_notes:
        lead.extend(conv_notes)
    body_text = (clean or "").strip()
    if lead:
        body_text = "\n".join(lead) + ("\n\n" + body_text if body_text else "")
    blocks: List[dict] = []
    if body_text:
        blocks.append({"type": "text", "text": body_text})
    blocks.extend(image_blocks)
    if not blocks:
        blocks.append({"type": "text", "text": "(текст задания отсутствует)"})
    return blocks


def openrouter_messages(
    api_key: str,
    model: str,
    messages: List[dict],
    system: str,
    max_tokens: int = 8192,
) -> Tuple[str, Dict[str, int]]:
    """Call OpenRouter OpenAI-compatible API. Converts Anthropic content format to OpenAI format."""
    full_messages: List[dict] = [{"role": "system", "content": system}]
    for msg in messages:
        content = msg.get("content")
        if isinstance(content, str):
            full_messages.append({"role": msg["role"], "content": content})
        elif isinstance(content, list):
            converted: List[dict] = []
            for block in content:
                btype = block.get("type")
                if btype == "text":
                    converted.append({"type": "text", "text": block.get("text") or ""})
                elif btype == "image":
                    src = block.get("source") or {}
                    if src.get("type") == "base64":
                        data_url = f"data:{src['media_type']};base64,{src['data']}"
                        converted.append({"type": "image_url", "image_url": {"url": data_url}})
            full_messages.append({"role": msg["role"], "content": converted or [{"type": "text", "text": ""}]})
        else:
            full_messages.append({"role": msg["role"], "content": ""})

    resp = requests.post(
        "https://openrouter.ai/api/v1/chat/completions",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://fipi-app.onrender.com",
        },
        json={"model": model, "max_tokens": max_tokens, "messages": full_messages},
        timeout=300,
    )
    if resp.status_code != 200:
        try:
            err = resp.json()
        except Exception:
            err = {"raw": resp.text[:2000]}
        raise RuntimeError(f"OpenRouter HTTP {resp.status_code}: {json.dumps(err, ensure_ascii=False)}")
    data = resp.json()
    text = ""
    try:
        text = data["choices"][0]["message"]["content"] or ""
    except (KeyError, IndexError, TypeError):
        pass
    usage = data.get("usage") or {}
    u = {
        "input_tokens": int(usage.get("prompt_tokens") or 0),
        "output_tokens": int(usage.get("completion_tokens") or 0),
    }
    return text, u


def call_ai(
    api_key: str,
    model: str,
    messages: List[dict],
    system: str,
    max_tokens: int = 8192,
    proxy_url: Optional[str] = None,
    provider: str = "anthropic",
) -> Tuple[str, Dict[str, int]]:
    """Dispatch to Anthropic or OpenRouter depending on provider."""
    if provider == "openrouter":
        return openrouter_messages(api_key, model, messages, system, max_tokens)
    return anthropic_messages(api_key, model, messages, system, max_tokens, proxy_url)


def anthropic_messages(
    api_key: str,
    model: str,
    messages: List[dict],
    system: str,
    max_tokens: int = 8192,
    proxy_url: Optional[str] = None,
) -> Tuple[str, Dict[str, int]]:
    proxies = _proxies(proxy_url)
    resp = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers={
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        },
        json={"model": model, "max_tokens": max_tokens, "system": system, "messages": messages},
        proxies=proxies,
        timeout=300,
    )
    if resp.status_code != 200:
        try:
            err = resp.json()
        except Exception:
            err = {"raw": resp.text[:2000]}
        raise RuntimeError(f"Anthropic HTTP {resp.status_code}: {json.dumps(err, ensure_ascii=False)}")
    data = resp.json()
    text_parts = []
    for block in data.get("content") or []:
        if block.get("type") == "text":
            text_parts.append(block.get("text") or "")
    text = "\n".join(text_parts)
    usage = data.get("usage") or {}
    u = {
        "input_tokens": int(usage.get("input_tokens") or 0),
        "output_tokens": int(usage.get("output_tokens") or 0),
    }
    return text, u


def usage_cost_usd(inp: int, out: int, model: str = "", provider: str = "anthropic") -> float:
    in_p, out_p = model_pricing(model, provider)
    return (inp / 1_000_000.0) * in_p + (out / 1_000_000.0) * out_p


def usage_cost_rub(inp: int, out: int, model: str = "", provider: str = "anthropic") -> float:
    return usage_cost_usd(inp, out, model, provider) * RUB_PER_USD


def _topics_payload_for_prompt(topics: List[dict]) -> List[dict]:
    return [
        {
            "topic_id": t["id"],
            "section": t["section"],
            "subsection": t.get("subsection") or "",
            "topic": t["topic"],
            "grade_class": t["grade_class"],
            "description": (t.get("topic_description") or "")[:500],
        }
        for t in topics
    ]


def _topics_json_for_prompt(topics: List[dict]) -> str:
    return json.dumps(_topics_payload_for_prompt(topics), ensure_ascii=False)


def _subsections_payload_for_prompt(topics: List[dict]) -> List[dict]:
    """Groups topics into unique (section, subsection) pairs for the first pass."""
    seen: Dict[tuple, int] = {}
    result: List[dict] = []
    for t in topics:
        section = (t.get("section") or "").strip()
        subsection = (t.get("subsection") or "").strip()
        key = (section, subsection)
        if key not in seen:
            idx = len(result)
            seen[key] = idx
            result.append({"idx": idx, "section": section, "subsection": subsection, "topic_count": 1})
        else:
            result[seen[key]]["topic_count"] += 1
    return result


def _subsections_json_for_prompt(topics: List[dict]) -> str:
    return json.dumps(_subsections_payload_for_prompt(topics), ensure_ascii=False)


def substitute_analysis_system_template(template: str, topics: List[dict], kes: str, meta: dict) -> str:
    topics_json = _topics_json_for_prompt(topics)
    subsections_json = _subsections_json_for_prompt(topics)
    meta_json = json.dumps(meta, ensure_ascii=False)
    repl = {
        "{topics_json}": topics_json,
        "{subsections_json}": subsections_json,
        "{meta_json}": meta_json,
        "{kes}": kes or "—",
        "{json_output_strict}": JSON_OUTPUT_STRICT_RU,
        "{analysis_json_schema}": get_analysis_json_schema_text(),
        "{analysis_field_rules}": get_analysis_field_rules_text(),
        "{exam_type}": _as_str(meta.get("exam_type")),
        "{task_number}": _as_str(meta.get("task_number")),
        "{answer_type}": _as_str(meta.get("answer_type")),
        "{answer_format_import}": _as_str(meta.get("answer_format_import")),
        "{subject_import}": _as_str(meta.get("subject_import")),
        "{analysis_methodology}": ANALYSIS_METHODOLOGY_TASK_ANALYZER_RU,
    }
    out = template
    for key in sorted(repl.keys(), key=len, reverse=True):
        out = out.replace(key, repl[key])
    return out


def _legacy_build_system_prompt(topics: List[dict], base: str, kes: str, meta: dict) -> str:
    topics_json = _topics_json_for_prompt(topics)
    meta_json = json.dumps(meta, ensure_ascii=False)
    b = (base or "").strip()
    if not b:
        b = DEFAULT_ANALYSIS_PROMPT_BODY
    task_block = f"СЛУЖЕБНЫЕ ДАННЫЕ ЗАДАНИЯ:\n{meta_json}\n\nКЭС: {kes or '—'}"
    suffix = (
        f"\n\n{JSON_OUTPUT_STRICT_RU}\n\n"
        + ANALYSIS_METHODOLOGY_TASK_ANALYZER_RU
        + f"\n\nСПРАВОЧНИК ТЕМ:\n{topics_json}\n\n{task_block}\n\n"
        + "СТРУКТУРА JSON:\n" + get_analysis_json_schema_text()
        + "\n\nПРАВИЛА ПОЛЕЙ:\n" + get_analysis_field_rules_text()
        + "\n\nПОВТОР: весь ответ — только один JSON-объект.\n"
    )
    return b + suffix


def build_analysis_system_prompt(custom_template: str, topics: List[dict], kes: str, meta: dict) -> str:
    t = (custom_template or "").strip()
    if not t:
        t = load_default_analysis_system_template()
    if "{topics_json}" in t or "{subsections_json}" in t:
        return substitute_analysis_system_template(t, topics, kes, meta)
    return _legacy_build_system_prompt(topics, t, kes, meta)


def _use_subsection_flow(template: str) -> bool:
    """True if the template uses the new subsection-based flow."""
    has_topics = "{topics_json}" in template
    has_subs = "{subsections_json}" in template
    if has_subs:
        return True
    if has_topics:
        return False
    # Default template (loaded from file) — use new flow
    return True


USER_MESSAGE_ANALYSIS_DESCRIPTION = """Роль user — одно сообщение с несколькими блоками content."""


def build_analysis_prompt_page_payload() -> Dict[str, str]:
    file_body = load_default_analysis_system_template()
    versions = get_analysis_prompt_versions()
    return {
        "file_body": file_body,
        "versions": versions,
        "placeholders_help": ANALYSIS_PLACEHOLDERS_HELP,
        "user_message_description": USER_MESSAGE_ANALYSIS_DESCRIPTION,
    }


def build_analysis_fallback_snapshot(topic_id, topic_row, raw, normalized, result_payload) -> str:
    ce = []
    for el in normalized.get("content_elements") or []:
        ce.append({"название": el.get("label"), "важность": el.get("importance"), "этап": el.get("stage")})
    sk = []
    for step in normalized.get("skill_steps") or []:
        sk.append({"действие": step.get("label"), "пререквизиты_к_индексам_шагов": step.get("prereq_indices") or []})
    payload = {
        "сохранено_utc": datetime.now(timezone.utc).isoformat(),
        "topic_id_на_момент_анализа": topic_id,
        "куррикулум_текстом": {
            "предмет_в_строке_справочника": topic_row.get("subject"),
            "раздел": topic_row.get("section"),
            "подраздел": topic_row.get("subsection") or "",
            "тема": topic_row.get("topic"),
            "класс": str(topic_row.get("grade_class") or ""),
            "описание_темы": (topic_row.get("topic_description") or "")[:2000],
        },
        "элементы_содержания": ce,
        "навыки_по_шагам_решения": sk,
        "тип_вопроса_и_формат": result_payload.get("raw_excerpt") or {},
        "разметка_task_markup": task_markup_slice_from_raw(raw),
        "шаги_решения_текстом": raw.get("solution_steps") if isinstance(raw.get("solution_steps"), list) else [],
        "ответ": raw.get("final_answer"),
    }
    return json.dumps(payload, ensure_ascii=False)


DEFAULT_ANALYSIS_PROMPT_BODY = """Ты аналитик учебных заданий по математике (Россия, школа)."""


def _is_math_subject(subject: str) -> bool:
    s = (subject or "").strip().lower()
    if not s:
        return True
    if "матем" in s:
        return True
    if s == "math" or s == "maths" or s.startswith("math "):
        return True
    if "mathematics" in s:
        return True
    return False


SOLVE_SYSTEM_PROMPT = """Ты — учитель математики. Реши задание пошагово и верни JSON.

СТИЛЬ РЕШЕНИЯ:
Пиши только вычисления и ключевые выводы — без воды.
Не объясняй формулы перед использованием: сразу записывай выражения и считай.
Не выноси «известные данные» отдельным шагом — подставляй значения прямо в ход решения.
Заголовок шага — одна короткая фраза (что делаем), затем сразу математика.
Если вычисление не является сутью задачи — сокращай его до одного выражения с результатом.

СТРУКТУРА ШАГОВ:
Каждый элемент solution_steps — один СМЫСЛОВОЙ шаг, а не отдельное действие.
Группируй вычисления внутри одного шага: выкладки, подстановки, упрощения, проверки —
всё, что относится к одной логической части, пишется в одном элементе массива.
Ориентируйся на 3–7 шагов; не дроби решение мельче, чем нужно для понимания.

ФОРМАТ КАЖДОГО ШАГА: краткий заголовок (что делаем), затем выкладки/пояснения.
Формулы — LaTeX через одиночный доллар: $a^2 + b^2 = c^2$.
Markdown разрешён: **жирный**, *курсив*, ненумерованные списки.

ТРЕБОВАНИЯ К ВЫВОДУ:
- Только один JSON-объект: {"solution_steps": [...], "final_answer": "..."}
- solution_steps — массив строк (каждая строка — один шаг целиком)
- final_answer — строка с ответом для ученика
- Первый непробельный символ «{», последний значимый «}», без блоков ```, без текста вне JSON"""


def _repair_solve_json(text: str) -> Optional[dict]:
    """Try to extract partial solution from truncated or malformed JSON."""
    import re
    steps: List[str] = []
    fa = ""

    # Find solution_steps array and extract complete quoted strings from it
    sol_start = text.find('"solution_steps"')
    if sol_start >= 0:
        bracket = text.find('[', sol_start)
        if bracket >= 0:
            region = text[bracket + 1:]
            for m in re.finditer(r'"((?:[^"\\]|\\.)*)"', region, re.DOTALL):
                val = (m.group(1)
                       .replace('\\n', '\n').replace('\\"', '"')
                       .replace('\\\\', '\\').replace('\\t', '\t'))
                if val.strip():
                    steps.append(val)
                after = region[m.end():].lstrip(' ,\n\t')
                if after.startswith(']'):
                    break

    fa_match = re.search(r'"final_answer"\s*:\s*"((?:[^"\\]|\\.)*)"', text, re.DOTALL)
    if fa_match:
        fa = (fa_match.group(1)
              .replace('\\n', '\n').replace('\\"', '"')
              .replace('\\\\', '\\').replace('\\t', '\t'))

    if steps or fa:
        return {"solution_steps": steps, "final_answer": fa, "_repaired": True}

    # Last resort: strip code fences and return as a single step
    import re as _re
    clean = _re.sub(r'```[a-z]*\n?', '', text, flags=_re.MULTILINE).strip()
    if clean:
        return {"solution_steps": [clean], "final_answer": "", "_repaired": True}
    return None


def run_task_solve(
    task: dict,
    api_key: str,
    model: str = "claude-sonnet-4-20250514",
    proxy_url: Optional[str] = None,
    provider: str = "anthropic",
) -> dict:
    """Call AI to solve the task only (no analysis). Returns solution_steps + final_answer."""
    body_text = (task.get("formatted_text") or "").strip() or (task.get("text") or "")
    images = collect_images_for_analysis_task(task)

    user_parts = build_message_content(body_text, images)
    messages = [{"role": "user", "content": user_parts}]

    usage_total = {"input_tokens": 0, "output_tokens": 0}
    try:
        text, u = call_ai(api_key, model, messages, SOLVE_SYSTEM_PROMPT,
                          max_tokens=8192, proxy_url=proxy_url, provider=provider)
    except Exception as e:
        return {"ok": False, "error": str(e)}

    usage_total["input_tokens"] += u["input_tokens"]
    usage_total["output_tokens"] += u["output_tokens"]

    raw: Optional[dict] = None
    repaired = False
    try:
        raw = extract_json_object(text)
    except Exception:
        raw = _repair_solve_json(text)
        if raw is None:
            return {
                "ok": False,
                "error": "Не удалось разобрать ответ модели — JSON не найден или оборван",
                "raw_text": text[:4000],
                "usage": usage_total,
            }
        repaired = True

    steps = raw.get("solution_steps")
    if not isinstance(steps, list):
        steps = [_as_str(steps)] if steps else []
    else:
        steps = [_as_str(s) for s in steps if _as_str(s)]

    final_answer = _as_str(raw.get("final_answer") or "")

    solution_text = "\n".join(f"{i+1}. {s}" for i, s in enumerate(steps) if s)
    if final_answer:
        solution_text = (solution_text + "\n\nОтвет: " + final_answer).strip()

    inp_p, out_p = model_pricing(model, provider)
    cost_usd = (usage_total["input_tokens"] / 1_000_000) * inp_p + \
               (usage_total["output_tokens"] / 1_000_000) * out_p
    cost_rub = cost_usd * RUB_PER_USD

    header = f"[Решение: {model} · ~{cost_rub:.2f} ₽]"
    solution_text = header + "\n\n" + solution_text

    return {
        "ok": True,
        "solution_steps": steps,
        "final_answer": final_answer,
        "solution_text": solution_text,
        "repaired": repaired,
        "usage": usage_total,
        "cost_usd": round(cost_usd, 5),
        "cost_rub": round(cost_rub, 3),
    }


RECOGNIZE_SYSTEM_PROMPT = """Ты получаешь одно или несколько изображений из условия задания по математике или другому предмету.
Каждое изображение помечено меткой «Файл: <имя>» прямо перед ним.

Для каждого изображения определи, что на нём изображено, и верни результат в JSON.

КЛАССИФИКАЦИЯ:
«formula» — если на изображении ТОЛЬКО математическое выражение, формула или уравнение:
  числа, переменные, знаки операций, дроби, корни, степени, суммы, интегралы, матрицы и т.п.,
  без координатных осей, без геометрических фигур, без рисунков.
«image» — всё остальное: графики функций, геометрические чертежи, схемы, диаграммы, таблицы, рисунки,
  системы координат, изображения физических объектов.

ДЛЯ ФОРМУЛ (type = "formula"):
Поле "formula" — LaTeX-код выражения.
  Правила:
  - Без обрамляющих символов: без $, $$, \\(, \\), \\[, \\].
  - Точно воспроизводи все числа, переменные, знаки, дроби (\\frac{}{}), корни (\\sqrt{}),
    степени, индексы, суммы (\\sum), интегралы (\\int), предельные значения (\\lim),
    греческие буквы, стрелки, скобки.
  - Если формула многострочная — используй среду aligned: \\begin{aligned}...\\end{aligned}.

ДЛЯ ИЗОБРАЖЕНИЙ (type = "image"):
Поле "description" — подробное текстовое описание на русском языке.
  Включи обязательно:
  - Тип объекта: график функции / геометрическая фигура / схема / таблица / …
  - Все числовые данные: подписи осей и их масштаб, координаты точек, длины отрезков, углы,
    значения в ячейках таблицы, подписи на рисунке.
  - Ключевые геометрические или функциональные характеристики: вид кривой, точки пересечения,
    вершины, асимптоты, особые точки.
  - Описание должно быть достаточным для решения задачи без исходного изображения.

ТРЕБОВАНИЯ К ОТВЕТУ:
- Только один JSON-объект без каких-либо пояснений вне него.
- Первый непробельный символ «{», последний значимый «}».
- Без блоков ```.
- Имена файлов берутся точно из меток «Файл: <имя>».

Формат:
{
  "results": {
    "<имя_файла_1>": {"type": "formula", "formula": "..."},
    "<имя_файла_2>": {"type": "image", "description": "..."}
  }
}"""


def substitute_formulas_in_text(text: str, recognition_results: dict) -> str:
    """Заменяет маркеры [img:fname] / [img_inline:fname] на LaTeX, если изображение распознано как формула.
    Нераспознанные и нефорульные картинки оставляет как есть."""
    if not text or not recognition_results:
        return text

    def replace_marker(m: re.Match) -> str:
        tag = m.group(1)         # 'img_inline' или 'img'
        fname = m.group(2).strip()
        rec = recognition_results.get(fname) or {}
        formula = rec.get("formula")
        if formula:
            return f"${formula}$" if tag == "img_inline" else f"$${formula}$$"
        return m.group(0)  # оставляем маркер без изменений

    return re.sub(r"\[(img_inline|img):([^\]]+)\]", replace_marker, text)


def run_image_recognition(
    task: dict,
    api_key: str,
    model: str = "claude-sonnet-4-20250514",
    proxy_url: Optional[str] = None,
    provider: str = "anthropic",
) -> dict:
    """Распознаёт изображения задания: формулы → LaTeX, рисунки → текстовое описание.
    Возвращает {ok, results: {filename: {formula?/description?}}, cost_rub, cost_usd}."""
    images = collect_images_for_analysis_task(task)
    if not images:
        return {"ok": False, "error": "В задании нет изображений"}

    # Строим сообщение: для каждого изображения — метка + image block
    user_parts: List[dict] = []
    skipped: List[str] = []
    sent_filenames: List[str] = []
    for filename, imgdata in images.items():
        w = imgdata.get("width") or None
        h = imgdata.get("height") or None
        prepared = _prepare_image_for_anthropic(
            imgdata.get("mime", "image/png"), imgdata.get("data", ""),
            width=int(w) if w else None, height=int(h) if h else None,
        )
        if prepared is None:
            skipped.append(filename)
            continue
        out_mime, out_b64 = prepared
        user_parts.append({"type": "text", "text": f"Файл: {filename}"})
        user_parts.append({"type": "image", "source": {"type": "base64", "media_type": out_mime, "data": out_b64}})
        sent_filenames.append(filename)

    if not user_parts:
        return {"ok": False, "error": "Не удалось подготовить ни одного изображения (неподдерживаемый формат)"}

    messages = [{"role": "user", "content": user_parts}]
    usage_total = {"input_tokens": 0, "output_tokens": 0}
    try:
        text, u = call_ai(api_key, model, messages, RECOGNIZE_SYSTEM_PROMPT,
                          max_tokens=4096, proxy_url=proxy_url, provider=provider)
    except Exception as e:
        return {"ok": False, "error": str(e)}

    usage_total["input_tokens"] += u["input_tokens"]
    usage_total["output_tokens"] += u["output_tokens"]

    raw: Optional[dict] = None
    try:
        raw = extract_json_object(text)
    except Exception:
        # попытка достать хоть что-то
        try:
            raw = json.loads(text.strip())
        except Exception:
            raw = None

    if not isinstance(raw, dict) or "results" not in raw:
        return {
            "ok": False,
            "error": "Не удалось разобрать ответ модели — JSON не найден или не содержит поля results",
            "raw_text": text[:4000],
            "usage": usage_total,
        }

    results: dict = {}
    for fname, rec in (raw.get("results") or {}).items():
        if not isinstance(rec, dict):
            continue
        kind = rec.get("type", "")
        if kind == "formula" and rec.get("formula"):
            results[fname] = {"formula": str(rec["formula"]).strip()}
        elif kind == "image" and rec.get("description"):
            results[fname] = {"description": str(rec["description"]).strip()}

    inp_p, out_p = model_pricing(model, provider)
    cost_usd = (usage_total["input_tokens"] / 1_000_000) * inp_p + \
               (usage_total["output_tokens"] / 1_000_000) * out_p
    cost_rub = cost_usd * RUB_PER_USD

    return {
        "ok": True,
        "results": results,
        "sent": sent_filenames,
        "skipped": skipped,
        "usage": usage_total,
        "cost_usd": round(cost_usd, 5),
        "cost_rub": round(cost_rub, 3),
    }


def run_task_analysis(
    task: dict,
    api_key: str,
    model: str = "claude-sonnet-4-20250514",
    proxy_url: Optional[str] = None,
    provider: str = "anthropic",
) -> dict:
    """Run AI analysis without saving to DB. Returns result + _save_data for later commit."""
    subject = (task.get("subject") or "").strip()
    if not _is_math_subject(subject):
        return {"ok": False, "error": "Пока анализ только для математики."}

    topics = get_curriculum_topics(None)
    if not topics:
        return {"ok": False, "error": "Справочник тем пуст. Добавьте темы на странице «Темы»."}

    custom_prompt, _ = get_analysis_prompt()
    template = load_default_analysis_system_template()
    subsection_flow = _use_subsection_flow(template)

    body_text = (task.get("formatted_text") or "").strip() or (task.get("text") or "")
    images = collect_images_for_analysis_task(task)
    kes = (task.get("kes") or "").strip()
    solution_in = (task.get("solution") or "").strip()

    meta = {
        "exam_type": task.get("exam_type"),
        "task_number": task.get("task_number"),
        "answer_type": task.get("answer_type"),
        "answer_format_import": task.get("answer_format"),
        "subject_import": task.get("subject"),
    }

    in_p, out_p = model_pricing(model, provider)

    system = build_analysis_system_prompt(custom_prompt, topics, kes, meta)
    user_parts = []
    user_parts.extend(build_message_content(body_text, images))
    sol_block = (
        f"\n\n---\nТЕКСТ РЕШЕНИЯ:\n{solution_in}"
        if solution_in
        else "\n\n---\nРешения нет — реши задание и заполни solution_steps и final_answer."
    )
    user_parts.append({"type": "text", "text": sol_block})
    messages = [{"role": "user", "content": user_parts}]

    usage_total = {"input_tokens": 0, "output_tokens": 0}
    try:
        text1, u1 = call_ai(api_key, model, messages, system, max_tokens=8192,
                            proxy_url=proxy_url, provider=provider)
    except Exception as e:
        return {"ok": False, "error": str(e)}

    usage_total["input_tokens"] += u1["input_tokens"]
    usage_total["output_tokens"] += u1["output_tokens"]

    raw: Optional[dict] = None
    parse_err: Optional[str] = None
    mech_notes: List[str] = []
    try:
        raw = extract_json_object(text1)
    except Exception as e:
        parse_err = str(e)

    first_pass_mode = "subsection_flow" if subsection_flow else "topic_flow"

    def _cost_rub_now():
        return usage_cost_rub(usage_total["input_tokens"], usage_total["output_tokens"], model, provider)

    if raw is None:
        try:
            t_fix, u_fix = repair_analysis_json_via_llm(api_key, model, proxy_url,
                raw_model_text=text1, reason=f"парсинг: {parse_err}", partial=None, provider=provider)
            usage_total["input_tokens"] += u_fix["input_tokens"]
            usage_total["output_tokens"] += u_fix["output_tokens"]
            raw = extract_json_object(t_fix)
            mech_notes.append("JSON восстановлен повторным вызовом модели")
        except Exception as e2:
            return {
                "ok": False,
                "error": f"Не удалось разобрать JSON: {parse_err}; исправление: {e2}",
                "raw_text": text1[:4000],
                "usage": usage_total,
                "cost_rub": _cost_rub_now(),
            }

    raw, fixes = normalize_analysis_raw(raw)
    mech_notes.extend(fixes)
    val_errs = validate_analysis_raw(raw, mode=first_pass_mode)
    if val_errs:
        try:
            t_fix2, u_fix2 = repair_analysis_json_via_llm(api_key, model, proxy_url,
                raw_model_text=text1, reason="валидация: " + "; ".join(val_errs), partial=raw,
                provider=provider)
            usage_total["input_tokens"] += u_fix2["input_tokens"]
            usage_total["output_tokens"] += u_fix2["output_tokens"]
            raw = extract_json_object(t_fix2)
            raw, fixes2 = normalize_analysis_raw(raw)
            mech_notes.extend(fixes2)
            mech_notes.append("JSON скорректирован повторным вызовом модели")
            val_errs = validate_analysis_raw(raw, mode=first_pass_mode)
        except Exception as e3:
            val_errs.append(f"исправление структуры: {e3}")

    if val_errs:
        return {
            "ok": False,
            "error": "Структура JSON анализа некорректна: " + "; ".join(val_errs),
            "mechanical_fixes": mech_notes,
            "raw_text": text1[:4000],
            "usage": usage_total,
            "cost_rub": _cost_rub_now(),
        }

    # ── Second pass: get topic_id (subsection flow) or use it from first pass (legacy) ──
    if subsection_flow:
        subsection_choice = (raw.get("subsection_choice") or "").strip()
        section_choice = (raw.get("section") or "").strip()
        subsection_topics = _get_topics_for_subsection_choice(topics, subsection_choice, section_choice)
        if not subsection_topics:
            return {
                "ok": False,
                "error": f"Подраздел «{subsection_choice}» не найден в справочнике тем",
                "usage": usage_total,
                "cost_rub": _cost_rub_now(),
            }

        merge_usage = merge_catalogs_with_topic_llm(
            raw, subsection_choice, section_choice, subsection_topics,
            api_key, model, proxy_url,
            task_body_text=body_text, task_images=images,
            provider=provider,
        )
        topic_id = merge_usage.get("topic_id")
        if not topic_id:
            return {
                "ok": False,
                "error": "Второй проход не вернул topic_id",
                "usage": usage_total,
                "cost_rub": _cost_rub_now(),
            }
    else:
        topic_id = raw.get("topic_id")
        try:
            topic_id = int(topic_id)
        except (TypeError, ValueError):
            return {
                "ok": False,
                "error": f"Некорректный topic_id: {topic_id!r}",
                "usage": usage_total,
                "cost_rub": _cost_rub_now(),
            }
        merge_usage = merge_catalogs_with_llm(raw, topic_id, api_key, model, proxy_url, provider=provider)

    mu = merge_usage.get("usage") or {}
    usage_total["input_tokens"] += int(mu.get("input_tokens") or 0)
    usage_total["output_tokens"] += int(mu.get("output_tokens") or 0)

    topic_row = get_curriculum_topic(topic_id)
    if not topic_row:
        return {
            "ok": False,
            "error": f"topic_id {topic_id} не найден в справочнике",
            "usage": usage_total,
            "cost_rub": _cost_rub_now(),
        }

    normalized = merge_usage["normalized"]

    new_solution = raw.get("solution_steps")
    if isinstance(new_solution, list):
        solution_text = "\n".join(f"{i+1}. {s}" for i, s in enumerate(new_solution) if s)
    else:
        solution_text = ""
    fa = raw.get("final_answer") or ""
    if fa:
        solution_text = (solution_text + "\n\nОтвет: " + str(fa)).strip()

    cost_usd = usage_cost_usd(usage_total["input_tokens"], usage_total["output_tokens"], model, provider)
    cost_rub = cost_usd * RUB_PER_USD
    pricing_label = f"{model} (${in_p}/M in, ${out_p}/M out, курс {RUB_PER_USD} ₽/$)"

    usage_json = {
        "primary": u1,
        "merge": merge_usage.get("usage") or {"input_tokens": 0, "output_tokens": 0},
        "totals": usage_total,
        "cost_usd": cost_usd,
        "cost_rub": cost_rub,
        "pricing_note": pricing_label,
    }
    if mech_notes:
        usage_json["json_normalization_notes"] = mech_notes

    result_payload = {
        "topic_id": topic_id,
        "curriculum": {
            "section": topic_row["section"],
            "subsection": topic_row.get("subsection") or "",
            "topic": topic_row["topic"],
            "grade_class": topic_row["grade_class"],
        },
        "normalized": normalized,
        "raw_excerpt": {
            k: raw.get(k)
            for k in ("task_type_by_action", "task_type_by_widget", "answer_format", "bloom_level")
        },
        "task_markup_ref": task_markup_slice_from_raw(raw),
    }

    fallback_json = build_analysis_fallback_snapshot(topic_id, topic_row, raw, normalized, result_payload)

    # _save_data contains everything needed for commit_task_analysis_to_db
    save_data = {
        "task_id": task.get("id"),
        "group_id": task.get("group_id"),
        "group_position": task.get("group_position"),
        "topic_id": topic_id,
        "section": topic_row["section"],
        "subsection": topic_row.get("subsection") or "",
        "topic": topic_row["topic"],
        "grade_class": str(topic_row.get("grade_class") or ""),
        "solution_text": solution_text or None,
        "final_answer": fa or None,
        "raw_json": json.dumps(raw, ensure_ascii=False),
        "result_json": json.dumps(result_payload, ensure_ascii=False),
        "usage_json": json.dumps(usage_json, ensure_ascii=False),
        "fallback_json": fallback_json,
        "normalized_content_elements": normalized.get("content_elements") or [],
        "normalized_skill_steps": normalized.get("skill_steps") or [],
    }

    return {
        "ok": True,
        "result": result_payload,
        "usage": usage_total,
        "cost_rub": cost_rub,
        "cost_usd": cost_usd,
        "usage_detail": usage_json,
        "_save_data": save_data,
    }


def commit_task_analysis_to_db(save_data: dict) -> dict:
    """Persist analysis results to DB. Call after user confirms via 'Сохранить в базу'."""
    task_id = save_data.get("task_id")
    group_id = save_data.get("group_id")
    group_position = save_data.get("group_position")
    topic_id = save_data.get("topic_id")

    task_rowid = get_task_rowid(task_id, group_id, group_position)
    if not task_rowid:
        return {"ok": False, "error": "Не найден rowid задания"}

    clear_task_analysis_links(task_rowid)

    # Upsert content elements and create links
    for el in save_data.get("normalized_content_elements") or []:
        eid = el.get("existing_id")
        if eid is None:
            eid = upsert_content_element(el["label"], topic_id)
        if eid:
            add_task_content_element(task_rowid, eid, el.get("importance"), el.get("stage"))

    # Upsert skills and create step links
    idx_map: Dict[int, int] = {}
    for step in save_data.get("normalized_skill_steps") or []:
        sid = step.get("existing_skill_id")
        if sid is None:
            sid = upsert_skill(step["label"], topic_id)
        if sid:
            idx_map[step["order"]] = sid
            add_task_skill_step(task_rowid, step["order"], sid, step.get("prereq_indices"))

    # Increment prerequisite counters
    for step in save_data.get("normalized_skill_steps") or []:
        sid = idx_map.get(step["order"])
        if not sid:
            continue
        for pi in step.get("prereq_indices") or []:
            if isinstance(pi, int) and pi in idx_map:
                increment_prerequisite(idx_map[pi], sid, 1)

    save_task_analysis(
        task_id, group_id, group_position,
        topic_id,
        save_data.get("section"),
        save_data.get("subsection"),
        save_data.get("topic"),
        save_data.get("grade_class"),
        save_data.get("solution_text"),
        save_data.get("raw_json"),
        save_data.get("result_json"),
        save_data.get("usage_json"),
        save_data.get("fallback_json"),
        suggested_answer=save_data.get("final_answer"),
    )
    return {"ok": True}


def merge_catalogs_with_llm(raw, topic_id, api_key, model, proxy_url, provider="anthropic"):
    from migration_database import get_conn

    conn = get_conn()
    ce_rows = conn.execute("SELECT id, label_display FROM content_element_defs").fetchall()
    sk_rows = conn.execute("SELECT id, label_display FROM skill_defs").fetchall()
    conn.close()

    ce_choices = {r[1]: r[0] for r in ce_rows}
    sk_choices = {r[1]: r[0] for r in sk_rows}
    ce_list = list(ce_choices.keys())
    sk_list = list(sk_choices.keys())

    content_items: List[dict] = []
    for li, el in enumerate(raw.get("content_elements") or []):
        name = (el.get("name") or "").strip()
        if not name:
            continue
        cand = []
        if ce_list:
            for m, score, _ in process.extract(name, ce_list, scorer=fuzz.token_sort_ratio, limit=5):
                if score >= 55:
                    cand.append({"id": ce_choices[m], "label": m, "score": score})
        content_items.append({"local_i": li, "text": name, "stage": el.get("stage"),
                               "importance": el.get("importance"), "candidates": cand})

    skill_items: List[dict] = []
    for ai, ed in enumerate(raw.get("educational_actions") or []):
        act = (ed.get("action") or "").strip()
        if not act:
            continue
        cand = []
        if sk_list:
            for m, score, _ in process.extract(act, sk_list, scorer=fuzz.token_sort_ratio, limit=5):
                if score >= 55:
                    cand.append({"id": sk_choices[m], "label": m, "score": score})
        skill_items.append({"action_index": ai, "text": act,
                            "prerequisite": ed.get("prerequisite") or [], "candidates": cand})

    if not content_items and not skill_items:
        return {"normalized": {"content_elements": [], "skill_steps": []},
                "usage": {"input_tokens": 0, "output_tokens": 0}}

    user = json.dumps({
        "instructions": (
            f"{JSON_OUTPUT_STRICT_RU} Верни только один JSON-объект (без markdown), строго вида:\n"
            '{"content_results":[{"local_i":0,"existing_id":5}...],'
            '"skill_results":[{"action_index":0,"existing_id":12}...]}\n'
            "existing_id — целое id из candidates при совпадении смысла, иначе null."
        ),
        "content": content_items,
        "skills": skill_items,
    }, ensure_ascii=False)

    sys = "\n\n".join([
        JSON_OUTPUT_STRICT_RU,
        "Ты сопоставляешь формулировки с элементами справочника.",
        MERGE_JSON_SCHEMA_TEXT,
        "Ответ — только один JSON-объект.",
    ])

    text, u = call_ai(api_key, model,
        [{"role": "user", "content": [{"type": "text", "text": user}]}],
        sys, max_tokens=4096, proxy_url=proxy_url, provider=provider)

    mapping: dict = {}
    try:
        mapping = extract_json_object(text)
    except Exception as parse_ex:
        try:
            text2, u2 = repair_merge_mapping_via_llm(api_key, model, proxy_url,
                failed_text=text, reason=f"парсинг merge JSON: {parse_ex}", payload_user=user,
                provider=provider)
            u["input_tokens"] += u2["input_tokens"]
            u["output_tokens"] += u2["output_tokens"]
            mapping = extract_json_object(text2)
        except Exception:
            mapping = {}

    mapping, _ = normalize_merge_mapping(mapping)

    cr = {}
    sr = {}
    if isinstance(mapping, dict):
        for row in mapping.get("content_results") or []:
            if row.get("local_i") is not None:
                cr[int(row["local_i"])] = row.get("existing_id")
        for row in mapping.get("skill_results") or []:
            if row.get("action_index") is not None:
                sr[int(row["action_index"])] = row.get("existing_id")

    normalized_content = []
    for it in content_items:
        li = it["local_i"]
        existing = cr.get(li)
        try:
            eid = int(existing) if existing is not None and str(existing).lower() != "null" else None
        except (TypeError, ValueError):
            eid = None
        normalized_content.append({"existing_id": eid, "label": it["text"],
                                    "importance": it.get("importance"), "stage": it.get("stage")})

    skill_steps_out: List[dict] = []

    for it in skill_items:
        ai = it["action_index"]
        existing = sr.get(ai)
        try:
            eid = int(existing) if existing is not None and str(existing).lower() != "null" else None
        except (TypeError, ValueError):
            eid = None
        skill_steps_out.append({
            "order": ai, "existing_skill_id": eid, "label": it["text"],
            "prereq_indices": it.get("prerequisite") or [],
        })

    skill_steps_out.sort(key=lambda x: x["order"])

    return {
        "normalized": {
            "content_elements": normalized_content,
            "skill_steps": skill_steps_out,
        },
        "usage": u,
    }
