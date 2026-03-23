"""
Экспорт данных из локальной SQLite-базы (tasks.db) в JSON-файлы
для последующего импорта в облачный PostgreSQL-сервер.

Запуск: python migration_export_sqlite.py
Результат: папка migration_export/ с файлами:
  - tasks_export.json       — все задания (грузится через /api/import)
  - curriculum_export.json  — темы куррикулума
  - kes_catalog_export.json — справочник КЭС
  - prompt_export.json      — промпт анализа
"""

import sqlite3
import json
import os
import sys

DB_PATH = os.path.join(os.path.dirname(__file__), 'tasks.db')
OUT_DIR = os.path.join(os.path.dirname(__file__), 'migration_export')


def export_all():
    if not os.path.isfile(DB_PATH):
        print(f'Файл базы данных не найден: {DB_PATH}')
        sys.exit(1)

    os.makedirs(OUT_DIR, exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # ── Задания ──────────────────────────────────────────────
    print('Экспорт заданий...')
    rows = conn.execute('SELECT * FROM tasks ORDER BY exam_number ASC, rowid ASC').fetchall()
    tasks = []
    for row in rows:
        d = dict(row)
        # Восстанавливаем вложенные объекты из JSON-строк
        d['images'] = json.loads(d.pop('images_json', '{}') or '{}')
        d['audio'] = json.loads(d.pop('audio_json', '{}') or '{}')
        d['attachments'] = json.loads(d.pop('attachments_json', '{}') or '{}')
        # Убираем служебные поля, которые не нужны при импорте
        d.pop('imported_at', None)
        d.pop('rowid', None)
        tasks.append(d)

    tasks_path = os.path.join(OUT_DIR, 'tasks_export.json')
    with open(tasks_path, 'w', encoding='utf-8') as f:
        json.dump(tasks, f, ensure_ascii=False, indent=2)
    print(f'  Сохранено {len(tasks)} заданий → {tasks_path}')

    # ── Куррикулум ───────────────────────────────────────────
    print('Экспорт тем куррикулума...')
    try:
        rows = conn.execute(
            'SELECT subject, section, subsection, topic, topic_description, grade_class '
            'FROM math_curriculum_topics ORDER BY id ASC'
        ).fetchall()
        curriculum = [dict(r) for r in rows]
    except sqlite3.OperationalError:
        curriculum = []
        print('  Таблица math_curriculum_topics не найдена — пропускаем.')

    curr_path = os.path.join(OUT_DIR, 'curriculum_export.json')
    with open(curr_path, 'w', encoding='utf-8') as f:
        json.dump(curriculum, f, ensure_ascii=False, indent=2)
    print(f'  Сохранено {len(curriculum)} тем → {curr_path}')

    # ── Каталог КЭС ──────────────────────────────────────────
    print('Экспорт каталога КЭС...')
    try:
        rows = conn.execute(
            'SELECT subject, kes_text FROM kes_catalog ORDER BY subject, kes_text'
        ).fetchall()
        kes = [dict(r) for r in rows]
    except sqlite3.OperationalError:
        kes = []
        print('  Таблица kes_catalog не найдена — пропускаем.')

    kes_path = os.path.join(OUT_DIR, 'kes_catalog_export.json')
    with open(kes_path, 'w', encoding='utf-8') as f:
        json.dump(kes, f, ensure_ascii=False, indent=2)
    print(f'  Сохранено {len(kes)} записей КЭС → {kes_path}')

    # ── Промпт анализа ───────────────────────────────────────
    print('Экспорт промпта анализа...')
    try:
        row = conn.execute('SELECT body, updated_at FROM analysis_prompts WHERE id = 1').fetchone()
        prompt = dict(row) if row else {'body': '', 'updated_at': None}
    except sqlite3.OperationalError:
        prompt = {'body': '', 'updated_at': None}
        print('  Таблица analysis_prompts не найдена — пропускаем.')

    prompt_path = os.path.join(OUT_DIR, 'prompt_export.json')
    with open(prompt_path, 'w', encoding='utf-8') as f:
        json.dump(prompt, f, ensure_ascii=False, indent=2)
    print(f'  Промпт сохранён → {prompt_path}')

    conn.close()
    print()
    print('Экспорт завершён. Файлы в папке:', OUT_DIR)
    print()
    print('Следующий шаг — импорт в облако:')
    print('  1. tasks_export.json      → загрузите через /api/import в веб-интерфейсе')
    print('  2. curriculum_export.json → сконвертируйте в XLSX и загрузите через /api/curriculum/import-xlsx')
    print('     ИЛИ используйте migration_import_curriculum.py (если он есть)')
    print('  3. КЭС пересоберётся автоматически после импорта заданий')
    print('  4. prompt_export.json     → скопируйте body в редактор промптов на сервере')


if __name__ == '__main__':
    export_all()
