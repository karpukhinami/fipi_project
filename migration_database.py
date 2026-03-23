import json
import os
import base64
from datetime import datetime

import psycopg2
import psycopg2.extras

# Строка подключения берётся из переменной окружения DATABASE_URL.
# Пример: postgresql://user:password@host:5432/dbname
DATABASE_URL = os.environ.get('DATABASE_URL', '')


def default_analysis_prompt_file_path():
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), 'prompts', 'default_analysis_system_prompt.txt')


def read_default_analysis_prompt_file():
    path = default_analysis_prompt_file_path()
    if not os.path.isfile(path):
        return None
    with open(path, encoding='utf-8') as f:
        return f.read().replace('\r\n', '\n')


def _prompts_dir():
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), 'prompts')


def read_analysis_json_schema_file():
    path = os.path.join(_prompts_dir(), 'analysis_json_schema.txt')
    if not os.path.isfile(path):
        return None
    with open(path, encoding='utf-8') as f:
        return f.read().replace('\r\n', '\n')


def read_analysis_field_rules_file():
    path = os.path.join(_prompts_dir(), 'analysis_field_rules.txt')
    if not os.path.isfile(path):
        return None
    with open(path, encoding='utf-8') as f:
        return f.read().replace('\r\n', '\n')


# ---------------------------------------------------------------------------
# Обёртка над psycopg2 — имитирует интерфейс sqlite3 (conn.execute, fetchall, etc.)
# ---------------------------------------------------------------------------

class _PGConn:
    """Тонкая обёртка над psycopg2 connection.

    Предоставляет метод execute() аналогичный SQLite-соединению.
    Все курсоры используют DictCursor — поддерживает как row['col'], так и row[0].
    Плейсхолдеры ? автоматически заменяются на %s.
    """

    def __init__(self, raw_conn):
        self._conn = raw_conn

    @staticmethod
    def _fix_sql(sql):
        return sql.replace('?', '%s')

    def execute(self, sql, params=None):
        cur = self._conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute(self._fix_sql(sql), params or ())
        return cur

    def executemany(self, sql, params_list):
        cur = self._conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.executemany(self._fix_sql(sql), params_list)
        return cur

    def commit(self):
        self._conn.commit()

    def close(self):
        self._conn.close()


def get_conn():
    conn = psycopg2.connect(DATABASE_URL)
    return _PGConn(conn)


def _get_existing_columns(conn, table_name):
    """Возвращает набор имён колонок таблицы через information_schema."""
    rows = conn.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = 'public' AND table_name = %s",
        (table_name,)
    ).fetchall()
    return {row[0] for row in rows}


# ---------------------------------------------------------------------------
# Инициализация схемы БД
# ---------------------------------------------------------------------------

def init_db():
    conn = get_conn()

    # Таблица заданий. rowid — явный суррогатный ключ (аналог SQLite rowid).
    conn.execute('''
        CREATE TABLE IF NOT EXISTS tasks (
            rowid               BIGSERIAL NOT NULL UNIQUE,
            id                  TEXT NOT NULL DEFAULT '',
            task_number         TEXT,
            group_id            TEXT NOT NULL DEFAULT '',
            group_position      TEXT NOT NULL DEFAULT '',
            exam_type           TEXT,
            subject             TEXT,
            kes                 TEXT,
            answer_type         TEXT,
            answer_format       TEXT,
            answer_unit         TEXT,
            answer              TEXT,
            solution            TEXT,
            text                TEXT,
            formatted_text      TEXT,
            html                TEXT,
            images_json         TEXT,
            audio_json          TEXT,
            imported_at         TEXT,
            manually_edited     INTEGER NOT NULL DEFAULT 0,
            edited_at           TEXT,
            exam_number         TEXT,
            topic_name          TEXT,
            category_name       TEXT,
            category_id         TEXT,
            source              TEXT,
            criteria            TEXT,
            attachments_json    TEXT,
            analyzed_topic_id   INTEGER,
            analyzed_section    TEXT,
            analyzed_subsection TEXT,
            analyzed_topic      TEXT,
            analyzed_grade_class TEXT,
            analysis_solution   TEXT,
            analysis_raw_json   TEXT,
            analysis_result_json TEXT,
            analysis_usage_json TEXT,
            analysis_fallback_json TEXT,
            PRIMARY KEY (id, group_id, group_position)
        )
    ''')

    conn.execute('CREATE INDEX IF NOT EXISTS idx_exam_number ON tasks(exam_number)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_exam_type  ON tasks(exam_type)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_subject    ON tasks(subject)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_kes        ON tasks(kes)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_group_id   ON tasks(group_id)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_tasks_analyzed_topic ON tasks(analyzed_topic_id)')

    # Справочник КЭС
    conn.execute('''
        CREATE TABLE IF NOT EXISTS kes_catalog (
            id          SERIAL PRIMARY KEY,
            subject     TEXT NOT NULL,
            kes_text    TEXT NOT NULL,
            UNIQUE(subject, kes_text)
        )
    ''')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_kc_subject ON kes_catalog(subject)')

    # Куррикулум математики
    conn.execute('''
        CREATE TABLE IF NOT EXISTS math_curriculum_topics (
            id                  SERIAL PRIMARY KEY,
            subject             TEXT NOT NULL,
            section             TEXT NOT NULL,
            subsection          TEXT,
            topic               TEXT NOT NULL,
            topic_description   TEXT,
            grade_class         TEXT NOT NULL
        )
    ''')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_mct_subject ON math_curriculum_topics(subject)')

    # Промпт анализа (единственная запись с id=1)
    conn.execute('''
        CREATE TABLE IF NOT EXISTS analysis_prompts (
            id          INTEGER PRIMARY KEY CHECK (id = 1),
            body        TEXT NOT NULL,
            updated_at  TEXT
        )
    ''')

    # Справочник элементов содержания
    conn.execute('''
        CREATE TABLE IF NOT EXISTS content_element_defs (
            id                  SERIAL PRIMARY KEY,
            label_normalized    TEXT NOT NULL UNIQUE,
            label_display       TEXT NOT NULL,
            default_topic_id    INTEGER REFERENCES math_curriculum_topics(id)
        )
    ''')

    # Справочник навыков
    conn.execute('''
        CREATE TABLE IF NOT EXISTS skill_defs (
            id                  SERIAL PRIMARY KEY,
            label_normalized    TEXT NOT NULL UNIQUE,
            label_display       TEXT NOT NULL,
            default_topic_id    INTEGER REFERENCES math_curriculum_topics(id)
        )
    ''')

    # Связи навыков (пресеты)
    conn.execute('''
        CREATE TABLE IF NOT EXISTS skill_prerequisites (
            from_skill_id   INTEGER NOT NULL REFERENCES skill_defs(id),
            to_skill_id     INTEGER NOT NULL REFERENCES skill_defs(id),
            weight          INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (from_skill_id, to_skill_id)
        )
    ''')

    # Привязка элементов содержания к заданиям
    conn.execute('''
        CREATE TABLE IF NOT EXISTS task_content_elements (
            id          SERIAL PRIMARY KEY,
            task_rowid  BIGINT NOT NULL,
            element_id  INTEGER NOT NULL REFERENCES content_element_defs(id),
            importance  TEXT,
            stage       TEXT
        )
    ''')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_tce_task ON task_content_elements(task_rowid)')

    # Привязка навыков-шагов к заданиям
    conn.execute('''
        CREATE TABLE IF NOT EXISTS task_skill_steps (
            id              SERIAL PRIMARY KEY,
            task_rowid      BIGINT NOT NULL,
            step_order      INTEGER NOT NULL,
            skill_id        INTEGER NOT NULL REFERENCES skill_defs(id),
            prereq_indices  TEXT,
            UNIQUE(task_rowid, step_order)
        )
    ''')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_tss_task ON task_skill_steps(task_rowid)')

    conn.commit()
    conn.close()

    _ensure_kes_catalog()
    _ensure_analysis_defaults()


# ---------------------------------------------------------------------------
# Куррикулум
# ---------------------------------------------------------------------------

def get_curriculum_topics(subject=None):
    conn = get_conn()
    if subject:
        rows = conn.execute(
            '''SELECT id, subject, section, subsection, topic, topic_description, grade_class
               FROM math_curriculum_topics WHERE lower(subject) = lower(%s)
               ORDER BY section, subsection, topic''',
            (subject,),
        ).fetchall()
    else:
        rows = conn.execute(
            '''SELECT id, subject, section, subsection, topic, topic_description, grade_class
               FROM math_curriculum_topics
               ORDER BY subject, section, subsection, topic''',
        ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_curriculum_topic(topic_id):
    conn = get_conn()
    row = conn.execute(
        'SELECT * FROM math_curriculum_topics WHERE id = %s', (int(topic_id),)
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def update_curriculum_topic(topic_id, fields):
    allowed = {'section', 'subsection', 'topic', 'topic_description', 'grade_class', 'subject'}
    updates = {k: v for k, v in fields.items() if k in allowed}
    if not updates:
        return False
    tid = int(topic_id)
    conn = get_conn()
    set_clause = ', '.join(f'{k} = %s' for k in updates)
    conn.execute(
        f'UPDATE math_curriculum_topics SET {set_clause} WHERE id = %s',
        list(updates.values()) + [tid],
    )
    conn.execute(
        '''UPDATE tasks SET
            analyzed_section = (SELECT section FROM math_curriculum_topics WHERE id = %s),
            analyzed_subsection = (SELECT subsection FROM math_curriculum_topics WHERE id = %s),
            analyzed_topic = (SELECT topic FROM math_curriculum_topics WHERE id = %s),
            analyzed_grade_class = (SELECT grade_class FROM math_curriculum_topics WHERE id = %s)
           WHERE analyzed_topic_id = %s''',
        (tid, tid, tid, tid, tid),
    )
    conn.commit()
    conn.close()
    return True


def add_curriculum_topic(subject, section, subsection, topic, topic_description, grade_class):
    conn = get_conn()
    cur = conn.execute(
        '''INSERT INTO math_curriculum_topics
           (subject, section, subsection, topic, topic_description, grade_class)
           VALUES (%s,%s,%s,%s,%s,%s) RETURNING id''',
        (subject, section, subsection or '', topic, topic_description or '', grade_class),
    )
    new_id = cur.fetchone()[0]
    conn.commit()
    conn.close()
    return new_id


def delete_curriculum_topic(topic_id):
    tid = int(topic_id)
    conn = get_conn()
    n = conn.execute(
        'SELECT COUNT(*) FROM tasks WHERE analyzed_topic_id = %s', (tid,)
    ).fetchone()[0]
    if n > 0:
        conn.close()
        return False, 'Есть задания, привязанные к этой теме'
    conn.execute('DELETE FROM math_curriculum_topics WHERE id = %s', (tid,))
    conn.commit()
    conn.close()
    return True, None


def import_math_curriculum_from_xlsx(path, replace=True):
    try:
        import openpyxl
    except ImportError as e:
        raise RuntimeError('Нужен пакет openpyxl: pip install openpyxl') from e

    path = os.path.abspath(path)
    if not os.path.isfile(path):
        raise FileNotFoundError(path)

    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    sh = wb.active
    rows = list(sh.iter_rows(values_only=True))
    if len(rows) < 2:
        wb.close()
        return 0
    data_rows = rows[1:]

    conn = get_conn()
    if replace:
        conn.execute(
            '''UPDATE tasks SET analyzed_topic_id = NULL, analyzed_section = NULL,
               analyzed_subsection = NULL, analyzed_topic = NULL, analyzed_grade_class = NULL
               WHERE analyzed_topic_id IS NOT NULL'''
        )
        conn.execute('UPDATE content_element_defs SET default_topic_id = NULL')
        conn.execute('UPDATE skill_defs SET default_topic_id = NULL')
        conn.execute('DELETE FROM math_curriculum_topics')

    inserted = 0
    for r in data_rows:
        if not r:
            continue
        cells = list(r)
        while len(cells) < 6:
            cells.append(None)
        subject = cells[0]
        subject = str(subject).strip() if subject is not None else ''
        if not subject:
            subject = 'Математика'
        section = str(cells[1] or '').strip()
        subsection = str(cells[2] or '').strip()
        topic = str(cells[3] or '').strip()
        grade = cells[4]
        grade = str(grade).strip() if grade is not None else ''
        desc = str(cells[5] or '').strip()
        if not topic:
            continue
        conn.execute(
            '''INSERT INTO math_curriculum_topics
               (subject, section, subsection, topic, topic_description, grade_class)
               VALUES (%s,%s,%s,%s,%s,%s)''',
            (subject, section, subsection, topic, desc, grade),
        )
        inserted += 1
    conn.commit()
    conn.close()
    wb.close()
    return inserted


# ---------------------------------------------------------------------------
# Промпт анализа
# ---------------------------------------------------------------------------

def get_analysis_prompt():
    conn = get_conn()
    row = conn.execute('SELECT body, updated_at FROM analysis_prompts WHERE id = 1').fetchone()
    conn.close()
    if not row:
        return '', None
    return row[0] or '', row[1]


def set_analysis_prompt(body):
    now = datetime.now().isoformat()
    conn = get_conn()
    conn.execute(
        'UPDATE analysis_prompts SET body = %s, updated_at = %s WHERE id = 1',
        (body or '', now),
    )
    conn.commit()
    conn.close()
    return True


# ---------------------------------------------------------------------------
# Анализ заданий: привязки к справочникам
# ---------------------------------------------------------------------------

def get_task_rowid(task_id, group_id, group_position):
    conn = get_conn()
    row = conn.execute(
        'SELECT rowid FROM tasks WHERE id = %s AND group_id = %s AND group_position = %s',
        (task_id or '', group_id or '', group_position or ''),
    ).fetchone()
    conn.close()
    return row[0] if row else None


def clear_task_analysis_links(task_rowid):
    conn = get_conn()
    conn.execute('DELETE FROM task_content_elements WHERE task_rowid = %s', (task_rowid,))
    conn.execute('DELETE FROM task_skill_steps WHERE task_rowid = %s', (task_rowid,))
    conn.commit()
    conn.close()


def clear_task_analysis(task_id, group_id, group_position):
    task_rowid = get_task_rowid(task_id, group_id, group_position)
    if not task_rowid:
        return False
    clear_task_analysis_links(task_rowid)
    conn = get_conn()
    conn.execute(
        '''UPDATE tasks SET
            analyzed_topic_id = NULL,
            analyzed_section = NULL,
            analyzed_subsection = NULL,
            analyzed_topic = NULL,
            analyzed_grade_class = NULL,
            analysis_solution = NULL,
            analysis_raw_json = NULL,
            analysis_result_json = NULL,
            analysis_usage_json = NULL,
            analysis_fallback_json = NULL
           WHERE id = %s AND group_id = %s AND group_position = %s''',
        (task_id or '', group_id or '', group_position or ''),
    )
    conn.commit()
    conn.close()
    return True


def upsert_content_element(label_display, default_topic_id=None):
    import re
    s = (label_display or '').strip()
    if not s:
        return None
    key = re.sub(r'\s+', ' ', s.lower())
    conn = get_conn()
    row = conn.execute(
        'SELECT id FROM content_element_defs WHERE label_normalized = %s', (key,)
    ).fetchone()
    if row:
        rid = row[0]
        conn.close()
        return rid
    cur = conn.execute(
        '''INSERT INTO content_element_defs (label_normalized, label_display, default_topic_id)
           VALUES (%s,%s,%s) RETURNING id''',
        (key, s, default_topic_id),
    )
    rid = cur.fetchone()[0]
    conn.commit()
    conn.close()
    return rid


def upsert_skill(label_display, default_topic_id=None):
    import re
    s = (label_display or '').strip()
    if not s:
        return None
    key = re.sub(r'\s+', ' ', s.lower())
    conn = get_conn()
    row = conn.execute(
        'SELECT id FROM skill_defs WHERE label_normalized = %s', (key,)
    ).fetchone()
    if row:
        rid = row[0]
        conn.close()
        return rid
    cur = conn.execute(
        '''INSERT INTO skill_defs (label_normalized, label_display, default_topic_id)
           VALUES (%s,%s,%s) RETURNING id''',
        (key, s, default_topic_id),
    )
    rid = cur.fetchone()[0]
    conn.commit()
    conn.close()
    return rid


def add_task_content_element(task_rowid, element_id, importance, stage):
    conn = get_conn()
    conn.execute(
        '''INSERT INTO task_content_elements (task_rowid, element_id, importance, stage)
           VALUES (%s,%s,%s,%s)''',
        (task_rowid, element_id, importance or '', stage or ''),
    )
    conn.commit()
    conn.close()


def add_task_skill_step(task_rowid, step_order, skill_id, prereq_indices):
    conn = get_conn()
    conn.execute(
        '''INSERT INTO task_skill_steps (task_rowid, step_order, skill_id, prereq_indices)
           VALUES (%s,%s,%s,%s)
           ON CONFLICT (task_rowid, step_order) DO UPDATE SET
               skill_id = EXCLUDED.skill_id,
               prereq_indices = EXCLUDED.prereq_indices''',
        (task_rowid, step_order, skill_id, json.dumps(prereq_indices or [], ensure_ascii=False)),
    )
    conn.commit()
    conn.close()


def increment_prerequisite(from_skill_id, to_skill_id, delta=1):
    if from_skill_id == to_skill_id or not from_skill_id or not to_skill_id:
        return
    conn = get_conn()
    row = conn.execute(
        'SELECT weight FROM skill_prerequisites WHERE from_skill_id = %s AND to_skill_id = %s',
        (from_skill_id, to_skill_id),
    ).fetchone()
    if row:
        conn.execute(
            'UPDATE skill_prerequisites SET weight = weight + %s WHERE from_skill_id = %s AND to_skill_id = %s',
            (delta, from_skill_id, to_skill_id),
        )
    else:
        conn.execute(
            'INSERT INTO skill_prerequisites (from_skill_id, to_skill_id, weight) VALUES (%s,%s,%s)',
            (from_skill_id, to_skill_id, delta),
        )
    conn.commit()
    conn.close()


def enrich_task_analysis_display(task_dict):
    if not task_dict:
        return
    tid = task_dict.get('analyzed_topic_id')
    if tid is None:
        task_dict['analysis_topic_linked'] = True
        return
    try:
        tid_int = int(tid)
    except (TypeError, ValueError):
        task_dict['analysis_topic_linked'] = False
        return
    conn = get_conn()
    row = conn.execute(
        'SELECT 1 FROM math_curriculum_topics WHERE id = %s', (tid_int,)
    ).fetchone()
    conn.close()
    task_dict['analysis_topic_linked'] = row is not None


def get_curriculum_subsections(subject=None):
    """Returns unique (section, subsection) pairs with topic count."""
    conn = get_conn()
    if subject:
        rows = conn.execute(
            '''SELECT section, subsection, COUNT(*) as topic_count
               FROM math_curriculum_topics
               WHERE subject = %s
               GROUP BY section, subsection
               ORDER BY section, subsection''',
            (subject,),
        ).fetchall()
    else:
        rows = conn.execute(
            '''SELECT section, subsection, COUNT(*) as topic_count
               FROM math_curriculum_topics
               GROUP BY section, subsection
               ORDER BY section, subsection'''
        ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_topics_by_subsection(section, subsection):
    """Returns all topics for a given (section, subsection) pair."""
    conn = get_conn()
    subsection_val = subsection or ''
    rows = conn.execute(
        '''SELECT id, subject, section, subsection, topic, topic_description, grade_class
           FROM math_curriculum_topics
           WHERE section = %s
             AND (subsection = %s OR (%s = '' AND (subsection IS NULL OR subsection = '')))
           ORDER BY grade_class, topic''',
        (section, subsection_val, subsection_val),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_skills_for_catalog(subject=None, section=None, subsection=None, topic_id=None):
    """Returns skill_defs with topic hierarchy, filtered by curriculum position."""
    conn = get_conn()
    where_parts = []
    params = []
    if topic_id is not None:
        where_parts.append('sd.default_topic_id = %s')
        params.append(topic_id)
    else:
        if subject is not None:
            where_parts.append('mct.subject = %s')
            params.append(subject)
        if section is not None:
            where_parts.append('mct.section = %s')
            params.append(section)
        if subsection is not None:
            subsection_val = subsection or ''
            where_parts.append(
                "(mct.subsection = %s OR (%s = '' AND (mct.subsection IS NULL OR mct.subsection = '')))"
            )
            params.extend([subsection_val, subsection_val])
    base_query = (
        'SELECT sd.id, sd.label_display,'
        ' mct.subject, mct.section, mct.subsection, mct.topic, mct.grade_class'
        ' FROM skill_defs sd'
        ' LEFT JOIN math_curriculum_topics mct ON mct.id = sd.default_topic_id'
    )
    if where_parts:
        base_query += ' WHERE ' + ' AND '.join(where_parts)
    base_query += ' ORDER BY mct.section, mct.subsection, mct.topic, sd.label_display'
    rows = conn.execute(base_query, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_content_elements_for_catalog(subject=None, section=None, subsection=None, topic_id=None):
    """Returns content_element_defs with topic hierarchy, filtered by curriculum position."""
    conn = get_conn()
    where_parts = []
    params = []
    if topic_id is not None:
        where_parts.append('ced.default_topic_id = %s')
        params.append(topic_id)
    else:
        if subject is not None:
            where_parts.append('mct.subject = %s')
            params.append(subject)
        if section is not None:
            where_parts.append('mct.section = %s')
            params.append(section)
        if subsection is not None:
            subsection_val = subsection or ''
            where_parts.append(
                "(mct.subsection = %s OR (%s = '' AND (mct.subsection IS NULL OR mct.subsection = '')))"
            )
            params.extend([subsection_val, subsection_val])
    base_query = (
        'SELECT ced.id, ced.label_display,'
        ' mct.subject, mct.section, mct.subsection, mct.topic, mct.grade_class'
        ' FROM content_element_defs ced'
        ' LEFT JOIN math_curriculum_topics mct ON mct.id = ced.default_topic_id'
    )
    if where_parts:
        base_query += ' WHERE ' + ' AND '.join(where_parts)
    base_query += ' ORDER BY mct.section, mct.subsection, mct.topic, ced.label_display'
    rows = conn.execute(base_query, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def save_task_analysis(
    task_id,
    group_id,
    group_position,
    analyzed_topic_id,
    analyzed_section,
    analyzed_subsection,
    analyzed_topic,
    analyzed_grade_class,
    analysis_solution,
    analysis_raw_json,
    analysis_result_json,
    analysis_usage_json,
    analysis_fallback_json=None,
    suggested_answer=None,
):
    conn = get_conn()
    sol = (analysis_solution or '').strip()
    ans = (suggested_answer or '').strip()
    conn.execute(
        '''UPDATE tasks SET
            analyzed_topic_id = %s,
            analyzed_section = %s,
            analyzed_subsection = %s,
            analyzed_topic = %s,
            analyzed_grade_class = %s,
            analysis_solution = %s,
            analysis_raw_json = %s,
            analysis_result_json = %s,
            analysis_usage_json = %s,
            analysis_fallback_json = %s,
            solution = CASE WHEN %s != '' THEN %s ELSE solution END,
            answer = CASE WHEN (answer IS NULL OR answer = '') AND %s != '' THEN %s ELSE answer END
           WHERE id = %s AND group_id = %s AND group_position = %s''',
        (
            analyzed_topic_id,
            analyzed_section,
            analyzed_subsection,
            analyzed_topic,
            analyzed_grade_class,
            analysis_solution,
            analysis_raw_json,
            analysis_result_json,
            analysis_usage_json,
            analysis_fallback_json,
            sol,
            sol,
            ans,
            ans,
            task_id or '',
            group_id or '',
            group_position or '',
        ),
    )
    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# КЭС-каталог
# ---------------------------------------------------------------------------

def _ensure_kes_catalog():
    conn = get_conn()
    count = conn.execute('SELECT COUNT(*) FROM kes_catalog').fetchone()[0]
    if count == 0:
        _rebuild_kes_catalog_conn(conn)
    conn.commit()
    conn.close()


def _rebuild_kes_catalog_conn(conn):
    rows = conn.execute(
        "SELECT DISTINCT subject, kes FROM tasks WHERE kes != '' AND kes IS NOT NULL"
    ).fetchall()
    for subject, kes_field in rows:
        subject = (subject or '').strip()
        for part in kes_field.split('\n'):
            part = part.strip()
            if part:
                conn.execute(
                    'INSERT INTO kes_catalog (subject, kes_text) VALUES (%s,%s) ON CONFLICT DO NOTHING',
                    (subject, part)
                )


def rebuild_kes_catalog():
    conn = get_conn()
    conn.execute('DELETE FROM kes_catalog')
    _rebuild_kes_catalog_conn(conn)
    conn.commit()
    conn.close()


def get_kes_catalog(subject=None):
    conn = get_conn()
    if subject:
        rows = conn.execute(
            'SELECT subject, kes_text FROM kes_catalog WHERE subject = %s ORDER BY kes_text',
            (subject,)
        ).fetchall()
    else:
        rows = conn.execute(
            'SELECT subject, kes_text FROM kes_catalog ORDER BY subject, kes_text'
        ).fetchall()
    conn.close()
    return [{'subject': r[0], 'kes_text': r[1]} for r in rows]


# ---------------------------------------------------------------------------
# Вспомогательные нормализаторы
# ---------------------------------------------------------------------------

def _normalize_kes(kes):
    if isinstance(kes, list):
        return '\n'.join(str(x).strip() for x in kes if x)
    return str(kes or '')


def _normalize_source(source):
    if source is None:
        return ''
    return json.dumps(source, ensure_ascii=False) if isinstance(source, (dict, list)) else str(source)


def _import_exists(conn, tid, group_id, group_position):
    if tid:
        row = conn.execute(
            'SELECT 1 FROM tasks WHERE id = %s AND group_id = %s AND group_position = %s',
            (tid, group_id, group_position)
        ).fetchone()
    else:
        row = conn.execute(
            'SELECT 1 FROM tasks WHERE id = %s AND group_id = %s AND group_position = %s',
            ('', group_id, group_position or '0')
        ).fetchone()
    return row is not None


# ---------------------------------------------------------------------------
# Импорт заданий
# ---------------------------------------------------------------------------

def import_tasks(tasks_list):
    conn = get_conn()
    added = 0
    updated = 0
    skipped = 0
    now = datetime.now().isoformat()

    for t in tasks_list:
        tid = t.get('id', '') or ''
        group_id = t.get('group_id', '') or ''
        group_position = t.get('group_position', '') or ''
        if not tid:
            group_position = group_position or '0'
        text = t.get('text', '') or ''
        formatted_text = t.get('formatted_text', '') or ''

        exists = _import_exists(conn, tid, group_id, group_position)
        action = 'update' if exists else 'insert'

        kes_val = _normalize_kes(t.get('kes'))
        source_val = _normalize_source(t.get('source'))
        criteria_val = t.get('criteria', '') or ''
        if isinstance(criteria_val, (dict, list)):
            criteria_val = json.dumps(criteria_val, ensure_ascii=False)
        exam_number = t.get('exam_number', '') or ''
        topic_name = t.get('topic_name', '') or ''
        category_name = t.get('category_name', '') or ''
        category_id = t.get('category_id', '') or ''
        attachments_val = json.dumps(t.get('attachments', t.get('attachment', {})) or {}, ensure_ascii=False)

        if action == 'insert':
            conn.execute('''
                INSERT INTO tasks (
                    id, task_number, group_id, group_position,
                    exam_type, subject, kes, answer_type,
                    answer_format, answer_unit, answer, solution,
                    text, formatted_text, html, images_json, audio_json, imported_at,
                    manually_edited, edited_at,
                    exam_number, topic_name, category_name, category_id, source, criteria, attachments_json
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,0,NULL,%s,%s,%s,%s,%s,%s,%s)
            ''', (
                tid,
                t.get('task_number', '') or '',
                group_id,
                group_position,
                t.get('exam_type', '') or '',
                t.get('subject', '') or '',
                kes_val,
                t.get('answer_type', '') or '',
                t.get('answer_format', '') or '',
                t.get('answer_unit', '') or '',
                t.get('answer', '') or '',
                t.get('solution', '') or '',
                text,
                formatted_text,
                t.get('html', '') or '',
                json.dumps(t.get('images', {}) or {}, ensure_ascii=False),
                json.dumps(t.get('audio', {}) or {}, ensure_ascii=False),
                now,
                exam_number, topic_name, category_name, category_id, source_val, criteria_val, attachments_val,
            ))
            added += 1

        elif action == 'update':
            conn.execute('''
                UPDATE tasks SET
                    task_number = %s, exam_type = %s, subject = %s, kes = %s,
                    answer_type = %s, answer_format = %s, answer_unit = %s,
                    answer = %s, solution = %s, text = %s, formatted_text = %s, html = %s,
                    images_json = %s, audio_json = %s, imported_at = %s,
                    exam_number = %s, topic_name = %s, category_name = %s, category_id = %s,
                    source = %s, criteria = %s,
                    attachments_json = %s, manually_edited = 0, edited_at = NULL
                WHERE id = %s AND group_id = %s AND group_position = %s
            ''', (
                t.get('task_number', '') or '',
                t.get('exam_type', '') or '',
                t.get('subject', '') or '',
                kes_val,
                t.get('answer_type', '') or '',
                t.get('answer_format', '') or '',
                t.get('answer_unit', '') or '',
                t.get('answer', '') or '',
                t.get('solution', '') or '',
                text,
                formatted_text,
                t.get('html', '') or '',
                json.dumps(t.get('images', {}) or {}, ensure_ascii=False),
                json.dumps(t.get('audio', {}) or {}, ensure_ascii=False),
                now,
                exam_number, topic_name, category_name, category_id, source_val, criteria_val, attachments_val,
                tid, group_id, group_position,
            ))
            updated += 1

    conn.commit()
    _rebuild_kes_catalog_conn(conn)
    conn.commit()
    conn.close()
    return added, updated, skipped


# ---------------------------------------------------------------------------
# Чтение заданий
# ---------------------------------------------------------------------------

def get_tasks(filters=None, page=1, page_size=50):
    filters = filters or {}
    conditions = []
    params = []

    if filters.get('exam_type'):
        conditions.append('exam_type = %s')
        params.append(filters['exam_type'])
    if filters.get('subject'):
        conditions.append('subject = %s')
        params.append(filters['subject'])
    if filters.get('kes'):
        conditions.append('kes LIKE %s')
        params.append(f"%{filters['kes']}%")
    if filters.get('answer_type'):
        conditions.append('answer_type = %s')
        params.append(filters['answer_type'])
    if filters.get('group_id'):
        conditions.append('group_id = %s')
        params.append(filters['group_id'])
    if filters.get('task_number'):
        conditions.append('task_number LIKE %s')
        params.append(f"%{filters['task_number']}%")
    if filters.get('search'):
        conditions.append('text LIKE %s')
        params.append(f"%{filters['search']}%")
    if filters.get('exam_number'):
        conditions.append('exam_number = %s')
        params.append(filters['exam_number'])

    where = ('WHERE ' + ' AND '.join(conditions)) if conditions else ''

    conn = get_conn()
    total = conn.execute(f'SELECT COUNT(*) FROM tasks {where}', params).fetchone()[0]

    offset = (page - 1) * page_size
    task_num_q = (filters.get('task_number') or '').strip()

    if task_num_q:
        order_sql = 'ORDER BY CASE WHEN task_number LIKE %s THEN 0 ELSE 1 END, exam_number ASC, imported_at DESC, rowid ASC'
        query_params = params + [f"{task_num_q}%", page_size, offset]
    else:
        order_sql = 'ORDER BY exam_number ASC, imported_at DESC, rowid ASC'
        query_params = params + [page_size, offset]

    rows = conn.execute(
        f'''SELECT id, task_number, group_id, group_position,
                   exam_type, subject, kes, answer_type,
                   answer_format, answer_unit, answer, solution,
                   text, formatted_text, imported_at, manually_edited, edited_at,
                   exam_number, topic_name, category_name
            FROM tasks {where}
            {order_sql}
            LIMIT %s OFFSET %s''',
        query_params
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows], total


def get_task_by_rowid(rowid):
    conn = get_conn()
    row = conn.execute('SELECT * FROM tasks WHERE rowid = %s', (rowid,)).fetchone()
    conn.close()
    if not row:
        return None
    d = dict(row)
    d['images'] = json.loads(d.pop('images_json', '{}') or '{}')
    d['audio'] = json.loads(d.pop('audio_json', '{}') or '{}')
    d['attachments'] = json.loads(d.pop('attachments_json', '{}') or '{}')
    enrich_task_analysis_display(d)
    return d


def get_task_by_id_params(task_id, group_id, group_position):
    conn = get_conn()
    # В PostgreSQL rowid — обычная колонка, включена в SELECT *
    row = conn.execute(
        'SELECT * FROM tasks WHERE id = %s AND group_id = %s AND group_position = %s',
        (task_id, group_id, group_position)
    ).fetchone()
    conn.close()
    if not row:
        return None
    d = dict(row)
    d['images'] = json.loads(d.pop('images_json', '{}') or '{}')
    d['audio'] = json.loads(d.pop('audio_json', '{}') or '{}')
    d['attachments'] = json.loads(d.pop('attachments_json', '{}') or '{}')
    enrich_task_analysis_display(d)
    return d


def get_attachment_data(task_id, group_id, group_position, filename):
    conn = get_conn()
    row = conn.execute(
        'SELECT attachments_json FROM tasks WHERE id = %s AND group_id = %s AND group_position = %s',
        (task_id or '', group_id or '', group_position or '')
    ).fetchone()
    conn.close()
    if not row:
        return None
    attachments = json.loads(row[0] or '{}')
    att = attachments.get(filename) if isinstance(attachments, dict) else None
    if not att or not att.get('data'):
        return None
    try:
        data = base64.b64decode(att['data'])
    except Exception:
        return None
    return data, att.get('mime', 'application/octet-stream'), filename


def get_group_wrapper(group_id):
    if not group_id:
        return None
    conn = get_conn()
    row = conn.execute(
        "SELECT * FROM tasks WHERE group_id = %s AND group_position = '0' LIMIT 1",
        (group_id,)
    ).fetchone()
    conn.close()
    if not row:
        return None
    d = dict(row)
    d['images'] = json.loads(d.pop('images_json', '{}') or '{}')
    d['audio'] = json.loads(d.pop('audio_json', '{}') or '{}')
    d['attachments'] = json.loads(d.pop('attachments_json', '{}') or '{}')
    enrich_task_analysis_display(d)
    return d


def update_task(task_id, group_id, group_position, fields):
    allowed = {
        'task_number', 'kes', 'answer_type', 'answer_format',
        'answer_unit', 'answer', 'solution', 'text', 'formatted_text'
    }
    updates = {k: v for k, v in fields.items() if k in allowed}
    if not updates:
        return False
    now = datetime.now().isoformat()
    updates['manually_edited'] = 1
    updates['edited_at'] = now
    set_clause = ', '.join(f'{k} = %s' for k in updates)
    values = list(updates.values()) + [task_id, group_id, group_position]
    conn = get_conn()
    conn.execute(
        f'UPDATE tasks SET {set_clause} WHERE id = %s AND group_id = %s AND group_position = %s',
        values
    )
    conn.commit()
    conn.close()
    return True


def delete_task(task_id, group_id, group_position):
    conn = get_conn()
    cur = conn.execute(
        'DELETE FROM tasks WHERE id = %s AND group_id = %s AND group_position = %s',
        (task_id or '', group_id or '', group_position or '')
    )
    n = cur.rowcount
    conn.commit()
    conn.close()
    return n


def _build_filter_conditions(filters):
    conditions = []
    params = []
    if filters.get('exam_type'):
        conditions.append('exam_type = %s')
        params.append(filters['exam_type'])
    if filters.get('subject'):
        conditions.append('subject = %s')
        params.append(filters['subject'])
    if filters.get('kes'):
        conditions.append('kes LIKE %s')
        params.append(f"%{filters['kes']}%")
    if filters.get('answer_type'):
        conditions.append('answer_type = %s')
        params.append(filters['answer_type'])
    if filters.get('group_id'):
        conditions.append('group_id = %s')
        params.append(filters['group_id'])
    if filters.get('task_number'):
        conditions.append('task_number LIKE %s')
        params.append(f"%{filters['task_number']}%")
    if filters.get('search'):
        conditions.append('text LIKE %s')
        params.append(f"%{filters['search']}%")
    if filters.get('exam_number'):
        conditions.append('exam_number = %s')
        params.append(filters['exam_number'])
    return conditions, params


def count_tasks_filtered(filters):
    conditions, params = _build_filter_conditions(filters or {})
    where = ('WHERE ' + ' AND '.join(conditions)) if conditions else ''
    conn = get_conn()
    total = conn.execute(f'SELECT COUNT(*) FROM tasks {where}', params).fetchone()[0]
    conn.close()
    return total


def delete_tasks_filtered(filters):
    conditions, params = _build_filter_conditions(filters or {})
    where = ('WHERE ' + ' AND '.join(conditions)) if conditions else ''
    conn = get_conn()
    cur = conn.execute(f'DELETE FROM tasks {where}', params)
    n = cur.rowcount
    conn.commit()
    conn.close()
    return n


def export_tasks(filters=None, include_fields=None):
    filters = filters or {}
    include_fields = include_fields or set()

    conditions = []
    params = []

    if filters.get('exam_type'):
        conditions.append('exam_type = %s')
        params.append(filters['exam_type'])
    if filters.get('subject'):
        conditions.append('subject = %s')
        params.append(filters['subject'])
    if filters.get('kes'):
        conditions.append('kes LIKE %s')
        params.append(f"%{filters['kes']}%")
    if filters.get('answer_type'):
        conditions.append('answer_type = %s')
        params.append(filters['answer_type'])
    if filters.get('group_id'):
        conditions.append('group_id = %s')
        params.append(filters['group_id'])
    if filters.get('task_number'):
        conditions.append('task_number LIKE %s')
        params.append(f"%{filters['task_number']}%")
    if filters.get('search'):
        conditions.append('text LIKE %s')
        params.append(f"%{filters['search']}%")
    if filters.get('exam_number'):
        conditions.append('exam_number = %s')
        params.append(filters['exam_number'])

    where = ('WHERE ' + ' AND '.join(conditions)) if conditions else ''

    conn = get_conn()
    rows = conn.execute(
        f'SELECT * FROM tasks {where} ORDER BY exam_number ASC, rowid ASC', params
    ).fetchall()
    conn.close()

    result = []
    for row in rows:
        d = dict(row)
        images = json.loads(d.pop('images_json', '{}') or '{}')
        audio = json.loads(d.pop('audio_json', '{}') or '{}')
        attachments = json.loads(d.pop('attachments_json', '{}') or '{}')
        d.pop('imported_at', None)
        d.pop('rowid', None)  # не включаем суррогатный ключ БД в экспорт

        if 'images' in include_fields:
            d['images'] = images
        if 'audio' in include_fields:
            d['audio'] = audio
        if 'attachments' in include_fields:
            d['attachments'] = attachments
        if 'html' not in include_fields:
            d.pop('html', None)

        result.append(d)
    return result


def get_filter_options():
    conn = get_conn()
    exam_types = [r[0] for r in conn.execute(
        "SELECT DISTINCT exam_type FROM tasks WHERE exam_type != '' AND exam_type IS NOT NULL ORDER BY exam_type"
    ).fetchall()]
    subjects = [r[0] for r in conn.execute(
        "SELECT DISTINCT subject FROM tasks WHERE subject != '' AND subject IS NOT NULL ORDER BY subject"
    ).fetchall()]
    answer_types = [r[0] for r in conn.execute(
        "SELECT DISTINCT answer_type FROM tasks WHERE answer_type != '' AND answer_type IS NOT NULL ORDER BY answer_type"
    ).fetchall()]
    exam_numbers = [r[0] for r in conn.execute(
        "SELECT DISTINCT exam_number FROM tasks WHERE exam_number != '' AND exam_number IS NOT NULL ORDER BY exam_number"
    ).fetchall()]
    conn.close()
    return {
        'exam_types': exam_types,
        'subjects': subjects,
        'answer_types': answer_types,
        'exam_numbers': exam_numbers,
    }


# ---------------------------------------------------------------------------
# Инициализация данных по умолчанию
# ---------------------------------------------------------------------------

def _ensure_analysis_defaults():
    conn = get_conn()
    n = conn.execute('SELECT COUNT(*) FROM math_curriculum_topics').fetchone()[0]
    if n == 0:
        samples = [
            ('Математика', 'Алгебра', 'Целые выражения', 'Одночлены и многочлены',
             'Операции с одночленами и многочленами', '7'),
            ('Математика', 'Алгебра', 'Уравнения', 'Линейные уравнения',
             'Решение линейных уравнений с одной переменной', '7'),
            ('Математика', 'Геометрия', 'Треугольники', 'Теорема Пифагора',
             'Применение теоремы Пифагора', '8'),
        ]
        conn.executemany(
            '''INSERT INTO math_curriculum_topics
               (subject, section, subsection, topic, topic_description, grade_class)
               VALUES (%s,%s,%s,%s,%s,%s)''',
            samples,
        )
    row = conn.execute('SELECT body FROM analysis_prompts WHERE id = 1').fetchone()
    now = datetime.now().isoformat()
    file_text = read_default_analysis_prompt_file()
    file_body = (file_text or '').strip()
    if not row:
        conn.execute(
            'INSERT INTO analysis_prompts (id, body, updated_at) VALUES (1, %s, %s)',
            (file_body, now),
        )
    elif not (row[0] or '').strip() and file_body:
        conn.execute(
            'UPDATE analysis_prompts SET body = %s, updated_at = %s WHERE id = 1',
            (file_body, now),
        )
    conn.commit()
    conn.close()
