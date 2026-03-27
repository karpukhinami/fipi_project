import json
import io
import os
import requests

from flask import Flask, request, jsonify, render_template, send_file
from migration_database import (
    init_db, import_tasks, get_tasks, get_task_by_id_params,
    update_task, update_task_images_json, export_tasks, get_filter_options, get_task_by_rowid, get_group_wrapper,
    clear_task_analysis,
    get_kes_catalog, rebuild_kes_catalog, delete_task, count_tasks_filtered, delete_tasks_filtered,
    get_attachment_data, get_curriculum_topics, update_curriculum_topic,
    add_curriculum_topic, delete_curriculum_topic, import_math_curriculum_from_xlsx,
    get_analysis_prompt, set_analysis_prompt,
    get_analysis_prompt_versions, get_analysis_prompt_version_by_id,
    save_analysis_prompt_version, write_default_analysis_prompt_file,
    get_skills_for_catalog, get_content_elements_for_catalog,
)
from migration_analysis_pipeline import (
    run_task_analysis, run_task_solve, run_image_recognition, build_analysis_prompt_page_payload, commit_task_analysis_to_db,
)

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 512 * 1024 * 1024  # 512 МБ
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'change-me-in-production')

init_db()


# ────────────────────────────────────────────────
# Главная страница
# ────────────────────────────────────────────────

@app.route('/')
def index():
    return render_template('index.html')


# ────────────────────────────────────────────────
# Загрузка JSON
# ────────────────────────────────────────────────

@app.route('/api/import', methods=['POST'])
def api_import():
    if 'file' not in request.files:
        return jsonify({'error': 'Файл не передан'}), 400
    f = request.files['file']
    if not f.filename.endswith('.json'):
        return jsonify({'error': 'Ожидается .json файл'}), 400
    try:
        data = json.loads(f.read().decode('utf-8'))
    except Exception as e:
        return jsonify({'error': f'Ошибка парсинга JSON: {e}'}), 400

    if isinstance(data, dict) and 'tasks' in data:
        data = data['tasks']
    if not isinstance(data, list):
        return jsonify({'error': 'JSON должен быть массивом заданий или объектом {"tasks": [...]}'}), 400

    added, updated, skipped = import_tasks(data)
    return jsonify({'added': added, 'updated': updated, 'skipped': skipped, 'total': len(data)})


# ────────────────────────────────────────────────
# Список заданий (с фильтрацией и пагинацией)
# ────────────────────────────────────────────────

@app.route('/api/tasks', methods=['GET'])
def api_tasks():
    filters = {
        'exam_type':   request.args.get('exam_type', ''),
        'subject':     request.args.get('subject', ''),
        'kes':         request.args.get('kes', ''),
        'answer_type': request.args.get('answer_type', ''),
        'group_id':    request.args.get('group_id', ''),
        'task_number': request.args.get('task_number', ''),
        'search':      request.args.get('search', ''),
        'exam_number': request.args.get('exam_number', ''),
    }
    try:
        page = int(request.args.get('page', 1))
        page_size = int(request.args.get('page_size', 50))
    except ValueError:
        page, page_size = 1, 50

    tasks, total = get_tasks(filters, page, page_size)
    return jsonify({
        'tasks': tasks,
        'total': total,
        'page': page,
        'page_size': page_size,
        'pages': max(1, (total + page_size - 1) // page_size),
    })


# ────────────────────────────────────────────────
# Получить одно задание (полное, с изображениями)
# ────────────────────────────────────────────────

@app.route('/api/tasks/detail', methods=['GET'])
def api_task_detail():
    task_id = request.args.get('id', '')
    group_id = request.args.get('group_id', '')
    group_position = request.args.get('group_position', '')
    task = get_task_by_id_params(task_id, group_id, group_position)
    if task is None:
        return jsonify({'error': 'Задание не найдено'}), 404
    return jsonify(task)


@app.route('/api/tasks/group_wrapper', methods=['GET'])
def api_group_wrapper():
    group_id = request.args.get('group_id', '')
    wrapper = get_group_wrapper(group_id)
    if wrapper is None:
        return jsonify({'wrapper': None})
    return jsonify({'wrapper': wrapper})


@app.route('/api/tasks/by_rowid/<int:rowid>', methods=['GET'])
def api_task_by_rowid(rowid):
    task = get_task_by_rowid(rowid)
    if task is None:
        return jsonify({'error': 'Задание не найдено'}), 404
    return jsonify(task)


@app.route('/api/tasks/attachment', methods=['GET'])
def api_task_attachment():
    task_id = request.args.get('id', '')
    group_id = request.args.get('group_id', '')
    group_position = request.args.get('group_position', '')
    filename = request.args.get('filename', '')
    if not filename:
        return jsonify({'error': 'Не указан filename'}), 400
    result = get_attachment_data(task_id, group_id, group_position, filename)
    if result is None:
        return jsonify({'error': 'Вложение не найдено'}), 404
    data, mime, name = result
    return send_file(io.BytesIO(data), mimetype=mime, as_attachment=True, download_name=name)


# ────────────────────────────────────────────────
# Редактирование задания
# ────────────────────────────────────────────────

@app.route('/api/tasks/update', methods=['PATCH'])
def api_task_update():
    body = request.get_json()
    if not body:
        return jsonify({'error': 'Нет данных'}), 400
    task_id = body.get('id', '')
    group_id = body.get('group_id', '')
    group_position = body.get('group_position', '')
    fields = body.get('fields', {})
    ok = update_task(task_id, group_id, group_position, fields)
    if not ok:
        return jsonify({'error': 'Нет полей для обновления или задание не найдено'}), 400
    return jsonify({'ok': True})


# ────────────────────────────────────────────────
# Удаление заданий
# ────────────────────────────────────────────────

def _get_filters_from_request():
    return {
        'exam_type':   request.args.get('exam_type', ''),
        'subject':     request.args.get('subject', ''),
        'kes':         request.args.get('kes', ''),
        'answer_type': request.args.get('answer_type', ''),
        'group_id':    request.args.get('group_id', ''),
        'task_number': request.args.get('task_number', ''),
        'search':      request.args.get('search', ''),
        'exam_number': request.args.get('exam_number', ''),
    }


@app.route('/api/tasks/count', methods=['GET'])
def api_tasks_count():
    filters = _get_filters_from_request()
    count = count_tasks_filtered(filters)
    return jsonify({'count': count})


@app.route('/api/tasks/delete', methods=['DELETE'])
def api_task_delete():
    mode = request.args.get('mode', 'single')
    if mode == 'filtered':
        filters = _get_filters_from_request()
        deleted = delete_tasks_filtered(filters)
        return jsonify({'deleted': deleted})
    task_id = request.args.get('id', '')
    group_id = request.args.get('group_id', '')
    group_position = request.args.get('group_position', '')
    deleted = delete_task(task_id, group_id, group_position)
    return jsonify({'deleted': deleted})


# ────────────────────────────────────────────────
# Опции для фильтров
# ────────────────────────────────────────────────

@app.route('/api/filters', methods=['GET'])
def api_filters():
    return jsonify(get_filter_options())


# ────────────────────────────────────────────────
# Справочник КЭС
# ────────────────────────────────────────────────

@app.route('/api/kes_catalog', methods=['GET'])
def api_kes_catalog():
    subject = request.args.get('subject', '')
    items = get_kes_catalog(subject if subject else None)
    return jsonify({'items': items})


@app.route('/api/kes_catalog/rebuild', methods=['POST'])
def api_kes_rebuild():
    rebuild_kes_catalog()
    items = get_kes_catalog()
    return jsonify({'ok': True, 'count': len(items)})


# ────────────────────────────────────────────────
# Экспорт JSON
# ────────────────────────────────────────────────

@app.route('/api/export', methods=['GET'])
def api_export():
    filters = {
        'exam_type':   request.args.get('exam_type', ''),
        'subject':     request.args.get('subject', ''),
        'kes':         request.args.get('kes', ''),
        'answer_type': request.args.get('answer_type', ''),
        'group_id':    request.args.get('group_id', ''),
        'task_number': request.args.get('task_number', ''),
        'search':      request.args.get('search', ''),
        'exam_number': request.args.get('exam_number', ''),
    }
    include_raw = request.args.get('include', '')
    include_fields = set(x.strip() for x in include_raw.split(',') if x.strip())
    tasks = export_tasks(filters, include_fields)
    json_bytes = json.dumps(tasks, ensure_ascii=False, indent=2).encode('utf-8')
    buf = io.BytesIO(json_bytes)
    buf.seek(0)
    return send_file(buf, mimetype='application/json', as_attachment=True, download_name='fipi_export.json')


# ────────────────────────────────────────────────
# Прокси для Claude API
# ────────────────────────────────────────────────

def resolve_anthropic_proxies(proxy_url_from_body):
    url = (proxy_url_from_body or '').strip()
    if not url:
        url = os.environ.get('ANTHROPIC_HTTPS_PROXY', '').strip()
    if not url:
        url = os.environ.get('HTTPS_PROXY', '').strip()
    if not url:
        return None
    return {'http': url, 'https': url}


@app.route('/proxy-test')
def proxy_test_page():
    return render_template('proxy_test.html')


@app.route('/api/proxy-test', methods=['POST'])
def api_proxy_test():
    body = request.get_json() or {}
    proxy_raw = (body.get('proxy_url') or '').strip()
    mode = (body.get('mode') or 'connectivity').strip()

    proxies = None
    if proxy_raw:
        proxies = {'http': proxy_raw, 'https': proxy_raw}

    if mode == 'connectivity':
        try:
            r = requests.post(
                'https://api.anthropic.com/v1/messages',
                headers={'content-type': 'application/json', 'anthropic-version': '2023-06-01'},
                json={'model': 'claude-sonnet-4-0', 'max_tokens': 1,
                      'messages': [{'role': 'user', 'content': '.'}]},
                proxies=proxies, timeout=45,
            )
        except requests.exceptions.Timeout:
            return jsonify({'ok': False, 'error': 'Таймаут — прокси недоступен или блокируется.'}), 504
        except requests.exceptions.ProxyError as e:
            return jsonify({'ok': False, 'error': f'Ошибка прокси: {e}'}), 502
        except requests.exceptions.RequestException as e:
            return jsonify({'ok': False, 'error': f'Сеть: {e}'}), 502

        if r.status_code == 401:
            msg = 'Маршрут работает: Anthropic ответил 401 без ключа — это нормально.'
        elif r.status_code == 403:
            msg = 'Соединение установлено, но доступ отклонён (403).'
        elif 200 <= r.status_code < 300:
            msg = 'Получен успешный ответ.'
        else:
            msg = f'Получен HTTP {r.status_code}.'
        return jsonify({'ok': True, 'http_status': r.status_code, 'message': msg})

    if mode == 'full':
        api_key = (body.get('api_key') or '').strip()
        if not api_key:
            return jsonify({'ok': False, 'error': 'Для полной проверки укажите API-ключ.'}), 400
        try:
            r = requests.post(
                'https://api.anthropic.com/v1/messages',
                headers={'x-api-key': api_key, 'anthropic-version': '2023-06-01',
                         'content-type': 'application/json'},
                json={'model': 'claude-sonnet-4-0', 'max_tokens': 32,
                      'messages': [{'role': 'user', 'content': 'Ответь одним словом: ок'}]},
                proxies=proxies, timeout=90,
            )
        except requests.exceptions.Timeout:
            return jsonify({'ok': False, 'error': 'Таймаут запроса к Claude.'}), 504
        except requests.exceptions.ProxyError as e:
            return jsonify({'ok': False, 'error': f'Ошибка прокси: {e}'}), 502
        except requests.exceptions.RequestException as e:
            return jsonify({'ok': False, 'error': str(e)}), 502

        if r.status_code != 200:
            try:
                detail = r.json()
            except Exception:
                detail = {'raw': r.text[:800]}
            return jsonify({'ok': False, 'http_status': r.status_code,
                            'error': f'Claude API вернул {r.status_code}', 'detail': detail}), 200

        try:
            data = r.json()
            preview = data['content'][0]['text']
        except (KeyError, IndexError, TypeError):
            preview = r.text[:500]

        return jsonify({'ok': True, 'message': 'Запрос с ключом выполнен успешно.', 'preview': preview})

    return jsonify({'error': 'Неизвестный режим: используйте connectivity или full.'}), 400


@app.route('/api/claude', methods=['POST'])
def api_claude():
    body = request.get_json()
    if not body:
        return jsonify({'error': 'Нет данных'}), 400
    api_key = body.get('api_key', '').strip()
    if not api_key:
        return jsonify({'error': 'API-ключ не указан'}), 400

    model = body.get('model', 'claude-sonnet-4-0')
    prompt = body.get('prompt', '').strip()
    task_text = body.get('task_text', '').strip()

    if not prompt and not task_text:
        return jsonify({'error': 'Нет промпта или текста задания'}), 400

    user_message = prompt
    if task_text:
        user_message = f"{prompt}\n\n---\nТекст задания:\n{task_text}" if prompt else task_text

    payload = {
        'model': model,
        'max_tokens': 4096,
        'messages': [{'role': 'user', 'content': user_message}]
    }
    proxies = resolve_anthropic_proxies(body.get('proxy_url'))

    try:
        resp = requests.post(
            'https://api.anthropic.com/v1/messages',
            headers={'x-api-key': api_key, 'anthropic-version': '2023-06-01',
                     'content-type': 'application/json'},
            json=payload, proxies=proxies, timeout=120,
        )
    except requests.exceptions.Timeout:
        return jsonify({'error': 'Таймаут запроса к Claude API'}), 504
    except requests.exceptions.RequestException as e:
        return jsonify({'error': f'Ошибка сети: {e}'}), 502

    if resp.status_code != 200:
        try:
            err = resp.json()
        except Exception:
            err = {'raw': resp.text}
        return jsonify({'error': f'Claude API вернул ошибку {resp.status_code}', 'detail': err}), resp.status_code

    try:
        data = resp.json()
        content = data['content'][0]['text']
    except (KeyError, IndexError, ValueError) as e:
        return jsonify({'error': f'Неожиданный формат ответа Claude: {e}', 'raw': resp.text}), 500

    return jsonify({'response': content})


# ────────────────────────────────────────────────
# Анализ заданий, куррикулум, промпт
# ────────────────────────────────────────────────

@app.route('/curriculum')
def curriculum_page():
    return render_template('curriculum.html')


@app.route('/analysis-prompt')
def analysis_prompt_page():
    return render_template('analysis_prompt.html')


@app.route('/api/curriculum/topics', methods=['GET'])
def api_curriculum_list():
    subject = request.args.get('subject', '') or None
    return jsonify({'topics': get_curriculum_topics(subject)})


@app.route('/api/curriculum/topics', methods=['POST'])
def api_curriculum_add():
    body = request.get_json() or {}
    if not (body.get('topic') or '').strip():
        return jsonify({'error': 'Укажите название темы'}), 400
    try:
        tid = add_curriculum_topic(
            body.get('subject') or 'Математика',
            body.get('section') or '',
            body.get('subsection') or '',
            body.get('topic') or '',
            body.get('topic_description') or '',
            body.get('grade_class') or '',
        )
    except Exception as e:
        return jsonify({'error': str(e)}), 400
    return jsonify({'ok': True, 'id': tid})


@app.route('/api/curriculum/topics/<int:topic_id>', methods=['PATCH'])
def api_curriculum_patch(topic_id):
    body = request.get_json() or {}
    if not update_curriculum_topic(topic_id, body):
        return jsonify({'error': 'Нет полей или тема не найдена'}), 400
    return jsonify({'ok': True})


@app.route('/api/curriculum/topics/<int:topic_id>', methods=['DELETE'])
def api_curriculum_delete(topic_id):
    ok, err = delete_curriculum_topic(topic_id)
    if not ok:
        return jsonify({'error': err or 'Нельзя удалить'}), 400
    return jsonify({'ok': True})


@app.route('/api/curriculum/import-xlsx', methods=['POST'])
def api_curriculum_import_xlsx():
    if 'file' not in request.files:
        return jsonify({'error': 'Файл не передан'}), 400
    f = request.files['file']
    if not f.filename.lower().endswith('.xlsx'):
        return jsonify({'error': 'Ожидается .xlsx'}), 400
    import tempfile
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx')
    tmp_path = tmp.name
    tmp.close()
    try:
        f.save(tmp_path)
        n = import_math_curriculum_from_xlsx(tmp_path, replace=True)
    finally:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
    return jsonify({'ok': True, 'inserted': n})


@app.route('/api/analysis-prompt', methods=['GET'])
def api_analysis_prompt_get():
    return jsonify(build_analysis_prompt_page_payload())


@app.route('/api/analysis-prompt', methods=['POST'])
def api_analysis_prompt_set():
    body = request.get_json() or {}
    prompt_body = body.get('body', '')
    label = (body.get('label') or '').strip()
    save_analysis_prompt_version(prompt_body, label or None)
    payload = build_analysis_prompt_page_payload()
    payload['ok'] = True
    return jsonify(payload)


@app.route('/api/analysis-prompt/version/<int:vid>', methods=['GET'])
def api_analysis_prompt_version(vid):
    v = get_analysis_prompt_version_by_id(vid)
    if not v:
        return jsonify({'error': 'Версия не найдена'}), 404
    return jsonify(v)


@app.route('/api/analysis-prompt/write-file', methods=['POST'])
def api_analysis_prompt_write_file():
    body = request.get_json() or {}
    text = body.get('body', '')
    if not text.strip():
        return jsonify({'ok': False, 'error': 'Пустой текст'}), 400
    ok = write_default_analysis_prompt_file(text)
    if not ok:
        return jsonify({'ok': False, 'error': 'Не удалось записать файл'}), 500
    save_analysis_prompt_version(text, (body.get('label') or '').strip() or 'write-file')
    payload = build_analysis_prompt_page_payload()
    payload['ok'] = True
    return jsonify(payload)


@app.route('/api/tasks/analyze', methods=['POST'])
def api_tasks_analyze():
    try:
        body = request.get_json() or {}
        provider = (body.get('provider') or 'anthropic').strip()
        if provider == 'openrouter':
            api_key = os.environ.get('OPENROUTER_API_KEY', '').strip()
            if not api_key:
                return jsonify({'ok': False, 'error': 'Переменная OPENROUTER_API_KEY не задана на сервере'}), 400
        else:
            api_key = (body.get('api_key') or body.get('claude_api_key') or '').strip()
            if not api_key:
                api_key = os.environ.get('ANTHROPIC_API_KEY', '').strip()
            if not api_key:
                return jsonify({'ok': False, 'error': 'Нужен API-ключ Anthropic (Claude)'}), 400

        task_id = body.get('id', '')
        group_id = body.get('group_id', '')
        group_position = body.get('group_position', '')
        task = get_task_by_id_params(task_id, group_id, group_position)
        if not task:
            return jsonify({'ok': False, 'error': 'Задание не найдено'}), 404
        model = body.get('model') or ('claude-sonnet-4-20250514' if provider == 'anthropic' else 'qwen/qwen3-235b-a22b-2507')
        proxy_url = (body.get('proxy_url') or '').strip() or None
        result = run_task_analysis(task, api_key, model=model, proxy_url=proxy_url, provider=provider)
        if not result.get('ok'):
            return jsonify(result), 400
        # Don't refresh task from DB yet — it hasn't been saved
        return jsonify(result)
    except Exception as e:
        import traceback
        app.logger.exception('api_tasks_analyze')
        payload = {'ok': False, 'error': f'Внутренняя ошибка сервера: {e}'}
        if app.debug:
            payload['traceback'] = traceback.format_exc()
        return jsonify(payload), 500


@app.route('/api/tasks/solve', methods=['POST'])
def api_tasks_solve():
    try:
        body = request.get_json() or {}
        provider = (body.get('provider') or 'anthropic').strip()
        if provider == 'openrouter':
            api_key = os.environ.get('OPENROUTER_API_KEY', '').strip()
            if not api_key:
                return jsonify({'ok': False, 'error': 'Переменная OPENROUTER_API_KEY не задана на сервере'}), 400
        else:
            api_key = (body.get('api_key') or body.get('claude_api_key') or '').strip()
            if not api_key:
                api_key = os.environ.get('ANTHROPIC_API_KEY', '').strip()
            if not api_key:
                return jsonify({'ok': False, 'error': 'Нужен API-ключ Anthropic (Claude)'}), 400

        task_id = body.get('id', '')
        group_id = body.get('group_id', '')
        group_position = body.get('group_position', '')
        task = get_task_by_id_params(task_id, group_id, group_position)
        if not task:
            return jsonify({'ok': False, 'error': 'Задание не найдено'}), 404
        model = body.get('model') or ('claude-sonnet-4-20250514' if provider == 'anthropic' else 'qwen/qwen3-235b-a22b-2507')
        proxy_url = (body.get('proxy_url') or '').strip() or None
        result = run_task_solve(task, api_key, model=model, proxy_url=proxy_url, provider=provider)
        if not result.get('ok'):
            return jsonify(result), 400

        # Сохраняем решение и ответ напрямую в задание
        update_task(task_id, group_id, group_position, {
            'solution': result.get('solution_text') or '',
            'answer': result.get('final_answer') or '',
        })
        fresh = get_task_by_id_params(task_id, group_id, group_position)
        result['task'] = fresh
        return jsonify(result)
    except Exception as e:
        import traceback
        app.logger.exception('api_tasks_solve')
        payload = {'ok': False, 'error': f'Внутренняя ошибка сервера: {e}'}
        if app.debug:
            payload['traceback'] = traceback.format_exc()
        return jsonify(payload), 500


@app.route('/api/tasks/recognize-images', methods=['POST'])
def api_tasks_recognize_images():
    try:
        body = request.get_json() or {}
        provider = (body.get('provider') or 'anthropic').strip()
        if provider == 'openrouter':
            api_key = os.environ.get('OPENROUTER_API_KEY', '').strip()
            if not api_key:
                return jsonify({'ok': False, 'error': 'Переменная OPENROUTER_API_KEY не задана на сервере'}), 400
        else:
            api_key = (body.get('api_key') or body.get('claude_api_key') or '').strip()
            if not api_key:
                api_key = os.environ.get('ANTHROPIC_API_KEY', '').strip()
            if not api_key:
                return jsonify({'ok': False, 'error': 'Нужен API-ключ (Claude)'}), 400

        task_id = body.get('id', '')
        group_id = body.get('group_id', '')
        group_position = body.get('group_position', '')
        task = get_task_by_id_params(task_id, group_id, group_position)
        if not task:
            return jsonify({'ok': False, 'error': 'Задание не найдено'}), 404

        model = body.get('model') or ('claude-sonnet-4-20250514' if provider == 'anthropic' else 'google/gemini-2.5-flash-preview')
        proxy_url = (body.get('proxy_url') or '').strip() or None
        result = run_image_recognition(task, api_key, model=model, proxy_url=proxy_url, provider=provider)
        if not result.get('ok'):
            return jsonify(result), 400

        # Мержим результаты в существующий images_json
        images = dict(task.get('images') or {})
        for fname, rec in (result.get('results') or {}).items():
            if fname in images:
                images[fname] = {**images[fname], **rec}
        update_task_images_json(task_id, group_id, group_position, images)

        fresh = get_task_by_id_params(task_id, group_id, group_position)
        result['task'] = fresh
        return jsonify(result)
    except Exception as e:
        import traceback
        app.logger.exception('api_tasks_recognize_images')
        payload = {'ok': False, 'error': f'Внутренняя ошибка сервера: {e}'}
        if app.debug:
            payload['traceback'] = traceback.format_exc()
        return jsonify(payload), 500


@app.route('/api/tasks/save-analysis', methods=['POST'])
def api_tasks_save_analysis():
    try:
        body = request.get_json() or {}
        save_data = body.get('save_data')
        if not save_data or not isinstance(save_data, dict):
            return jsonify({'ok': False, 'error': 'Нет save_data'}), 400
        result = commit_task_analysis_to_db(save_data)
        if not result.get('ok'):
            return jsonify(result), 400
        task_id = save_data.get('task_id', '')
        group_id = save_data.get('group_id', '')
        group_position = save_data.get('group_position', '')
        fresh = get_task_by_id_params(task_id, group_id, group_position)
        return jsonify({'ok': True, 'task': fresh})
    except Exception as e:
        import traceback
        app.logger.exception('api_tasks_save_analysis')
        payload = {'ok': False, 'error': f'Внутренняя ошибка: {e}'}
        if app.debug:
            payload['traceback'] = traceback.format_exc()
        return jsonify(payload), 500


@app.route('/api/tasks/clear-analysis', methods=['POST'])
def api_tasks_clear_analysis():
    body = request.get_json() or {}
    task_id = body.get('id', '')
    group_id = body.get('group_id', '')
    group_position = body.get('group_position', '')
    if not clear_task_analysis(task_id, group_id, group_position):
        return jsonify({'ok': False, 'error': 'Задание не найдено'}), 404
    task = get_task_by_id_params(task_id, group_id, group_position)
    return jsonify({'ok': True, 'task': task})




@app.route('/skills-catalog')
def skills_catalog():
    return render_template('skills_catalog.html')


@app.route('/api/skills-catalog/items', methods=['GET'])
def api_skills_catalog_items():
    item_type  = request.args.get('type', 'skills')
    subject    = request.args.get('subject') or None
    section    = request.args.get('section') or None
    subsection = request.args.get('subsection') or None
    topic_id_raw = request.args.get('topic_id')
    topic_id   = int(topic_id_raw) if topic_id_raw and topic_id_raw.isdigit() else None

    if item_type == 'elements':
        rows = get_content_elements_for_catalog(
            subject=subject, section=section, subsection=subsection, topic_id=topic_id
        )
    else:
        rows = get_skills_for_catalog(
            subject=subject, section=section, subsection=subsection, topic_id=topic_id
        )
    return jsonify({'ok': True, 'type': item_type, 'items': rows})


if __name__ == '__main__':
    debug_mode = os.environ.get('FLASK_DEBUG', 'false').lower() == 'true'
    port = int(os.environ.get('PORT', 5000))
    app.run(debug=debug_mode, host='0.0.0.0', port=port)
