# encoding=utf-8

import os
import sys
import threading
import subprocess
import shutil
import hashlib
from datetime import datetime
from local_settings import *
from JsonExporterForIOS import build_split_archive, SEFARIA_EXPORT_PATH, get_export_path, updated_books_list, new_books_since_last_update, clear_old_bundles, SCHEMA_VERSION, PREV_SCHEMA_VERSION
from flask import Flask, request, Response, jsonify
from werkzeug.datastructures import FileStorage
try:
    from local_settings import DEBUG_MODE
except ImportError:
    DEBUG_MODE = False

# schema version is now required when requesting an API view that is schema specific
# in the case it's not provided, we will fall back to the last version that allowed it to not be defined (which is buggy behavior)
DEFAULT_API_SCHEMA_VERSION = "6"
app = Flask(__name__)
URL_BASE = 'static/ios-export'

# Keep in sync with the action names in the __main__ block of JsonExporterForIOS.py.
EXPORT_ACTIONS = frozenset({
    'export_all',
    'export_all_skip_existing',
    'export_text',
    'export_updated',
    'purge_cloudflare',
    'export_toc',
    'export_hebrew_categories',
    'export_calendar',
    'export_authors',
    'export_base_files_to_sources',
    'export_packages',
    'write_last_updated',
})


def url_stubs(bundle_path, schema_version):
    bundle_name = os.path.basename(bundle_path)
    try:
        values = [f'{URL_BASE}/{schema_version}/bundles/{bundle_name}/{f}' for f in os.listdir(bundle_path)]
    except FileNotFoundError:
        values = []
    values.sort()
    return values


def get_directory_size(dir_path):
    total = 0
    for f in os.listdir(dir_path):  # this is naive but works for the case at hand
        total += os.path.getsize(os.path.join(dir_path, f))
    return total


def create_zip_bundle(book_list, zip_path, zip_dirname, file_locations):
    # thread safety is critical in this method.
    def is_recent_dir(dirname):  # this method should only return False if the directory in question exists and is "old"
        try:
            s = os.stat(dirname)
        except FileNotFoundError:
            return True

        diff = datetime.now() - datetime.fromtimestamp(s.st_mtime)
        return diff.total_seconds() < 120

    try:
        os.mkdir('./tmp')
    except FileExistsError:
        pass

    tmp_dir = f'./tmp/{zip_dirname}'
    if os.path.exists(tmp_dir):
        if is_recent_dir(tmp_dir):
            return

    build_split_archive(book_list, tmp_dir, file_locations)
    try:
        shutil.move(tmp_dir, zip_path)
    except FileNotFoundError:  # can theoretically happen if another thread is active
        return


def get_schema_from_request(req):
    try:
        return int(req.args.get('schema_version'))
    except (ValueError, TypeError):
        return DEFAULT_API_SCHEMA_VERSION


@app.route('/makeBundle', methods=['POST'])
def make_bundle():
    if not request.json or not request.json.get('books'):
        return Response(status=400, response='Invalid JSON')

    schema_version = get_schema_from_request(request)
    export_path = f'{SEFARIA_EXPORT_PATH}/{schema_version}'

    book_list = [f'{b}.zip' for b in request.json['books']]
    book_list = [b for b in book_list if os.path.exists(os.path.join(export_path, b))]
    zip_dirname = get_bundle_filename(book_list)
    zip_path = f'{export_path}/bundles/{zip_dirname}'

    if os.path.exists(zip_path):
        return {
            'bundleArray': url_stubs(zip_path, schema_version),
            'downloadSize': get_directory_size(zip_path),
        }
    else:
        clear_old_bundles(SCHEMA_VERSION)
        clear_old_bundles(PREV_SCHEMA_VERSION)
        t = threading.Thread(target=create_zip_bundle, args=(book_list, zip_path, zip_dirname, export_path))
        t.start()
        return Response(status=202, response='Accepted')


@app.route('/packageData', methods=['GET'])
def get_package_paths():
    if not request.args or not request.args.get('package'):
        return jsonify([])
    package_name = request.args['package']
    schema_version = get_schema_from_request(request)
    base_path = f'{SEFARIA_EXPORT_PATH}/{schema_version}/bundles'
    return jsonify(url_stubs(f'{base_path}/{package_name}', schema_version))


def get_bundle_filename(book_list: list) -> str:
    books_hash = hashlib.sha1('|'.join(sorted(book_list)).encode('utf-8')).hexdigest()
    return f'{books_hash}'


def password_protect(user_password):
    try:
        password = os.environ['PASSWORD']
    except KeyError:
        return False
    return user_password == password


@app.route('/update')
def update():
    try:
        password = os.environ['PASSWORD']
    except KeyError:
        return Response(status=403, response='Forbidden')
    user_password = request.args.get('password')
    if user_password != password:
        return Response(status=403, response='Forbidden')
    action, index = request.args.get('action', default='export_updated'), request.args.get('index', default='')
    if action not in EXPORT_ACTIONS:
        return Response(status=400, response='Unknown action')
    start_export(action, index)
    return {'status': 'ok'}


def start_export(action, index):
    command = [sys.executable, 'JsonExporterForIOS.py', action]
    if index:
        command.append(index)
    process = subprocess.Popen(command, cwd=os.path.dirname(os.path.abspath(__file__)))
    # Gunicorn reaps only its workers, so this thread reaps the exporter when it exits.
    threading.Thread(target=process.wait, daemon=True).start()


@app.route('/booksExport', methods=['GET', 'POST'])
def books_export():
    user_password = request.args.get('password')
    if not password_protect(user_password) and not DEBUG_MODE:
        return Response(status=403, response='Forbidden')

    if request.method == 'GET':
        return jsonify(updated_books_list() + new_books_since_last_update())
    elif request.method == 'POST':
        f = request.args.get('filename')
        if not f:
            return Response(status=400, response='Missing filename')
        export_dir = os.path.realpath(get_export_path(SCHEMA_VERSION))
        destination = os.path.realpath(os.path.join(export_dir, f))
        if os.path.dirname(destination) != export_dir:
            return Response(status=400, response='Invalid filename')
        FileStorage(request.stream).save(destination)
        return Response(status=200, response='ok')


@app.route('/healthz')
def healthcheck():
    return Response(status=200, response='Health: Ok')


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=5000, debug=True)
