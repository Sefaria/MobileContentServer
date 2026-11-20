# encoding=utf-8

import os
import threading
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


def url_stubs(bundle_path, schema_version):
    bundle_name = os.path.basename(bundle_path)
    try:
        values = [f'{URL_BASE}/{schema_version}/bundles/{bundle_name}/{f}' for f in os.listdir(bundle_path)]
    except (FileNotFoundError, NotADirectoryError):  # no such bundle, or a bundle name that is a file
        values = []
    values.sort()
    return values


def get_bundle_dir(base_path, package_name):
    """Return the bundle directory named by package_name, or None if that name is not a bundle inside base_path.

    Bundle names come from the client: a package title from packages.json, or the sha1 name that makeBundle
    built. Both are a single path component, so a value with a separator, with a "." or ".." component, or
    that resolves outside base_path is not a bundle and must not be listed.
    """
    if not package_name or package_name in ('.', '..') or os.path.basename(package_name) != package_name:
        return None
    base_dir = os.path.realpath(base_path)
    bundle_path = os.path.realpath(os.path.join(base_dir, package_name))
    if os.path.dirname(bundle_path) != base_dir:
        return None
    return bundle_path


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
    package_name = request.args.get('package')
    if not package_name:
        return jsonify([])
    schema_version = get_schema_from_request(request)
    base_path = f'{SEFARIA_EXPORT_PATH}/{schema_version}/bundles'
    bundle_path = get_bundle_dir(base_path, package_name)
    if bundle_path is None:
        return jsonify([])
    return jsonify(url_stubs(bundle_path, schema_version))


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
    os.system(f'python JsonExporterForIOS.py {action} {index} &')
    return {'status': 'ok'}


@app.route('/booksExport', methods=['GET', 'POST'])
def books_export():
    user_password = request.args.get('password')
    if not password_protect(user_password) and not DEBUG_MODE:
        return Response(status=403, response='Forbidden')

    if request.method == 'GET':
        return jsonify(updated_books_list() + new_books_since_last_update())
    elif request.method == 'POST':
        f = request.args.get('filename')
        FileStorage(request.stream).save(os.path.join(get_export_path(SCHEMA_VERSION), f))
        return Response(status=200, response='ok')


@app.route('/healthz')
def healthcheck():
    return Response(status=200, response='Health: Ok')


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=5000, debug=True)
