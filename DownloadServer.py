# encoding=utf-8

import os
import zipfile
import hashlib
from local_settings import *
from JsonExporterForIOS import keep_directory, build_split_archive, SEFARIA_EXPORT_PATH, SCHEMA_VERSION,\
    EXPORT_PATH, updated_books_list, new_books_since_last_update
from flask import Flask, request, Response, jsonify
from werkzeug.datastructures import FileStorage
try:
    from local_settings import DEBUG_MODE
except ImportError:
    DEBUG_MODE = False

app = Flask(__name__)
URL_BASE = 'static/ios-export'


def url_stubs(bundle_path, schema_version):
    bundle_name = os.path.basename(bundle_path)
    try:
        values = [f'{URL_BASE}/{schema_version}/bundles/{bundle_name}/{f}' for f in os.listdir(bundle_path)]
    except FileNotFoundError:
        values = []
    values.sort()
    return values


@app.route('/makeBundle', methods=['POST'])
@keep_directory
def create_zip_bundle():
    if not request.json or not request.json.get('books'):
        return Response(status=400, response='Invalid JSON')

    try:
        schema_version = int(request.args.get('schema_version', SCHEMA_VERSION))
    except ValueError:
        schema_version = SCHEMA_VERSION
    export_path = f'{SEFARIA_EXPORT_PATH}/{schema_version}'

    original_dir = os.getcwd()
    os.chdir(export_path)
    book_list = [f'{b}.zip' for b in request.json['books']]
    book_list = [b for b in book_list if os.path.exists(b)]
    if not book_list:
        os.chdir(original_dir)
        return {'error': 'requested books not found'}

    bundle_path = f'{export_path}/bundles'
    if not os.path.isdir(bundle_path):
        os.mkdir(bundle_path)

    zip_filename = get_bundle_filename(book_list)
    zip_path = f'{bundle_path}/{zip_filename}'
    if os.path.exists(zip_path):
        filenames = os.listdir(zip_path)
    else:
        print(f'building new zip: {zip_filename}')
        filenames = build_split_archive(book_list, zip_path)

    os.chdir(original_dir)
    return jsonify(url_stubs(zip_path, schema_version))


@app.route('/packageData', methods=['GET'])
def get_package_paths():
    if not request.args or not request.args.get('package'):
        return jsonify([])
    package_name = request.args['package']
    try:
        schema_version = int(request.args.get('schema_version', SCHEMA_VERSION))
    except ValueError:
        schema_version = SCHEMA_VERSION

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
    action, index = request.args.get('action', default=''), request.args.get('index', default='')
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
        FileStorage(request.stream).save(os.path.join(EXPORT_PATH, f))
        return Response(status=200, response='ok')


@app.route('/healthz')
def healthcheck():
    return Response(status=200, response='Health: Ok')


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=5000, debug=True)
