# encoding=utf-8

import os
import zipfile
import hashlib
from local_settings import *
from JsonExporterForIOS import keep_directory, SEFARIA_EXPORT_PATH, SCHEMA_VERSION
from flask import Flask, request, Response

app = Flask(__name__)


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
    if not os.path.exists(zip_path):
        print(f'building new zip: {zip_filename}')
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as z:
            for b in book_list:
                z.write(b)

    os.chdir(original_dir)
    return {'bundle': zip_filename}


def get_bundle_filename(book_list: list) -> str:
    books_hash = hashlib.sha1('|'.join(sorted(book_list)).encode('utf-8')).hexdigest()
    return f'{books_hash}.zip'


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


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=5000, debug=True)
