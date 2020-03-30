# encoding=utf-8

import os
import zipfile
import hashlib
from local_settings import *
from JsonExporterForIOS import EXPORT_PATH
from flask import Flask, request, Response

app = Flask(__name__)


@app.route('/makeBundle', methods=['POST'])
def create_zip_bundle():
    if not request.json or not request.json.get('books'):
        return Response(status=400, response='Invalid JSON')

    original_dir = os.getcwd()
    os.chdir(EXPORT_PATH)
    book_list = [f'{b}.zip' for b in request.json['books']]
    book_list = [b for b in book_list if os.path.exists(b)]
    if not book_list:
        os.chdir(original_dir)
        return {'error': 'requested books not found'}

    zip_filename = get_bundle_filename(book_list)
    zip_path = f'{SEFARIA_EXPORT_PATH}/bundles/{zip_filename}'
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


"""
Lorenzo talk:
1) make a post endpoint that will zip files together and return a filename
2) Serve bundles as static files
3) Packages can have a reserved filename and won't need to go through the zip build
4) Make the dump a part of the Flask app. We'll use a cronjob to trigger the dump. Triggering the dump will require
a kubernetes secret. Without the secret, you'll get a Forbidden response.
"""
if __name__ == '__main__':
    app.run(host='127.0.0.1', port=5000, debug=True)
