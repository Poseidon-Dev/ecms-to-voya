from ftplib import FTP
import pysftp
import app.config

def send_file(file_name):
    with pysftp.Connection(
        host=app.config.FTP_SERVER,
        port=app.config.FTP_PORT,
        username=app.config.FTP_USR,
        password=app.config.FTP_PWD,
        private_key=app.config.FTP_SSH,
        ) as sftp:

        with sftp.cd(app.config.FTP_PATH):
            sftp.put(f'C:/Apps/voya/dumps/{file_name}')
            print('File dumped')

