from ftplib import FTP
from pathlib import Path
import app.config

def send_file(file_name):
    with FTP() as ftp:
        ftp.connect(app.config.FTP_SERVER, app.config.FTP_PORT)
        ftp.login(app.config.FTP_USR, app.config.FTP_PWD)
        ftp.cwd(app.config.FTP_PATH)

        try:
            file = open(f'C:/Apps/voya/dumps/{file_name}', 'rb')
            ftp.storbinary(f'STOR {file_name}', file)
            print('File sent')
            file.close()
        except Exception as e:
            print(e)
        
