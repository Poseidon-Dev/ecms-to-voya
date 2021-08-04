from ftplib import FTP
import os
import fileinput
import config

def send_file(file_name):
    with FTP() as ftp:
        ftp.connect(config.FTP_SERVER, config.FTP_PORT)
        ftp.login(config.FTP_USR, config.FTP_PWD)
        ftp.cwd(config.FTP_PATH)

        file = open(file_name, 'rb')
        ftp.storbinary(f'STOR {file_name}', file)
        file.close()
