import os
import base64

TESTING = True

ERP_HOST = os.getenv('ERP_HOST')
ERP_UID = os.getenv('ERP_UID')
ERP_PWD = os.getenv('ERP_PWD')

FTP_SERVER = os.getenv('FTP_SERVER')
FTP_PORT = int(os.getenv('FTP_PORT'))
FTP_USR = os.getenv('FTP_USR')
FTP_PWD = os.getenv('FTP_PWD')
FTP_PATH = os.getenv('FTP_PATH')

GPG_PATH = 'C:/Apps/GnuPG/gpg.exe'
GPG_HOME = 'C:/Apps/voya/pgp'
GPG_KEY = os.getenv('GPG_KEY')
GPG_FINGERPRINT = os.getenv('GPG_FINGERPRINT')

EMAIL_UID = os.getenv('EMAIL_UID')
EMAIL_PWD = os.getenv('EMAIL_PWD')
EMAIL_SMTP = os.getenv('EMAIL_SMTP')
EMAIL_SMTP_PORT = int(os.getenv('EMAIL_SMTP_PORT'))

DUMP_LOCALE = os.getenv('DUMP_LOCALE')