from os import write
from rsa.pkcs1 import encrypt
from app.voya import collect_voya_data
from app.ftp import send_file, send_to_sftp
from app.pyencrypt import pgp_encryption
import app.config


if __name__ == "__main__":
    file = collect_voya_data()
    pgp_encryption(file)
    send_to_sftp(file)

