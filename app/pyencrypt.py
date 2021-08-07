import gnupg
import app.config

def pgp_encryption(filename):
    path = f'C:/Apps/voya/dumps/{filename}'
    gpg = gnupg.GPG(gpgbinary=app.config.GPG_PATH, gnupghome=app.config.GPG_HOME)
    with open(path, 'rb') as f:
        status = gpg.encrypt_file(f, recipients=[app.config.GPG_FINGERPRINT], output=f'{path}.encrypted', always_trust=True)
    return status