from ftplib import FTP
import pysftp
import app.config

def send_to_sftp(filename):
    import paramiko

    if app.config.TESTING:
        in_directory = 'testin'
        out_directory = 'testout'
    else:
        in_directory = 'incoming'
        out_directory = 'outgoing'

    path = f'C:/Apps/voya/dumps/{filename}.pgp'
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        ssh.connect(app.config.FTP_SERVER, app.config.FTP_PORT, app.config.FTP_USR, app.config.FTP_PWD)
        sftp = ssh.open_sftp()
        sftp.chdir(in_directory)
        sftp.put(path, f'{filename}.pgp')
    except Exception as e:
        print(e)
