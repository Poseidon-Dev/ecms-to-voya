from app import collect_voya_data, send_to_sftp, pgp_encryption, clean_up
from app.config import DUMP_LOCALE
from app.email import send_email

if __name__ == "__main__":
    try:
        file = collect_voya_data()
        response = f'Data Collected: Filename {file}\n'
        try:
            pgp_encryption(file)
            response += f'Encrption Successful\n'
            try: 
                send_to_sftp(file)
                response += f'File Tranmission Successful\n'
                clean_up(True)
                send_email('Voya Tranmission', response)
            except Exception as e:
                response = f'Transmission failed with error: {e}'
        except Exception as e:
            response = f'Encryption failed with error: {e}'
    except Exception as e:
        resposne = f'File collection failed with error: {e}'

    # pgp_encryption(file)
    # send_to_sftp(file)
    # clean_up(True)

