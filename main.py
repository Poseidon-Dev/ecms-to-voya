import os

from app import collect_voya_data, send_to_sftp, pgp_encryption, clean_up
from app.config import DUMP_LOCALE, FTP_PATH
from app.email import send_email

if __name__ == "__main__":
    try:
        file = collect_voya_data()
        response = f'Data Collected: Filename {file[:8]}\n'
        try:
            pgp_encryption(file)
            response += f'Encryption Successful\n'
            try: 
                send_to_sftp(file)
                response += f'File Tranmission Successful: Location {FTP_PATH}/testin/{file}.pgp \n'
                clean_up(True, file)
                send_email('Voya Tranmission', response)
            except Exception as e:
                response = f'Transmission failed with error: {e}'
        except Exception as e:
            response = f'Encryption failed with error: {e}'
    except Exception as e:
        resposne = f'File collection failed with error: {e}'
    finally:
        print(response)

