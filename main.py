from app import collect_voya_data, send_to_sftp, pgp_encryption, clean_up


if __name__ == "__main__":
    file = collect_voya_data()
    pgp_encryption(file)
    send_to_sftp(file)
    clean_up(True)

