from app.voya import collect_voya_data
from app.ftp import send_file

if __name__ == "__main__":
    # file = collect_voya_data()
    file = '20210730.csv'
    send_file(file)
