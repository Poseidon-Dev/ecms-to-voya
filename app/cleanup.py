import os

def clean_up(mode, filename):
    if mode:
        path = f'C:/Apps/voya/dumps/'
        os.chdir(path)
        files = os.listdir()
        for file in files:
            if filename not in file:
                if '.csv' in file:
                    os.remove(file)
