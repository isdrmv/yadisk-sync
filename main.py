"""A simple Yandex.Disk synchronization script.

Synchronize Minecraft server backup files. Priority for local files.

Author: Denis Romanov (https://github.com/isdrmv).
"""

from __future__ import annotations

import logging
import os
import sys

import tqdm
import tqdm.utils
import yadisk

LOG_FILE = 'yadisk_sync.log'
BACKUPS_DIR = 'backups'

# Directories on Yandex.Disk.
APP_MC_DIR = 'app:/mc'
APP_BACKUPS_DIR = 'app:/mc/backups'

# Create application on https://oauth.yandex.ru/client/new
# Permission: "cloud_api:disk.app_folder".
# Set ClientID and open:
# https://oauth.yandex.ru/authorize?response_type=token&client_id=<ClientID>
# Copy the token from the received link of the form:
# https://oauth.yandex.ru/verification_code?dev=True#access_token=<token>
yadisk: yadisk.YaDisk = yadisk.YaDisk(token=os.getenv('YADISK_TOKEN'))


def run_logging() -> None:
    """Run logging to the file and console."""
    logging.basicConfig(
        format='[%(asctime)s %(levelname)s]: %(message)s',
        datefmt='%d.%m.%Y %H:%M:%S',
        level=logging.INFO,
        handlers=[
            logging.FileHandler(LOG_FILE),
            logging.StreamHandler(),
        ]
    )


def handle_dirs() -> None:
    """Create the necessary directories if they do not exist."""
    if not os.path.exists(BACKUPS_DIR):
        os.makedirs(BACKUPS_DIR, exist_ok=True)
        logging.info(
            f'The "{BACKUPS_DIR}" directory is created on the local drive.'
        )

    if not yadisk.exists(APP_MC_DIR):
        yadisk.mkdir(APP_MC_DIR)
        logging.info(
            f'The "{APP_MC_DIR}" directory is created on Yandex.Disk.'
        )
    if not yadisk.exists(APP_BACKUPS_DIR):
        yadisk.mkdir(APP_BACKUPS_DIR)
        logging.info(
            f'The "{APP_BACKUPS_DIR}" directory is created on Yandex.Disk.'
        )


def sync() -> None:
    """Deleting old files and uploading new ones.

    The priority for the local directory.
    """
    logging.info('Yandex.Disk synchronization is running.')

    local_files = os.listdir(BACKUPS_DIR)
    yadisk_files = []
    for elem in yadisk.listdir(APP_BACKUPS_DIR, fields=('name', 'type')):
        if elem.type == 'file':
            if f'{elem.name}.tgz' not in local_files:
                yadisk.remove(
                    f'{APP_BACKUPS_DIR}/{elem.name}', permanently=True,
                )
                logging.info(f'File "{elem.name}" deleted from Yandex.Disk.')
            else:
                yadisk_files.append(elem.name)

    # Remove the file extension to bypass a strange limitations of Yandex.Disk.
    for file in local_files:
        if file.replace('.tgz', '') not in yadisk_files:
            path = f'{BACKUPS_DIR}/{file}'
            size = os.stat(path).st_size
            with open(path, 'rb') as infile:
                with tqdm.tqdm(
                    total=size, unit='B', unit_scale=True, unit_divisor=1024,
                ) as cb:
                    wrapped_file = tqdm.utils.CallbackIOWrapper(
                        cb.update, infile, 'read',
                    )
                    yadisk.upload(
                        wrapped_file,
                        f'{APP_BACKUPS_DIR}/{file.replace(".tgz", "")}',
                    )
                    logging.info(
                        f'File "{file.replace(".tgz", "")}" uploaded to '
                        f'Yandex.Disk.'
                    )

    logging.info('Yandex.Disk synchronization is finished.')


def main() -> None:
    """Run logging and Yandex.Disk synchronization."""
    run_logging()

    handle_dirs()
    sync()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        # Clean fail on keyboard interrupt - beautiful.
        sys.exit(1)
    except Exception as exc:
        # Just in case.
        logging.exception(exc, exc_info=True)
