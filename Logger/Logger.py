import logging
from logging.handlers import RotatingFileHandler
import datetime
import os
import glob
import zipfile
from pathlib import Path


def get_project_root() -> Path:
    current_file = Path(__file__).absolute()

    for parent in current_file.parents:
        if (parent / '.git').exists() or (parent / 'requirements.txt').exists():
            return parent

    return current_file.parents[2]


LOGS_DIR = get_project_root() / 'Logs'
LOGS_DIR.mkdir(exist_ok=True, parents=True)


class CustomLogHandler(RotatingFileHandler):
    def __init__(self, filename, maxBytes, backupCount):
        self.backup_count = backupCount
        self.log_directory = os.path.dirname(filename)
        self.base_filename = os.path.basename(filename)
        super().__init__(
            filename=filename,
            maxBytes=maxBytes,
            backupCount=0,
            encoding='utf-8',
            delay=False
        )

    def doRollover(self):
        if self.stream:
            self.stream.close()
            self.stream = None

        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        archive_name = f"{self.baseFilename}.{timestamp}"
        os.rename(self.baseFilename, archive_name)

        self._archive_file(archive_name)

        self.stream = self._open()

        self._cleanup_logs()

    def _archive_file(self, filename):
        zip_name = f"{filename}.zip"
        with zipfile.ZipFile(zip_name, 'w', zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(filename, os.path.basename(filename))
        os.remove(filename)

    def _cleanup_logs(self):
        zip_files = glob.glob(os.path.join(self.log_directory, "*.zip"))
        zip_files.sort(key=os.path.getctime, reverse=True)

        while len(zip_files) > self.backup_count:
            os.remove(zip_files.pop())


def setup_logger():
    log_format = (
        '[%(levelname)s] %(asctime)s | %(filename)s(%(funcName)s) - %(lineno)d: %(message)s'
    )
    formatter = logging.Formatter(log_format)

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    log_file = LOGS_DIR / f"ASG_{datetime.datetime.now().strftime('%Y-%m-%d')}.log"
    file_handler = CustomLogHandler(
        filename=log_file,
        maxBytes=100 * 1024 * 1024,
        backupCount=5
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger


logger = setup_logger()
