# scripts/notebook_logger.py

import logging
from pathlib import Path

try:
    from IPython.display import display, Markdown
    IN_NOTEBOOK = True
except ImportError:
    IN_NOTEBOOK = False

class NotebookHandler(logging.Handler):
    def emit(self, record):
        log_entry = self.format(record)
        if IN_NOTEBOOK:
            display(Markdown(f"`{log_entry}`"))
        else:
            print(log_entry)

def get_notebook_logger(name='notebook', log_file="pipeline.log", level=logging.INFO):
    logger = logging.getLogger(name)
    logger.setLevel(level)

    if not logger.handlers:
        formatter = logging.Formatter('%(asctime)s — %(levelname)s — %(message)s')

        logs_dir = Path(__file__).resolve().parent.parent / "logs"
        logs_dir.mkdir(parents=True, exist_ok=True)

        log_path = logs_dir / log_file

        file_handler = logging.FileHandler(log_path, encoding='utf-8', mode='a')  # <-- mode='a' appends
        file_handler.setFormatter(formatter)
        file_handler.setLevel(level)

        notebook_handler = NotebookHandler()
        notebook_handler.setFormatter(formatter)
        notebook_handler.setLevel(level)

        logger.addHandler(file_handler)
        logger.addHandler(notebook_handler)

    return logger
