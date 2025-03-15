from datetime import datetime


class StateManager:
    last_error = None
    start_time = None
    processing = False

    # Errors
    @classmethod
    def update_error(cls, error_msg: str):
        cls.last_error = error_msg

    @classmethod
    def get_last_error(cls) -> str:
        return cls.last_error

    # Time
    @classmethod
    def update_start_time(cls, start_time: datetime | None):
        cls.start_time = start_time

    @classmethod
    def get_start_time(cls) -> str|None:
        try:
            return cls.start_time.strftime("%Y-%m-%d %H:%M:%S")
        except:
            return None

    # Processing
    @classmethod
    def update_processing(cls, processing: bool):
        cls.processing = processing

    @classmethod
    def get_processing(cls) -> bool:
        return cls.processing
