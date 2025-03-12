import os
from pathlib import Path
from FindPath import sync_async_method
from Utills.Logger import logger
from Utills import StateManager as state


class Finder:
    def __init__(self):
        self.excel_files: list[str] | None = None
        self.downloads_path: str | None = None
        self.passengers_path: str | None = None
        self.home = Path.home()
        self.downloads_candidates = [
            self.home / "Downloads",  # English
            self.home / "Загрузки",  # Russian
            self.home / "Descargas",  # Spanish
            self.home / "Téléchargements",  # French
            self.home / "Downloads",  # For macOS/Linux
        ]

    @sync_async_method
    async def downloads(self) -> str | FileNotFoundError:
        """
        Finds and returns Downloads path in your OS

        **Can be sync or async**
        :return: downloads path
        """

        for path in self.downloads_candidates:
            if path.exists():
                self.downloads_path = str(path)

                return self.downloads_path

        if os.name == "nt":
            # Path via environment variable USERPROFILE
            win_downloads = Path(os.environ.get("USERPROFILE", "")) / "Downloads"

            if win_downloads.exists():
                self.downloads_path = str(win_downloads)

                return self.downloads_path

            # Search by keywords in folder name
            user_profile = Path(os.environ.get("USERPROFILE", self.home))

            for dir_entry in user_profile.iterdir():
                dir_name = dir_entry.name.lower()

                if dir_entry.is_dir() and ("download" in dir_name or "загруз" in dir_name):
                    self.downloads_path = str(dir_entry)

                    return self.downloads_path

            # Additional checks for Linux/macOS
        else:
            # Search via variable XDG_DOWNLOAD_DIR (Linux)
            xdg_dir = os.environ.get("XDG_DOWNLOAD_DIR")

            if xdg_dir and Path(xdg_dir).exists():
                self.downloads_path = xdg_dir

                return self.downloads_path

            # Search by keywords in folder name
            for dir_entry in self.home.iterdir():
                dir_name = dir_entry.name.lower()

                if dir_entry.is_dir() and "download" in dir_name:
                    self.downloads_path = str(dir_entry)

                    return self.downloads_path

            # If the folder is not found
        logger.critical("Downloads folder not found")
        state.update_error("Downloads folder not found")
        raise FileNotFoundError("Downloads folder not found")

    @sync_async_method
    async def passengers(self) -> str | FileNotFoundError:
        """
        Finds and returns PassengersData path in Downloads folder

        **Can be sync or async**
        :return: PassengersData path
        """

        if not self.downloads_path:
            await self.downloads()

        passengers_folder = os.path.join(self.downloads_path, "PassengersData")

        if os.path.exists(passengers_folder):
            self.passengers_path = str(passengers_folder)

            return self.passengers_path

        logger.critical("PassengersData folder not found")
        state.update_error("PassengersData folder not found")
        raise FileNotFoundError("PassengersData folder not found")

    @sync_async_method
    async def all_data(self) -> list[str] | None:
        """
        Finds and returns all .xlsx files in the PassengersData folder.

        **Can be sync or async**
        :return: List of paths to .xlsx files
        """

        if not self.passengers_path:
            await self.passengers()

        if not self.passengers_path:
            logger.critical("PassengersData folder not found")
            state.update_error("PassengersData folder not found")
            raise FileNotFoundError("PassengersData folder not found")

        passengers_dir = Path(self.passengers_path)

        self.excel_files = [
            str(file) for file in passengers_dir.glob("**/*.xlsx")
            if file.is_file()
        ]

        return self.excel_files
