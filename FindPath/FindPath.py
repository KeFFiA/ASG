import os
from pathlib import Path
from Utills.Logger import logger
from Utills import StateManager as state


class Finder:
    def __init__(self):
        self.excel_files: list[str] | None = None
        self.downloads_path: str | None = None
        self.passengers_path: str | None = None
        self.finances_path: str | None = None
        self.finances_files: list[str] | None = None
        self.home = Path.home()
        self.downloads_candidates = [
            self.home / "Downloads",  # English
            self.home / "Загрузки",  # Russian
            self.home / "Descargas",  # Spanish
            self.home / "Téléchargements",  # French
            self.home / "Downloads",  # For macOS/Linux
        ]

    async def downloads(self) -> str | FileNotFoundError:
        """
        Finds and returns Downloads path in your OS

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

    async def passengers(self) -> str | FileNotFoundError:
        """
        Finds and returns PassengersData path in Downloads folder

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

    async def all_data(self) -> list[str] | None:
        """
        Finds and returns all .xlsx files in the PassengersData folder.

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

    async def finances(self) -> str | FileNotFoundError:
        """
        Finds and returns FinancesData path in Downloads folder

        :return: FinancesData path
        """

        if not self.downloads_path:
            await self.downloads()

        finances_folder = os.path.join(self.downloads_path, "FinancesData")

        if os.path.exists(finances_folder):
            self.finances_path = str(finances_folder)

            return self.finances_path

        logger.critical("FinancesData folder not found")
        state.update_error("FinancesData folder not found")
        raise FileNotFoundError("FinancesData folder not found")

    async def all_data_finances(self) -> list[str] | None:
        """
        Finds and returns all .xlsx files in the FinancesData folder.

        :return: List of paths to .xlsx files
        """

        if not self.finances_path:
            await self.finances()

        if not self.finances_path:
            logger.critical("FinancesData folder not found")
            state.update_error("FinancesData folder not found")
            raise FileNotFoundError("FinancesData folder not found")

        finances_dir = Path(self.finances_path)

        self.finances_files = [
            str(file) for file in finances_dir.glob("**/*.xlsx")
            if file.is_file()
        ]

        return self.finances_files
