import sqlite3
from pathlib import Path
from threading import Lock
from typing import NamedTuple, Tuple, Union, Optional, List, Dict, Generator


class DBCommandSQL(NamedTuple):
    command: str
    data: Tuple


class DBCommandCommit(NamedTuple):
    commit: bool


class DBCommandQuit(NamedTuple):
    quit: bool


DBResult = List[Tuple]

DBCommand = Union[DBCommandCommit, DBCommandSQL, DBCommandQuit]
DBRespond = Optional[Union[Exception, bool, DBResult]]


class SQLiteDB:
    """
    Synchronized worker with the SQLite database
    """

    def __init__(self, file_path: Union[str, Path]) -> None:
        """
        Initialize the static database
        :param file_path: path to the saved file with database
        :return: None
        """
        if type(file_path) is str:
            self.memory_db = file_path == ':memory:'
            if not self.memory_db:
                file_path = Path(file_path)
        else:
            self.memory_db = False

        self.exited = False
        self.__db_lock = Lock()

        def routine_db() -> Generator[DBRespond, DBCommand, None]:
            """
            This thread runs in background and performs all operations with the database if needed
            Creates new database if not exists yet
            """
            if self.memory_db:
                connection = sqlite3.connect(":memory:")
            else:
                db_dir = file_path.parent
                if not db_dir.exists():
                    db_dir.mkdir(parents=True, mode=0o750)
                connection = sqlite3.connect(f"{file_path.absolute()}")

            respond: DBRespond = None

            while not self.exited:
                command = yield respond
                try:
                    # the present key determines what time of data this is
                    if type(command) is DBCommandSQL:  # perform SQL query
                        respond = list(connection.execute(command.command, command.data))
                    elif type(command) is DBCommandCommit:  # commit the saved data
                        connection.commit()
                        respond = True
                    elif type(command) is DBCommandQuit:
                        self.exited = True
                        respond = True
                    else:  # not sure what to do, just respond None
                        respond = None
                except Exception as e:
                    respond = e

            connection.commit()
            connection.close()
            self.exited = True
            yield respond

        self.__routine_db = routine_db()
        next(self.__routine_db)

    def execute(self, command: str, data: Tuple = ()) -> DBResult:
        """
        Executes the command on database
        :param command: SQL command to be executed
        :param data: tuple of data that are safely entered into the SQL command to prevent SQL injection
        :return: list of returned rows
        """
        with self.__db_lock:
            respond = self.__routine_db.send(DBCommandSQL(command=command, data=data))
        if isinstance(respond, Exception):
            raise respond
        return respond

    def json(self, command: str, table: str, data: Tuple = ()) -> List[Dict[str, any]]:
        """
        Performs SQL query on table and returns the result as list of dictionaries
        :param command: SQL command to be executed
        :param table: target table of the command. From this table the names of columns are parsed
        :param data: tuple of data that are safely entered into the SQL command to prevent SQL injection
        :return: list of rows, rows are dictionaries where keys are names of columns
        """
        table = table.replace('`', '``')
        columns = [column_data[1] for column_data in self.execute(f"PRAGMA table_info(`{table}`)")]
        records = self.execute(command, data)
        return [{columns[i]: value for i, value in enumerate(record)} for record in records]

    def commit(self) -> bool:
        """
        Commits the databse to the disc
        :return: None
        """
        with self.__db_lock:
            respond = self.__routine_db.send(DBCommandCommit(commit=True))
        if isinstance(respond, Exception):
            raise respond
        return respond

    def quit(self) -> bool:
        """
        End the database connection
        :return: None
        """
        with self.__db_lock:
            respond = self.__routine_db.send(DBCommandQuit(quit=True))
        if isinstance(respond, Exception):
            raise respond
        return respond

    def __del__(self):
        self.quit()
