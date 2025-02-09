import mysql.connector


class SQL_InsertFromFile:
    @staticmethod
    def __parseColumnToList(line: str):
        returnData: list[str] = list()
        line.strip()
        line = "".join(line.splitlines())
        array: list[str] = line.split(",")
        for obj in array:
            obj.strip()
            returnData.append(obj)
        return returnData

    def __parseLineToList(self, line: str):
        returnData: list[str] = list()
        line.strip()
        line = "".join(line.splitlines())
        array: list[str] = line.split(",")
        for obj in array:
            obj.strip()
            returnData.append(f"'{obj}'")
        return returnData

    def __parseLineToStr(self, line: str):
        array = self.__parseLineToList(line)
        returnData = ",".join(array)
        return returnData

    def __defineColumn(self, line: str):
        self.__m_columns.clear()
        self.__m_columns.extend(self.__parseColumnToList(line))

    def __addData(self, line: str):
        self.__m_data.append(self.__parseLineToStr(line))

    __m_columns: list[str] = list()
    __m_data: list[str] = list()
    __m_dbName: str = ""
    __m_tableName: str = ""
    __m_fileName: str = ""
    __m_target: str = "localhost" #default
    __m_user: str = "root"        #default
    __m_passwd: str = "root"      #default

    def defineDatabaseName(self, name: str):
        self.__m_dbName = name.strip()
    def defineTableName(self, name: str):
        self.__m_tableName = name.strip()
    def defineFileName(self, name: str):
        self.__m_fileName = name.strip()
    def defineDatabaseTarget(self, address: str):
        self.__m_target = address.strip()
    def defineDatabaseUser(self, user: str):
        self.__m_user = user.strip()
    def defineDatabasePassword(self, password: str):
        self.__m_passwd = password.strip()

    def run(self):
        self.__m_columns.clear()
        self.__m_data.clear()
        if self.__m_dbName == "":
            return "Error: Variable m_dbName is not set. Use 'defineDatabaseName(name: str)' to set."
        if self.__m_tableName == "":
            return "Error: Variable m_tableName is not set. Use 'defineTableName(name: str)' to set."
        if self.__m_fileName == "":
            return "Error: Variable m_fileName is not set. Use 'defineFileName(name: str)' to set."


        datei = open(self.__m_fileName, 'r')
        print(f"Filename: {self.__m_fileName}")
        self.__defineColumn(datei.readline())
        for line in datei.readlines():
            self.__addData(line)

        mydb = mysql.connector.connect(
            host=self.__m_target,
            user=self.__m_user,
            password=self.__m_passwd,
            database=self.__m_dbName
        )

        # print(mydb)

        mycursor = mydb.cursor()
        print(self.__m_data)
        for data in self.__m_data:
            columns = ",".join(self.__m_columns)
            print(f"INSERT INTO {self.__m_tableName} ({columns}) VALUES ({data});")
            mycursor.execute(f"INSERT INTO {self.__m_tableName} ({columns}) VALUES ({data});")

        mydb.commit()
