from typing import List
import Utility.DBConnector as Connector
from Utility.Status import Status
from Utility.Exceptions import DatabaseException
from Utility.DBConnector import ResultSet
from Business.File import File
from Business.RAM import RAM
from Business.Disk import Disk
from psycopg2 import sql


def createTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("CREATE TABLE Files"
                     "(file_id integer NOT NULL PRIMARY KEY, CHECK(file_id > 0),"
                     "type text NOT NULL,"
                     "size integer NOT NULL CHECK(size >= 0))")
        conn.execute("CREATE TABLE Disks"
                     "(disk_id integer NOT NULL PRIMARY KEY, CHECK(disk_id > 0),"
                     "company text NOT NULL,"
                     "speed integer NOT NULL CHECK(speed > 0),"
                     "free_space integer NOT NULL CHECK (free_space >= 0),"
                     "cost integer NOT NULL CHECK(cost > 0))")
        conn.execute("CREATE TABLE Rams"
                     "(ram_id integer NOT NULL PRIMARY KEY, CHECK(ram_id > 0),"
                     "company text NOT NULL,"
                     "size integer NOT NULL CHECK(size > 0))")

        # save pair of (file_id, disk_id) if file with file_id is in disk with disk_id
        conn.execute("CREATE TABLE FilesInDisks"
                     "(file_id integer,"
                     "FOREIGN KEY (file_id) REFERENCES Files(file_id) ON DELETE CASCADE,"
                     "disk_id integer,"
                     "FOREIGN KEY (disk_id) REFERENCES Disks(disk_id) ON DELETE CASCADE)")
        # save pair of (ram_id, disk_id) if ram with ram_id is in disk with disk_id
        conn.execute("CREATE TABLE RamsInDisks"
                     "(ram_id integer,"
                     "FOREIGN KEY (ram_id) REFERENCES Rams(ram_id) ON DELETE CASCADE,"
                     "disk_id integer,"
                     "FOREIGN KEY (disk_id) REFERENCES Disks(disk_id) ON DELETE CASCADE)")
        # views?

        conn.commit()

    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        # will happen any way after try termination or exception handling
        conn.close()


def clearTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute(sql.SQL("DELETE FROM Files"))
        conn.execute(sql.SQL("DELETE FROM Disks"))
        conn.execute(sql.SQL("DELETE FROM Rams"))
        conn.execute(sql.SQL("DELETE FROM FilesInDisks"))
        conn.execute(sql.SQL("DELETE FROM RamsInDisks"))
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        conn.close()


def dropTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("DROP TABLE IF EXISTS Files CASCADE")
        conn.execute("DROP TABLE IF EXISTS Disks CASCADE")
        conn.execute("DROP TABLE IF EXISTS Rams CASCADE")
        conn.execute("DROP TABLE IF EXISTS FilesInDisks CASCADE")
        conn.execute("DROP TABLE IF EXISTS RamsInDisks CASCADE")
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()


def addFile(file: File) -> Status:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "INSERT INTO Files(file_id, type, size) VALUES({file_id}, {type}, {size})").format(
            file_id=sql.Literal(file.getFileID()),
            type=sql.Literal(file.getType()),
            size=sql.Literal(file.getSize()))
        conn.execute(query)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        conn.rollback()
        return Status.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        conn.rollback()
        return Status.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        conn.rollback()
        return Status.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        conn.rollback()
        return Status.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        conn.rollback()
        return Status.BAD_PARAMS
    except Exception as e:
        conn.rollback()
        return Status.ERROR
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()
    return Status.OK


def getFileByID(fileID: int) -> File:
    conn = None
    ret = File.badFile()
    rows_affected = 0
    result = ResultSet()

    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "SELECT * FROM Files WHERE file_id={id}").format(
            id=sql.Literal(fileID))
        rows_affected, result = conn.execute(query)

        if rows_affected != 0: # TODO: make sure it works
            ret = File(result[0]["file_id"], result[0]["type"], result[0]["size"])

    except:
        ret = File.badFile()
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()
        return ret


def deleteFile(file: File) -> Status:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "DELETE FROM Files WHERE file_id={id}").format(
            id=sql.Literal(file.getFileID()))
        rows_effected, _ =conn.execute(query)
        if rows_effected == 0:
            return Status.NOT_EXISTS # TODO: ????
        conn.commit()

    except:
        conn.rollback()
        return Status.ERROR
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()
    return Status.OK


def addDisk(disk: Disk) -> Status:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "INSERT INTO Disks(disk_id, company, speed, free_space, cost) VALUES({disk_id}, {company}, {speed}, {free_space}, {cost})").format(
            disk_id=sql.Literal(disk.getDiskID()),
            company=sql.Literal(disk.getCompany()),
            speed=sql.Literal(disk.getSpeed()),
            free_space=sql.Literal(disk.getFreeSpace()),
            cost=sql.Literal(disk.getCost()))
        conn.execute(query)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        conn.rollback()
        return Status.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        conn.rollback()
        return Status.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        conn.rollback()
        return Status.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        conn.rollback()
        return Status.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        conn.rollback()
        return Status.BAD_PARAMS
    except Exception as e:
        conn.rollback()
        return Status.ERROR
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()
    return Status.OK


def getDiskByID(diskID: int) -> Disk:
    conn = None
    ret = Disk.badDisk()
    rows_effected = 0
    result = ResultSet()

    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "SELECT * FROM Disks WHERE disk_id={id}").format(
            id=sql.Literal(diskID))
        rows_effected, result = conn.execute(query)

        if rows_effected != 0:  # TODO: make sure it works
            ret = Disk(result[0]["disk_id"], result[0]["company"], result[0]["speed"], result[0]["free_space"], result[0]["cost"])

    except:
        ret = Disk.badDisk()
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()
        return ret


def deleteDisk(diskID: int) -> Status:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "DELETE FROM Disks WHERE disk_id={id}").format(
            id=sql.Literal(diskID))
        rows_effected, _ = conn.execute(query)
        if rows_effected == 0:
            return Status.NOT_EXISTS
        conn.commit()

    except:
        conn.rollback()
        return Status.ERROR
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()
    return Status.OK


def addRAM(ram: RAM) -> Status:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "INSERT INTO Rams(ram_id, company, size) VALUES({ram_id}, {company}, {size})").format(
            ram_id=sql.Literal(ram.getRamID()),
            company=sql.Literal(ram.getCompany()),
            size=sql.Literal(ram.getSize()))
        conn.execute(query)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        conn.rollback()
        return Status.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        conn.rollback()
        return Status.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        conn.rollback()
        return Status.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        conn.rollback()
        return Status.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        conn.rollback()
        return Status.BAD_PARAMS
    except Exception as e:
        conn.rollback()
        return Status.ERROR
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()
    return Status.OK


def getRAMByID(ramID: int) -> RAM:
    conn = None
    ret = RAM.badRAM()
    rows_effected = 0
    result = ResultSet()

    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "SELECT * FROM Rams WHERE ram_id={id}").format(
            id=sql.Literal(ramID))
        rows_effected, result = conn.execute(query)

        if rows_effected != 0:  # TODO: make sure it works
            ret = RAM(result[0]["ram_id"], result[0]["company"], result[0]["size"])

    except:
        ret = RAM.badRAM()
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()
        return ret


def deleteRAM(ramID: int) -> Status:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "DELETE FROM Rams WHERE ram_id={id}").format(
            id=sql.Literal(ramID))
        rows_effected, _ = conn.execute(query)
        if rows_effected == 0:
            return Status.NOT_EXISTS
        conn.commit()

    except:
        conn.rollback()
        return Status.ERROR
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()
    return Status.OK


def addDiskAndFile(disk: Disk, file: File) -> Status:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "BEGIN; INSERT INTO Files(file_id, type, size) VALUES({file_id}, {type}, {size}); "
            "INSERT INTO Disks(disk_id, company, speed, free_space, cost) VALUES({disk_id}, {company}, {speed}, {free_space}, {cost}); COMMIT;").format(
            file_id=sql.Literal(file.getFileID()),
            type=sql.Literal(file.getType()),
            size=sql.Literal(file.getSize()),
            disk_id=sql.Literal(disk.getDiskID()),
            company=sql.Literal(disk.getCompany()),
            speed=sql.Literal(disk.getSpeed()),
            free_space=sql.Literal(disk.getFreeSpace()),
            cost=sql.Literal(disk.getCost()))
        conn.execute(query)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        conn.rollback()
        return Status.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        conn.rollback()
        return Status.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        conn.rollback()
        return Status.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        conn.rollback()
        return Status.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        conn.rollback()
        return Status.BAD_PARAMS
    except Exception as e:
        conn.rollback()
        return Status.ERROR
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()
    return Status.OK


def addFileToDisk(file: File, diskID: int) -> Status:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "BEGIN;"
            "INSERT INTO FilesInDisks(file_id, disk_id) "
                "SELECT {file_id}, (SELECT disk_id FROM Disks WHERE disk_id={d_id} AND free_space>={needed_space});"
            "UPDATE Disks SET free_space = free_space - {needed_space} WHERE disk_id={d_id};"
            "COMMIT;").format(
            file_id=sql.Literal(file.getFileID()),
            d_id=sql.Literal(diskID),
            needed_space=sql.Literal(file.getSize()))
        conn.execute(query)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        conn.rollback()
        return Status.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        conn.rollback()
        return Status.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        conn.rollback()
        return Status.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        conn.rollback()
        return Status.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        conn.rollback()
        return Status.BAD_PARAMS
    except Exception as e:
        conn.rollback()
        return Status.ERROR
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()
    return Status.OK


def removeFileFromDisk(file: File, diskID: int) -> Status:
    return Status.OK


def addRAMToDisk(ramID: int, diskID: int) -> Status:
    return Status.OK


def removeRAMFromDisk(ramID: int, diskID: int) -> Status:
    return Status.OK


def averageFileSizeOnDisk(diskID: int) -> float:
    return 0


def diskTotalRAM(diskID: int) -> int:
    return 0


def getCostForType(type: str) -> int:
    return 0


def getFilesCanBeAddedToDisk(diskID: int) -> List[int]:
    return []


def getFilesCanBeAddedToDiskAndRAM(diskID: int) -> List[int]:
    return []


def isCompanyExclusive(diskID: int) -> bool:
    return True


def getConflictingDisks() -> List[int]:
    return []


def mostAvailableDisks() -> List[int]:
    return []


def getCloseFiles(fileID: int) -> List[int]:
    return []




# TODO: delete before submission

if __name__ == '__main__':
    print("hello")
    print("Creating all tables")
    createTables()
    print("Add file {1, pdf, 100}")
    print(addFile(File(1, 'pdf', 100)))
    print("Add disk {10, c1, 2, 1000, 300}")
    print(addDisk(Disk(10, 'c1', 2, 100, 300)))

    print("Add file 1 to disk 10")
    print(addFileToDisk(File(1, 'pdf', 100), 10))


    # print("Add RAM {100, c2, 40}")
    # print(addRAM(RAM(100, 'c2', 40)))


    # print("Add file {2, pdf, 100} and disk {20, c1, 2, 1000, 300}")
    # print(addDiskAndFile(Disk(20, 'c1', 2, 1000, 300), File(2, 'pdf', 100)))

    # print('Can\'t reinsert the same row')
    # print(addFile(File(1, 'pdf', 100)))
    #
    # print("get file")
    # print(getFileByID(1).getFileID())
    # print("get disk")
    # print(getDiskByID(10).getDiskID())
    # print("get RAM")
    # print(getRAMByID(100).getRamID())
    #
    # print("Delete file {1, pdf, 100}")
    # print(deleteFile(File(1, 'pdf', 100)))
    # print("Delete disk {10, c1, 2, 1000, 300}")
    # print(deleteDisk(10))
    # print("Delete RAM {100, c2, 40}")
    # print(deleteRAM(100))
    #
    # print("Delete when it doent exist")
    # print("Delete file {1, pdf, 100}")
    # print(deleteFile(File(1, 'pdf', 100)))
    # print("Delete disk {10, c1, 2, 1000, 300}")
    # print(deleteDisk(10))
    # print("Delete RAM {100, c2, 40}")
    # print(deleteRAM(100))
    #
    # print("Add file {1, pdf, 100}")
    # print(addFile(File(1, 'pdf', 100)))

    clearTables()

    dropTables()