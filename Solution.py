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
                     "FOREIGN KEY (disk_id) REFERENCES Disks(disk_id) ON DELETE CASCADE,"
                     "PRIMARY KEY (file_id, disk_id))")
        # save pair of (ram_id, disk_id) if ram with ram_id is in disk with disk_id
        conn.execute("CREATE TABLE RamsInDisks"
                     "(ram_id integer,"
                     "FOREIGN KEY (ram_id) REFERENCES Rams(ram_id) ON DELETE CASCADE,"
                     "disk_id integer,"
                     "FOREIGN KEY (disk_id) REFERENCES Disks(disk_id) ON DELETE CASCADE,"
                     "PRIMARY KEY (ram_id, disk_id))")
        # views

        conn.execute("CREATE VIEW FilesInDisksWithFileData AS "
                     "SELECT FilesInDisks.file_id AS file_id, FilesInDisks.disk_id AS disk_id, "
                     "Files.size AS file_size, Files.type AS file_type "
                     "FROM FilesInDisks "
                     "INNER JOIN Files ON FilesInDisks.file_id=Files.file_id")

        conn.execute("CREATE VIEW RamsInDisksWithRamData AS "
                     "SELECT RamsInDisks.ram_id AS ram_id, RamsInDisks.disk_id AS disk_id, "
                     "Rams.size AS ram_size, Rams.company AS ram_company "
                     "FROM RamsInDisks "
                     "INNER JOIN Rams ON RamsInDisks.ram_id=Rams.ram_id")

        conn.execute("CREATE VIEW FilesInDisksWithFileDataAndCost AS "
                     "SELECT FilesInDisksWithFileData.disk_id AS disk_id, Disks.cost as disk_cost, "
                     "FilesInDisksWithFileData.file_size AS file_size, FilesInDisksWithFileData.file_type AS file_type "
                     "FROM FilesInDisksWithFileData "
                     "INNER JOIN Disks ON FilesInDisksWithFileData.disk_id=Disks.disk_id")

        conn.execute("CREATE VIEW RamsInDisksWithRamDataAndCompany AS "
                     "SELECT RamsInDisksWithRamData.disk_id AS disk_id, Disks.company as disk_company, "
                     "RamsInDisksWithRamData.ram_id AS ram_id, RamsInDisksWithRamData.ram_company AS ram_company "
                     "FROM RamsInDisksWithRamData "
                     "INNER JOIN Disks ON RamsInDisksWithRamData.disk_id=Disks.disk_id")

        conn.execute("CREATE VIEW PricePerType AS "
                     "SELECT file_type, disk_cost * file_size AS price "
                     "FROM FilesInDisksWithFileDataAndCost")

        conn.execute("CREATE VIEW CountFilesInDisks1 AS "
                     "SELECT file_id, COUNT(*) FROM FilesInDisks GROUP BY file_id HAVING COUNT(file_id)>1")

        conn.execute("CREATE VIEW FilesInDisksWithoutSingleFiles AS "
                     "SELECT file_id, disk_id FROM FilesInDisks WHERE file_id IN (SELECT file_id FROM CountFilesInDisks1)")

        conn.execute("CREATE VIEW FilesCanBeInDisks AS "
                     "SELECT file_id, disk_id, speed FROM Files, Disks WHERE Files.size <= Disks.free_space")

        conn.execute("CREATE VIEW CountFilesCanBeInDisks AS "
            "SELECT disk_id, COUNT(*), speed FROM FilesCanBeInDisks GROUP BY disk_id, speed")

        conn.execute("CREATE VIEW FilesInNoDisk AS "
                     "SELECT file_id FROM Files EXCEPT SELECT file_id FROM FilesInDisks")

        conn.execute("CREATE VIEW FilesInDisksForClose AS "
                     "SELECT t1.file_id AS file_id, t1.disk_id AS disk_id, t2.file_id AS file2_id "
                     "FROM FilesInDisks t1 INNER JOIN FilesInDisks t2 ON t1.disk_id = t2.disk_id "
                     "UNION "
                     "SELECT FilesInNoDisk.file_id, NULL, Files.file_id AS file2_id FROM FilesInNoDisk, Files")

        conn.execute("CREATE VIEW FilesInDisksForCloseNoDup AS "
                     "SELECT file_id, disk_id, file2_id FROM FilesInDisksForClose "
                     "WHERE file_id!=file2_id")

        conn.execute("CREATE VIEW CountFilesInDisks2 AS "
                     "SELECT file_id, COUNT(*) FROM FilesInDisks GROUP BY file_id "
                     "UNION "
                     "SELECT (SELECT file_id FROM FilesInNoDisk), 0")

        conn.execute("CREATE VIEW CountFilesInDisksForClose AS "
                     "SELECT file_id, file2_id, COUNT(*) FROM FilesInDisksForCloseNoDup GROUP BY file_id, file2_id")



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
        conn.execute(sql.SQL("DELETE FROM FilesInDisksWithFileData"))
        conn.execute(sql.SQL("DELETE FROM RamsInDisksWithRamData"))
        conn.execute(sql.SQL("DELETE FROM FilesInDisksWithFileDataAndCost"))
        conn.execute(sql.SQL("DELETE FROM RamsInDisksWithRamDataAndCompany"))
        conn.execute(sql.SQL("DELETE FROM PricePerType"))
        conn.execute(sql.SQL("DELETE FROM CountFilesInDisks1"))
        conn.execute(sql.SQL("DELETE FROM FilesCanBeInDisks"))
        conn.execute(sql.SQL("DELETE FROM CountFilesCanBeInDisks"))
        conn.execute(sql.SQL("DELETE FROM FilesInDisksForClose"))
        conn.execute(sql.SQL("DELETE FROM FilesInDisksForCloseNoDup"))
        conn.execute(sql.SQL("DELETE FROM CountFilesInDisks2"))
        conn.execute(sql.SQL("DELETE FROM CountFilesInDisksForClose"))
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
        conn.execute("DROP VIEW IF EXISTS FilesInDisksWithFileData CASCADE")
        conn.execute("DROP VIEW IF EXISTS RamsInDisksWithRamData CASCADE")
        conn.execute("DROP VIEW IF EXISTS FilesInDisksWithFileDataAndCost CASCADE")
        conn.execute("DROP VIEW IF EXISTS RamsInDisksWithRamDataAndCompany CASCADE")
        conn.execute("DROP VIEW IF EXISTS PricePerType CASCADE")
        conn.execute("DROP VIEW IF EXISTS CountFilesInDisks1 CASCADE")
        conn.execute("DROP VIEW IF EXISTS FilesCanBeInDisks CASCADE")
        conn.execute("DROP VIEW IF EXISTS CountFilesCanBeInDisks CASCADE")
        conn.execute("DROP VIEW IF EXISTS FilesInDisksForClose CASCADE")
        conn.execute("DROP VIEW IF EXISTS FilesInDisksForCloseNoDup CASCADE")
        conn.execute("DROP VIEW IF EXISTS CountFilesInDisks2 CASCADE")
        conn.execute("DROP VIEW IF EXISTS CountFilesInDisksForClose CASCADE")
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
        # if rows_effected == 0:
        #     return Status.NOT_EXISTS # TODO: ????
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
            "INSERT INTO FilesInDisks(file_id, disk_id) VALUES({f_id}, {d_id});"
            "UPDATE Disks SET free_space = free_space - {needed_space} WHERE disk_id={d_id};"
            "COMMIT;").format(
            f_id=sql.Literal(file.getFileID()),
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
        return Status.NOT_EXISTS
    except Exception as e:
        conn.rollback()
        return Status.ERROR
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()
    return Status.OK


def removeFileFromDisk(file: File, diskID: int) -> Status:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "BEGIN;"
            "UPDATE Disks SET free_space = free_space + {needed_space} "
                "WHERE disk_id=(SELECT disk_id FROM FilesInDisks WHERE file_id={f_id} AND disk_id={d_id});"
            "DELETE FROM FilesInDisks WHERE file_id={f_id} AND disk_id={d_id};"
            "COMMIT;").format(
            f_id=sql.Literal(file.getFileID()),
            d_id=sql.Literal(diskID),
            needed_space=sql.Literal(file.getSize()))
        conn.execute(query)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        conn.rollback()
        return Status.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        conn.rollback()
        return Status.ERROR
    except DatabaseException.CHECK_VIOLATION as e:
        conn.rollback()
        return Status.OK
    except DatabaseException.UNIQUE_VIOLATION as e:
        conn.rollback()
        return Status.ERROR
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        conn.rollback()
        return Status.OK
    except Exception as e:
        conn.rollback()
        return Status.ERROR
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()
    return Status.OK


def addRAMToDisk(ramID: int, diskID: int) -> Status:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "INSERT INTO RamsInDisks(ram_id, disk_id) VALUES({r_id},{d_id})").format(
               # "(SELECT ram_id FROM Rams WHERE ram_id={r_id}), (SELECT disk_id FROM Disks WHERE disk_id={d_id})").format(
            r_id=sql.Literal(ramID),
            d_id=sql.Literal(diskID))
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
        return Status.NOT_EXISTS
    except Exception as e:
        conn.rollback()
        return Status.ERROR
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()
    return Status.OK


def removeRAMFromDisk(ramID: int, diskID: int) -> Status:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "DELETE FROM RamsInDisks WHERE ram_id={r_id} AND disk_id={d_id};").format(
            r_id=sql.Literal(ramID),
            d_id=sql.Literal(diskID))
        conn.execute(query)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        conn.rollback()
        return Status.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        conn.rollback()
        return Status.ERROR
    except DatabaseException.CHECK_VIOLATION as e:
        conn.rollback()
        return Status.NOT_EXISTS
    except DatabaseException.UNIQUE_VIOLATION as e:
        conn.rollback()
        return Status.ERROR
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        conn.rollback()
        return Status.NOT_EXISTS
    except Exception as e:
        conn.rollback()
        return Status.ERROR
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()
    return Status.OK


def averageFileSizeOnDisk(diskID: int) -> float:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "SELECT AVG(FilesInDisksWithFileData.file_size) FROM FilesInDisksWithFileData WHERE disk_id = {d_id}").format(
            d_id=sql.Literal(diskID))
        _ , result = conn.execute(query)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        conn.rollback()
        return -1
    except DatabaseException.NOT_NULL_VIOLATION as e:
        conn.rollback()
        return -1
    except DatabaseException.CHECK_VIOLATION as e:
        conn.rollback()
        return 0
    except DatabaseException.UNIQUE_VIOLATION as e:
        conn.rollback()
        return -1
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        conn.rollback()
        return 0
    except Exception as e:
        conn.rollback()
        return -1
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()
    if not list(result[0].values())[0]:
        return 0
    return list(result[0].values())[0]


def diskTotalRAM(diskID: int) -> int:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "SELECT SUM(RamsInDisksWithRamData.ram_size) FROM RamsInDisksWithRamData WHERE disk_id = {d_id}").format(
            d_id=sql.Literal(diskID))
        _, result = conn.execute(query)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        conn.rollback()
        return -1
    except DatabaseException.NOT_NULL_VIOLATION as e:
        conn.rollback()
        return -1
    except DatabaseException.CHECK_VIOLATION as e:
        conn.rollback()
        return 0
    except DatabaseException.UNIQUE_VIOLATION as e:
        conn.rollback()
        return -1
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        conn.rollback()
        return 0
    except Exception as e:
        conn.rollback()
        return -1
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()
    if not list(result[0].values())[0]:
        return 0
    return list(result[0].values())[0]


def getCostForType(type: str) -> int:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "SELECT SUM(price) FROM PricePerType WHERE file_type = {f_type}").format(
            f_type=sql.Literal(type))
        _, result = conn.execute(query)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        conn.rollback()
        return -1
    except DatabaseException.NOT_NULL_VIOLATION as e:
        conn.rollback()
        return -1
    except DatabaseException.CHECK_VIOLATION as e:
        conn.rollback()
        return 0
    except DatabaseException.UNIQUE_VIOLATION as e:
        conn.rollback()
        return -1
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        conn.rollback()
        return 0
    except Exception as e:
        conn.rollback()
        return -1
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()
    if not list(result[0].values())[0]:
        return 0
    return list(result[0].values())[0]


def getFilesCanBeAddedToDisk(diskID: int) -> List[int]:
    my_result = []
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "SELECT file_id FROM Files "
            "WHERE Files.size <= "
            "(SELECT free_space FROM Disks WHERE disk_id={d_id})"
            "ORDER BY file_id DESC;").format(
            d_id=sql.Literal(diskID))
        _, result = conn.execute(query)
        conn.commit()

    except:
        conn.rollback()
        return my_result
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()
    if not result[0] or len(list(result[0].values())) == 0 or not list(result[0].values())[0]:
        return my_result
    for i in range(len(list(result.rows))):
        if i >= 5:
            break
        my_result.append(list(result[i].values())[0])
    return my_result


def getFilesCanBeAddedToDiskAndRAM(diskID: int) -> List[int]:
    conn = None
    my_result = []
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "SELECT file_id FROM Files "
            "WHERE Files.size <= "
            "("
            "(SELECT free_space FROM Disks WHERE disk_id={d_id}) "
            "+ "
            "(SELECT COALESCE(SUM(RamsInDisksWithRamData.ram_id) , 0) FROM RamsInDisksWithRamData WHERE disk_id = 10)"
            ")"
            "ORDER BY file_id ASC;").format(
            d_id=sql.Literal(diskID))
        _, result = conn.execute(query)
        conn.commit()

    except:
        conn.rollback()
        return my_result
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()
    if not result[0] or len(list(result[0].values())) == 0 or not list(result[0].values())[0]:
        return my_result
    for i in range(len(list(result.rows))):
        if i >= 5:
            break
        my_result.append(list(result[i].values())[0])
    return my_result


def isCompanyExclusive(diskID: int) -> bool:
    conn = None
    rows_effected = 0
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "SELECT ram_id, disk_id FROM RamsInDisksWithRamDataAndCompany "
            "WHERE ram_company!=disk_company AND disk_id={d_id}").format(
            d_id=sql.Literal(diskID))
        rows_effected, _= conn.execute(query)
        conn.commit()

    except:
        conn.rollback()
        return False
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()
    if rows_effected  == 0:
        return True
    return False


def getConflictingDisks() -> List[int]:
    conn = None
    my_result = []
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "SELECT DISTINCT disk_id FROM FilesInDisksWithoutSingleFiles "
            "ORDER BY disk_id ASC")
        _, result = conn.execute(query)
        conn.commit()

    except Exception as e:
        conn.rollback()
        return my_result
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()
    for i in range(len(list(result.rows))):
        my_result.append(list(result[i].values())[0])
    return my_result


def mostAvailableDisks() -> List[int]:
    conn = None
    my_result = []
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "SELECT disk_id FROM CountFilesCanBeInDisks "
            "ORDER BY count DESC, speed DESC, disk_id ASC")
        _, result = conn.execute(query)
        conn.commit()

    except:
        conn.rollback()
        return my_result
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()
    for i in range(len(list(result.rows))):
        if i>=5:
            break
        my_result.append(list(result[i].values())[0])
    return my_result


def getCloseFiles(fileID: int) -> List[int]:
    conn = None
    my_result = []
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "SELECT file2_id FROM CountFilesInDisksForClose "
            "WHERE file_id = {f_id} AND count * 2 >= (SELECT count FROM CountFilesInDisks2 WHERE file_id = {f_id}) "
            "ORDER BY file2_id ASC").format(
            f_id=sql.Literal(fileID))
        _, result = conn.execute(query)
        conn.commit()

    except Exception as e:
        conn.rollback()
        return my_result
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()
    for i in range(len(list(result.rows))):
        if i >= 10:
            break
        my_result.append(list(result[i].values())[0])
    return my_result


# TODO: delete before submission


if __name__ == '__main__':
    createTables()

    disk1 = Disk(1, "DELL", 10, 10, 10)
    ram1 = RAM(1, "wav", 15)
    addRAM(ram1)
    result = addRAMToDisk(ram1.getRamID(), disk1.getDiskID())
    print(result)

    clearTables()
    dropTables()


#     print("hello")
#     print("Creating all tables")
#     createTables()
#     print("Add file {1, pdf, 100}")
#     print(addFile(File(1, 'pdf', 100)))
#     print("Add file {2, pdf, 50}")
#     print(addFile(File(2, 'pdf', 50)))
#     print("Add file {3, pdf, 50}")
#     print(addFile(File(3, 'pdf', 50)))
#     print("Add file {4, pdf, 50}")
#     print(addFile(File(4, 'pdf', 50)))
#     print("Add file {5, pdf, 50}")
#     print(addFile(File(5, 'pdf', 60)))
#     print("Add file {6, pdf, 50}")
#     print(addFile(File(6, 'pdf', 50)))
#     print("Add disk {10, c1, 2, 90, 300}")
#     print(addDisk(Disk(10, 'c1', 2, 900, 300)))
#     print("Add disk {11, c1, 2, 90, 300}")
#     print(addDisk(Disk(11, 'c1', 2, 900, 300)))
#
#     print("Add RAM {100, c2, 40}")
#     print(addRAM(RAM(100, 'c2', 40)))
#
#     print("Add ram 100 to disk 10")
#   #  print(addRAMToDisk(100, 10))
#
#     print("Add file 1 to disk 10")
#     print(addFileToDisk(File(1, 'pdf', 100), 10))
#     print("Add file 1 to disk 11")
#     print(addFileToDisk(File(2, 'pdf', 100), 10))
#
#     print(getCloseFiles(1))
#
#     # print("remove file 1 to disk 10")
#     # print(removeFileFromDisk(File(1, 'pdf', 100), 10))
#
#
#
#
#     # print("Add file {2, pdf, 100} and disk {20, c1, 2, 1000, 300}")
#     # print(addDiskAndFile(Disk(20, 'c1', 2, 1000, 300), File(2, 'pdf', 100)))
#
#     # print('Can\'t reinsert the same row')
#     # print(addFile(File(1, 'pdf', 100)))
#     #
#     # print("get file")
#     # print(getFileByID(1).getFileID())
#     # print("get disk")
#     # print(getDiskByID(10).getDiskID())
#     # print("get RAM")
#     # print(getRAMByID(100).getRamID())
#     #
#     # print("Delete file {1, pdf, 100}")
#     # print(deleteFile(File(1, 'pdf', 100)))
#     # print("Delete disk {10, c1, 2, 1000, 300}")
#     # print(deleteDisk(10))
#     # print("Delete RAM {100, c2, 40}")
#     # print(deleteRAM(100))
#     #
#     # print("Delete when it doent exist")
#     # print("Delete file {1, pdf, 100}")
#     # print(deleteFile(File(1, 'pdf', 100)))
#     # print("Delete disk {10, c1, 2, 1000, 300}")
#     # print(deleteDisk(10))
#     # print("Delete RAM {100, c2, 40}")
#     # print(deleteRAM(100))
#     #
#     # print("Add file {1, pdf, 100}")
#     # print(addFile(File(1, 'pdf', 100)))
#
