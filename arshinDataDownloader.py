import requests
import zipfile
import os
from tqdm import tqdm  # Для отображения прогресса загрузки
import pandas as pd
from sqlalchemy import create_engine, text
import requests, re
import logging
from os import path
from sqlalchemy import Column, Integer, BigInteger, VARCHAR, Boolean, SmallInteger, Date, create_engine, Table, MetaData, TEXT
from sqlalchemy.schema import ForeignKey
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.dialects.postgresql import TSVECTOR
import psycopg2 
import hashlib
import zipfile
from pathlib import Path
import time

Base = declarative_base()
metadata = MetaData()
# ----------------------- Модели базы данных

# class UniquePoveritelOrgs(Base):
# 	__tablename__ = 'UniquePoveritelOrgs'
# 	id = Column(Integer(), primary_key=True)
# 	poveritelOrg = Column(VARCHAR(256), unique=True)


# class UniqueTypeNames(Base):
#     __tablename__ = 'UniqueTypeNames'
#     id = Column(Integer(), primary_key=True)
#     typeName = Column(VARCHAR(512))

# class UniqueRegisterNumbers(Base):
# 	__tablename__ = 'UniqueRegisterNumbers'
# 	id = Column(Integer(), primary_key=True)
# 	registerNumber = Column(VARCHAR(16), unique=True)

# class UniqueTypes(Base):
#     __tablename__ = 'UniqueTypes'
#     id = Column(Integer(), primary_key=True)
#     type = Column(VARCHAR(4096))
     
     
# class UniqueModifications(Base):
#     __tablename__ = 'UniqueModifications'
#     id = Column(Integer(), primary_key=True)
#     modification = Column(VARCHAR(2048))


# class EquipmentInfoPartitioned(Base):
#     __tablename__ = 'EquipmentInfoPartitioned'
#     id = Column(Integer(), primary_key=True)
#     serialNumber = Column(VARCHAR(256))
#     svidetelstvoNumber = Column(VARCHAR(256))
#     poverkaDate = Column(Date())
#     konecDate = Column(Date())
#     vri_id = Column(Integer())
#     isPrigodno = Column(Boolean())
#     poveritelOrgId = Column(Integer(), ForeignKey('UniquePoveritelOrgs.id'))
#     typeNameId = Column(Integer(), ForeignKey('UniqueTypeNames.id'))
#     registerNumberId = Column(Integer(), ForeignKey('UniqueRegisterNumbers.id'))
#     typeId = Column(Integer(), ForeignKey('UniqueTypes.id'))
#     modificationId = Column(Integer(), ForeignKey('UniqueModifications.id'))
#     year = Column(SmallInteger())


class UniquePoveritelOrgs(Base):
	__tablename__ = 'UniquePoveritelOrgs'
	id = Column(Integer(), primary_key=True)
	poveritelOrg = Column(TEXT, unique=True)


class UniqueTypeNames(Base):
    __tablename__ = 'UniqueTypeNames'
    id = Column(Integer(), primary_key=True)
    typeName = Column(TEXT)

class UniqueRegisterNumbers(Base):
	__tablename__ = 'UniqueRegisterNumbers'
	id = Column(Integer(), primary_key=True)
	registerNumber = Column(TEXT, unique=True)

class UniqueTypes(Base):
    __tablename__ = 'UniqueTypes'
    id = Column(Integer(), primary_key=True)
    type = Column(TEXT)
     
     
class UniqueModifications(Base):
    __tablename__ = 'UniqueModifications'
    id = Column(Integer(), primary_key=True)
    modification = Column(TEXT)


class EquipmentInfoPartitioned(Base):
    __tablename__ = 'EquipmentInfoPartitioned'
    id = Column(Integer(), primary_key=True)
    serialNumber = Column(TEXT)
    svidetelstvoNumber = Column(TEXT)
    poverkaDate = Column(Date())
    konecDate = Column(Date())
    vri_id = Column(Integer())
    isPrigodno = Column(Boolean())
    poveritelOrgId = Column(Integer(), ForeignKey('UniquePoveritelOrgs.id'))
    typeNameId = Column(Integer(), ForeignKey('UniqueTypeNames.id'))
    registerNumberId = Column(Integer(), ForeignKey('UniqueRegisterNumbers.id'))
    typeId = Column(Integer(), ForeignKey('UniqueTypes.id'))
    modificationId = Column(Integer(), ForeignKey('UniqueModifications.id'))
    year = Column(SmallInteger())


class DownloadedFiles(Base):
    __tablename__ = 'DownloadedFiles'
    id = Column(BigInteger, primary_key=True)
    fileId = Column(BigInteger)
    fileName = Column(VARCHAR(256))


# temp_table = Table('temp_table', metadata,
#     Column('serialNumber', VARCHAR(256)),
#     Column('svidetelstvoNumber', VARCHAR(256)),
#     Column('poverkaDate', Date()),
#     Column('poveritelOrg', VARCHAR(256)),
#     Column('typeName', VARCHAR(512)),
#     Column('registerNumber', VARCHAR(16)),
#     Column('type', VARCHAR(4096)),
#     Column('modification', VARCHAR(2048)),
#     Column('konecDate', Date()),
#     Column('vri_id', BigInteger()),
#     Column('isPrigodno', Boolean()),
#     Column('year', SmallInteger)
# )

temp_table = Table('temp_table', metadata,
    Column('serialNumber', TEXT),
    Column('svidetelstvoNumber', TEXT),
    Column('poverkaDate', Date()),
    Column('poveritelOrg', TEXT),
    Column('typeName', TEXT),
    Column('registerNumber', TEXT),
    Column('type', TEXT),
    Column('modification', TEXT),
    Column('konecDate', Date()),
    Column('vri_id', BigInteger()),
    Column('isPrigodno', Boolean()),
    Column('year', SmallInteger)
)

class ArshinDataDownloader:
    '''Класс отвечает за скачивание, распаковку и загрузку в БД инф-ии с сайта ФГИС Аршин'''

    # Фильтр, исключающий предупреждения
    class NoWarningFilter(logging.Filter):
        def filter(self, record):
            return record.levelno not in (pd.errors.DtypeWarning,)

    def __init__(self):
        self.rowCount = 0

        self.__startUrl = 'https://fgis.gost.ru/fundmetrology/exporter/poverki/'
        #self.__current_year =  datetime.now().year
        self.__engine = create_engine('postgresql://postgres:password@localhost:5432/Arshindb')
        self.__Session = sessionmaker(bind=self.__engine)
        self.__session = self.__Session()

        # Настройка логгера
        self.logger = logging.getLogger('arshin_logger')
        self.logger.setLevel(logging.DEBUG)  # Установите уровень логирования на DEBUG
        
        # Определение пути к файлу логирования
        script_directory = os.path.dirname(os.path.abspath(__file__))
        log_file_path = os.path.join(script_directory, 'arshinDataDownloader.log')
        
        # Создание обработчика для записи в файл
        self.file_handler = logging.FileHandler(log_file_path)
        self.file_handler.setLevel(logging.DEBUG)

        # Создание обработчика для вывода на консоль
        self.console_handler = logging.StreamHandler()
        self.console_handler.setLevel(logging.DEBUG)

        # Установка формата для обработчиков
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        self.file_handler.setFormatter(formatter)
        self.console_handler.setFormatter(formatter)

        # Добавляем фильтр к обработчикам
        self.console_handler.addFilter(ArshinDataDownloader.NoWarningFilter())
        self.file_handler.addFilter(ArshinDataDownloader.NoWarningFilter())

        # Добавление обработчиков к логгеру
        self.logger.addHandler(self.file_handler)
        self.logger.addHandler(self.console_handler)


    # def download_file_with_retry(self, url, local_filename, max_retries=10, retry_delay=10, chunk_size=1024):
    #     # Проверяем наличие файла и его размер
    #     offset = os.path.getsize(local_filename) if os.path.exists(local_filename) else 0

    #     for attempt in range(max_retries):
    #         try:
    #             headers = {'Range': f'bytes={offset}-'}
    #             response = requests.get(url, headers=headers, stream=True)
    #             response.raise_for_status()  # Поднять исключение для кода статуса HTTP 4xx/5xx
                
    #             total_size = int(response.headers.get('content-length', 0)) + offset

    #             with open(local_filename, 'ab') as f:
    #                 pbar = tqdm(total=total_size, initial=offset, unit='B', unit_scale=True, desc=local_filename)
    #                 for chunk in response.iter_content(chunk_size=chunk_size):
    #                     if chunk:
    #                         f.write(chunk)
    #                         pbar.update(len(chunk))
    #                 pbar.close()
                
    #             # Проверка, завершена ли загрузка
    #             if os.path.getsize(local_filename) >= total_size:
    #                 return True
    #             else:
    #                 self.logger.info(f"Скачивание не завершено, продолжение с {os.path.getsize(local_filename)}...")
    #                 offset = os.path.getsize(local_filename)  # обновить смещение

    #         except requests.exceptions.RequestException as e:
    #             self.logger.error(f"Произошла ошибка: {e}. Повторение скачивания через {retry_delay} секунд...")
    #             time.sleep(retry_delay)
    #             offset = os.path.getsize(local_filename)  # обновить смещение

    #     return False


    def download_file_with_retry(self, url, local_filename, max_retries=10, retry_delay=10, chunk_size=1024):
        # Проверяем наличие файла и его размер
        offset = os.path.getsize(local_filename) if os.path.exists(local_filename) else 0

        for attempt in range(max_retries):
            try:
                headers = {'Range': f'bytes={offset}-'}
                response = requests.get(url, headers=headers, stream=True)
                response.raise_for_status()  # Поднять исключение для кода статуса HTTP 4xx/5xx
                
                total_size = int(response.headers.get('content-length', 0)) + offset

                with open(local_filename, 'ab') as f:
                    pbar = tqdm(total=total_size, initial=offset, unit='B', unit_scale=True, desc=local_filename)
                    for chunk in response.iter_content(chunk_size=chunk_size):
                        if chunk:
                            f.write(chunk)
                            pbar.update(len(chunk))
                    pbar.close()
                
                # Проверка, завершена ли загрузка
                if os.path.getsize(local_filename) >= total_size:
                    return True
                else:
                    self.logger.info(f"Скачивание не завершено, продолжение с {os.path.getsize(local_filename)}...")
                    offset = os.path.getsize(local_filename)  # обновить смещение

            except requests.exceptions.RequestException as e:
                self.logger.error(f"Произошла ошибка: {e}. Повторение скачивания через {retry_delay} секунд...")
                time.sleep(retry_delay)
                if self.IsNewFile == False:
                    offset = os.path.getsize(local_filename)  # обновить смещение

        return False



    def __unzip_file(self, zip_filename, extract_to):
        '''Распаковывает zip архив'''
        with zipfile.ZipFile(zip_filename, 'r') as zip_ref:
            zip_ref.extractall(extract_to)
            # Удаляем распакованный архив
        os.remove(zip_filename)


        
    def calculate_md5(self, file_path):
        # Вычисляем хэш-сумму MD5 файла
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()


    def is_folder_empty(self, folder_path):
        folder = Path(folder_path)
        # Проверяем, содержит ли папка файлы
        return not any(folder.iterdir())

    
    emptyFolderWasFind = False

    # def extract_and_add_to_db_old_files(self, rootPath):
    #     """ Разархивирует все вложенные архивы, добавляет в БД и удаляет все вложенные в них файлы"""
    #     for z, dirs, files in os.walk(rootPath):
    #         #files_to_process = files[:]
    #         # Проверяем содержит ли папка файлы
    #         #if len(files) > 0:
    #         for filename in files:
    #             fileSpec = os.path.join(z, filename)
    #             if filename[-4:] == '.zip':
    #                 if 'snapshot' in fileSpec.split('\\')[-1]:
    #                     self.__unzip_file(fileSpec, self.rootPath)
    #                     self.extract_and_add_to_db_old_files(self.rootPath)
    #                 self.__unzip_file(fileSpec, z)
    #                 self.__add_file_to_db(fileSpec[:-4])
    #                 self.logger.info(f'Файл {filename[:-4]} добавлен в БД')
    #                 os.remove(os.path.join(fileSpec[:-4]))
    #             elif filename[-4:] == '.csv':
    #                 self.__add_file_to_db(fileSpec)
    #                 self.logger.info(f'Файл {filename} добавлен в БД')
    #                 os.remove(fileSpec)
    #             self.extract_and_add_to_db_old_files(z)
    #         #else:
    #         # for dir in dirs:
    #         #     if (self.is_folder_empty(os.path.join(z, dir)) == True):
    #         #         os.rmdir(os.path.join(z, dir))
    #         #         self.logger.info('777')
    #         #         #self.logger.info(f'Всего было прочитано {self.rowCount} строк')
    #         #         self.extract_and_add_to_db_old_files(rootPath)
    #     self.logger.info(f'Всего было прочитано {self.rowCount} строк')
    #     return 0

    def extract_and_add_to_db_old_files(self, rootPath):
        """ Разархивирует все вложенные архивы, добавляет в БД и удаляет все вложенные в них файлы"""
        
        for z, dirs, files in os.walk(rootPath):

            # Проверяем содержит ли папка файлы
            for filename in files:
                fileSpec = os.path.join(z, filename)
                
                if filename.endswith('.zip'):
                    # Проверяем на наличие 'snapshot' в имени файла
                    if 'snapshot' in fileSpec.split('\\')[-1]:
                        self.__unzip_file(fileSpec, self.rootPath)
                        self.extract_and_add_to_db_old_files(self.rootPath)  # Рекурсия для новых файлов внутри snapshot
                    else:
                        self.__unzip_file(fileSpec, z)
                    
                    self.__add_file_to_db(fileSpec[:-4])
                    self.logger.info(f'Файл {filename[:-4]} добавлен в БД')
                    os.remove(fileSpec[:-4])
                
                elif filename.endswith('.csv'):
                    self.__add_file_to_db(fileSpec)
                    self.logger.info(f'Файл {filename} добавлен в БД')
                    os.remove(fileSpec)

        self.logger.info(f'Всего было прочитано {self.rowCount} строк')

        self.__remove_empty_dirs(rootPath)
        return 0
    
    def __remove_empty_dirs(self, rootPath):
        """ Рекурсивно удаляет все пустые папки, начиная с самой глубокой"""
        for dirpath, dirnames, filenames in os.walk(rootPath, topdown=False):  # topdown=False позволяет идти с самого конца
            # Удаляем папку, если в ней нет файлов и папок
            if not dirnames and not filenames:
                os.rmdir(dirpath)
                self.logger.info(f'Удалена пустая папка: {dirpath}')
                if dirpath == self.rootPath:
                    return 0
                self.__remove_empty_dirs(self.rootPath)


    # def extract_and_add_to_db_old_files(self, rootPath):
    #     """ Разархивирует все вложенные архивы, добавляет в БД и удаляет все вложенные в них файлы"""
        
    #     # Проходим по папке
    #     for z, dirs, files in os.walk(rootPath):
    #         files_to_process = files[:]  # Создаем копию списка файлов для работы
            
    #         # Проверяем содержит ли папка файлы
    #         for filename in files_to_process:
    #             fileSpec = os.path.join(z, filename)
                
    #             # Если файл - zip архив
    #             if filename.endswith('.zip'):
                    
    #                 # Проверяем на наличие 'snapshot' в имени
    #                 if 'snapshot' in fileSpec.split('\\')[-1]:
    #                     self.__unzip_file(os.path.join(z, filename), self.rootPath)
    #                     self.extract_and_add_to_db_old_files(self.rootPath)  # Рекурсивный вызов
                    
    #                 # Разархивируем и добавляем файл в БД
    #                 self.__unzip_file(fileSpec, z)
    #                 self.__add_file_to_db(fileSpec[:-4])
    #                 self.logger.info(f'Файл {filename[:-4]} добавлен в БД')
                    
    #                 # Удаляем файл и обновляем список
    #                 os.remove(fileSpec)
    #                 self.logger.info(f'Файл {filename[:-4]} удален')
                
    #             # Если файл - CSV
    #             elif filename.endswith('.csv'):
    #                 self.__add_file_to_db(fileSpec)
    #                 self.logger.info(f'Файл {filename} добавлен в БД')
                    
    #                 # Удаляем CSV файл и обновляем список
    #                 os.remove(fileSpec)
    #                 self.logger.info(f'Файл {filename} удален')

    #         # Проверяем директории
    #         for dir in dirs:
    #             dirPath = os.path.join(z, dir)
    #             if self.is_folder_empty(dirPath):
    #                 os.rmdir(dirPath)
    #                 self.logger.info(f'Пустая папка {dir} удалена')
        
    #     self.logger.info(f'Всего было прочитано {self.rowCount} строк')
    #     return

                

    def __add_old_data(self, responseData):
        '''Добавляет в БД все вложенные в архив файлы и удаляет'''

        # Запрос для получения содержимого файла
        # newFileName = responseData['snapshots'][-1]['relativeUri']
        # correctFileHashSum = responseData['snapshots'][-1]['md5sum']

        # startAbsUrl = os.path.join(os.path.abspath(newFileName)).replace(newFileName, '')
        # downloadedZipPath = startAbsUrl + newFileName

        # # Если файл скачен без ошибок и пройдена проверка целостности
        # if self.download_file_with_retry(self.__startUrl + newFileName, downloadedZipPath) and self.calculate_md5(downloadedZipPath) == correctFileHashSum:
        #     self.logger.info(f'Файл {newFileName} был скачан успешно')
        #     downloadedFolderPath = downloadedZipPath[:-4]

        #     self.__unzip_file(downloadedZipPath, downloadedFolderPath)
            
        #     self.rootPath = downloadedFolderPath
        #     self.extract_and_add_to_db_old_files(self.rootPath)
        #     os.rmdir(self.rootPath)
        # else:
        #     self.logger.info(f'Файл {newFileName} не был скачан')

        self.rootPath = 'C:\\Users\\LIKORIS001\\Desktop\\NewCreateScript\\usr'
        self.extract_and_add_to_db_old_files(self.rootPath)

        # if (self.is_folder_empty(self.rootPath) == True):
        #     os.rmdir(self.rootPath)


    def get_new_file_names_and_identifiers(self, responseData):
        '''Возвращает списки наименований файлов и их идентификаторов, которые надо закачать (исключает уже скачанные файлы)'''

        # Получаем список ранее скачанных файлов и их идентификаторов из БД 
        oldColumn = self.__session.query(DownloadedFiles.fileId, DownloadedFiles.fileName).all()
        
        oldIdentifiers = set([str(row[0]) for row in oldColumn])
        oldFileNames = set([row[1] for row in oldColumn])
        currentIdentifiers = []

        currentIdentifiers = [delta['id'] for delta in responseData['deltas']]
        currentFileNames = [delta['relativeUri'] for delta in responseData['deltas']]

        newIdentifiers = list(set(currentIdentifiers).difference(oldIdentifiers))
        newFileNames = list(set(currentFileNames).difference(oldFileNames))

        correctFileHashSums = []
        for newFileName in newFileNames:
            for delta in responseData['deltas']:
                if delta['relativeUri'] == newFileName:
                    correctFileHashSums.append(delta['md5sum'])

        # Проверка, что списки одинаковой длины
        if len(newIdentifiers) != len(newFileNames):
            self.logger.error("Ошибка чтения manifest.json, списки должны быть одинаковой длины ", exc_info=True)
            raise ValueError("Списки данных должны быть одинаковой длины")

        return newFileNames, newIdentifiers, correctFileHashSums


    def __add_new_data(self, responseData):
        
        '''Скачивает, разархивирует, добавляет в БД и удаляет все новые файлы'''

        res = self.get_new_file_names_and_identifiers(responseData)
        newFileNames = res[0]
        newIdentifiers = res[1]
        correctFileHashSums = res[2]
        countFiles = 0

        for newIdentifier, newFileName, correctFileHashSum in zip(newIdentifiers, newFileNames, correctFileHashSums):
            startAbsUrl = os.path.join(path.abspath(newFileName)).replace(newFileName, '').replace("\\", '/')
            zipPath = startAbsUrl + newFileName

            if self.download_file_with_retry(self.__startUrl + newFileName, zipPath) and self.calculate_md5(zipPath) == correctFileHashSum:
                filePath = zipPath[:-4]
                self.__unzip_file(zipPath, filePath)
                self.__add_file_to_db(filePath + '/' + newFileName[:-4])
                self.logger.info(f'Файл {newFileName[:-4]} добавлен в БД')
                new_record = DownloadedFiles(fileId=newIdentifier, fileName=newFileName)
                self.__session.add(new_record)
                self.__session.commit()
                # Удаляем файл
                os.remove(filePath + '/' + newFileName[:-4])
                # Удаляем пустую папку
                os.rmdir(filePath)
                countFiles += 1

            else:
                self.logger.error(f'Файл {newFileName} не был скачан')

        self.__session.close()
        self.logger.info(f"Количество добавленных файлов = {countFiles}")


    def __add_file_to_db(self, path):
            '''Читает содержимое файла, валидирует и записывает в БД. Запись ведётся в 3 таблицы.'''

            def convert_to_date(xVal):
                xVals = str(xVal).split(' ')
                for x in xVals:
                    if len(x) == 10:
                        x = re.sub(r"[.,:;]", "-", x)
                        year, month, day = x.split("-")
                        return f"{year}-{month}-{day}"
           
                return pd.NaT  # Если формат не распознан, вернуть NaT (Not a Time)

            names = ['Number', 'poveritelOrg', 'registerNumber', 'typeName', 'type', 'modification',
                        'serialNumber', 'poverkaDate', 'konecDate', 'svidetelstvoNumber', 'isPrigodno',
                        'Date1', 'Date2', 'Pusto', 'vri_id', 'rabbish']
            df = pd.read_csv(path, chunksize = 100000, on_bad_lines='skip', delimiter=';',quotechar='"', header=None, names=names)#, header = 0)

            readRowsCount = 0

            

            firstTime = True

            stmt = text("""INSERT INTO "EquipmentInfoPartitioned" ("vri_id", "serialNumber", "poverkaDate", "konecDate", "svidetelstvoNumber", "isPrigodno", "year",
                "poveritelOrgId", "typeNameId", "registerNumberId", "typeId", "modificationId")
            SELECT "vri_id", "serialNumber", "poverkaDate", "konecDate", "svidetelstvoNumber", "isPrigodno", EXTRACT(YEAR FROM "poverkaDate"),
                "UniquePoveritelOrgs"."id", "UniqueTypeNames"."id", "UniqueRegisterNumbers"."id", "UniqueTypes"."id", "UniqueModifications"."id"
            FROM "temp_table"
            JOIN "UniquePoveritelOrgs" ON "temp_table"."poveritelOrg" = "UniquePoveritelOrgs"."poveritelOrg"
            JOIN "UniqueTypeNames" ON "temp_table"."typeName" = "UniqueTypeNames"."typeName"
            JOIN "UniqueRegisterNumbers" ON "temp_table"."registerNumber" = "UniqueRegisterNumbers"."registerNumber"
            JOIN "UniqueTypes" ON "temp_table"."type" = "UniqueTypes"."type"
            JOIN "UniqueModifications" ON "temp_table"."modification" = "UniqueModifications"."modification"
            WHERE EXTRACT(YEAR FROM "poverkaDate") > 2018
            """)
            writtenRowsCount = 0
            for chunk in df:

                chunk = chunk.drop(columns=['Number', 'Date1', 'Date2', 'Pusto', 'rabbish'])

                readRowsCount += chunk.shape[0] 
                self.rowCount += chunk.shape[0]
                startChunk = chunk

                # Применяем преобразование к колонкам с датами
                chunk['poverkaDate'] = chunk['poverkaDate'].map(convert_to_date)
                chunk['konecDate'] = chunk['konecDate'].map(convert_to_date)

                # Преобразуем даты в тип datetime для pandas с errors='coerce'
                chunk['poverkaDate'] = pd.to_datetime(chunk['poverkaDate'], errors='coerce')
                chunk['konecDate'] = pd.to_datetime(chunk['konecDate'], errors='coerce')


                allColumns = ['poveritelOrg', 'registerNumber', 'typeName', 'type', 'modification',
                'serialNumber', 'poverkaDate', 'konecDate', 'svidetelstvoNumber', 'isPrigodno', 'vri_id']

                chunk[allColumns].replace('  ', ' ')
                chunk['isPrigodno'] = chunk['isPrigodno'].astype('bool')
                chunk['vri_id'] = pd.to_numeric(chunk['vri_id'], errors='coerce').astype('Int64')


                chunk = chunk.dropna(subset=['poverkaDate'])

                # Удаляем строки, где "svidetelstvoNumber" состоит только из дефисов и пробелов
                chunk = chunk[~chunk['svidetelstvoNumber'].str.fullmatch(r'[-\s]+', na=False)]

                # Откидываем строки, где 'konecDate' равно None и 'isPrigodno' равно True
                #chunk = chunk[~((chunk['konecDate'].isna()) & (chunk['isPrigodno'] == True))]

                chunk = chunk.where(pd.notnull(chunk), None) # Новый код



                # Будем добавлять данные в соответствующие колонки этих двух таблиц 
                table_column = {'UniquePoveritelOrgs' : 'poveritelOrg', 'UniqueTypeNames' : 'typeName', 'UniqueRegisterNumbers' : 'registerNumber', 'UniqueTypes' : 'type', 'UniqueModifications' : 'modification'}
                for table, col in table_column.items():

                    old = pd.read_sql_table(table, self.__engine)[col].to_frame()
                    new = chunk[col].to_frame()

                    merged_data = new.merge(old, on=col, how='left', indicator=True)

                    missing_data = merged_data[merged_data['_merge'] == 'left_only'].drop(columns=['_merge'])
                    unique_missing_data = missing_data.drop_duplicates(subset=col)

                    unique_missing_data = unique_missing_data.dropna()

                    if not unique_missing_data.empty:
                        unique_missing_data.to_sql(name=table, con=self.__engine, if_exists='append', index=False)


                dtype2 = {
                'serialNumber': TEXT,
                'svidetelstvoNumber': TEXT,
                'registerNumber': TEXT,
                'typeName': TEXT,
                'type': TEXT,
                'modification': TEXT,
                'poveritelOrg': TEXT,
                'poverkaDate': Date,
                'konecDate': Date,
                'vri_id': BigInteger,
                'isPrigodno': Boolean,
                'year': SmallInteger
                }
                if firstTime:
                    chunk.to_sql('temp_table', self.__engine, if_exists='replace', index=False, dtype=dtype2)
                else:
                    chunk.to_sql('temp_table', self.__engine, if_exists='append', index=False, dtype=dtype2)
                    firstTime = False
                
                writtenRowsCount += chunk.shape[0]

                # Объединяем два DataFrame и используем indicator для пометки различий
                # Объединяем два DataFrame с использованием merge и параметра indicator
                diff_df = pd.merge(startChunk, chunk, how='outer', indicator=True)

                # Выбираем строки, которые есть только в одном из DataFrame
                diff_rows = diff_df[diff_df['_merge'] != 'both']
                diff_rows.to_csv('invalid_rows.csv', mode='a', header=False, index=False)

                metadata = MetaData()
                metadata.reflect(bind=self.__engine, only=['temp_table'])


                self.__session.execute(stmt)
                self.__session.commit()

            self.logger.info(f'Прочитано {readRowsCount} строк')
            self.logger.info(f'Записано {writtenRowsCount} строк')




    def Main(self):

        #downloadOldData = int(input('Если хотите скачать даные за прошлые месяца, введите 1, иначе 0: '))
        #downloadOldData = 2
        for downloadOldData in range(1, 3):
            response = requests.get(self.__startUrl + 'manifest.json')

            self.logger.info(f"Статус код ответа от Аршин: {response.status_code}")

            # Проверка успешности запроса
            if response.status_code == 200:

                # Преобразование содержимого в JSON
                data = response.json()
                if downloadOldData == 1:
                    self.IsNewFile = False
                    self.__add_old_data(data)
                else:
                    #self.__add_file_to_db('C:\\Users\\LIKORIS001\\Desktop\\NewCreateScript\\poverki.delta.20240712--20240713.csv')
                    self.IsNewFile = True
                    self.rowCount = 0
                    self.__add_new_data(data)
                    self.logger.info(f'Всего было прочитано {self.rowCount} строк')

ekz = ArshinDataDownloader()
ekz.Main()



 # def __add_file_to_db(self, path):
    #     '''Читает содержимое файла, валидирует и записывает в БД. Запись ведётся в 3 таблицы.'''

    #     # def convert_to_date(x):
    #     #     x = str(x)
    #     #     # Пробуем преобразование для форматов DD.MM.YYYY и вариаций
    #     #     if re.match(r"\d{2}[-.,:;]\d{2}[-.,:;]\d{4}", x):
    #     #         # Нормализуем разделители к '-'
    #     #         x = re.sub(r"[.,:;]", "-", x)
    #     #         # Преобразуем в формат YYYY-MM-DD
    #     #         day, month, year = x.split("-")
    #     #         return f"{year}-{month}-{day}"
    #     #     elif re.match(r"\d{4}[-.,:;]\d{2}[-.,:;]\d{2}", x):
    #     #         # Если формат уже YYYY-MM-DD или его вариация, нормализуем разделители
    #     #         return re.sub(r"[.,:;]", "-", x)
    #     #     else:
    #     #         return pd.NaT  # Если формат не распознан, вернуть NaT (Not a Time)

    #     def convert_to_date(xVal):
    #         xVals = str(xVal).split(' ')
    #         for x in xVals:
    #             if len(x) == 10:
    #                 x = re.sub(r"[.,:;]", "-", x)
    #                 year, month, day = x.split("-")
    #                 return f"{year}-{month}-{day}"

    #         # # Пробуем преобразование для форматов DD.MM.YYYY и вариаций
    #         # if re.match(r"\d{2}[-.,:;]\d{2}[-.,:;]\d{4}", x):
    #         #     # Нормализуем разделители к '-'
    #         #     x = re.sub(r"[.,:;]", "-", x)
    #         #     # Преобразуем в формат YYYY-MM-DD
    #         #     day, month, year = x.split("-")
    #         #     return f"{year}-{month}-{day}"
    #         # elif re.match(r"\d{4}[-.,:;]\d{2}[-.,:;]\d{2}", x):
    #         #     # Если формат уже YYYY-MM-DD или его вариация, нормализуем разделители
    #         #     return re.sub(r"[.,:;]", "-", x)                 
    #         return pd.NaT  # Если формат не распознан, вернуть NaT (Not a Time)

    #     names = ['Number', 'poveritelOrg', 'registerNumber', 'typeName', 'type', 'modification',
    #                  'serialNumber', 'poverkaDate', 'konecDate', 'svidetelstvoNumber', 'isPrigodno',
    #                  'Date1', 'Date2', 'Pusto', 'vri_id', 'rabbish']
    #     df = pd.read_csv(path, chunksize = 100000, on_bad_lines='skip', delimiter=';',quotechar='"', header=None, names=names)#, header = 0)

    #     readRowsCount = 0

        

    #     firstTime = True

    #     stmt = text("""INSERT INTO "EquipmentInfoPartitioned" ("vri_id", "serialNumber", "poverkaDate", "konecDate", "svidetelstvoNumber", "isPrigodno", "year",
    #         "poveritelOrgId", "typeNameId", "registerNumberId", "typeId", "modificationId")
    #     SELECT "vri_id", "serialNumber", "poverkaDate", "konecDate", "svidetelstvoNumber", "isPrigodno", EXTRACT(YEAR FROM "poverkaDate"),
    #         "UniquePoveritelOrgs"."id", "UniqueTypeNames"."id", "UniqueRegisterNumbers"."id", "UniqueTypes"."id", "UniqueModifications"."id"
    #     FROM "temp_table"
    #     JOIN "UniquePoveritelOrgs" ON "temp_table"."poveritelOrg" = "UniquePoveritelOrgs"."poveritelOrg"
    #     JOIN "UniqueTypeNames" ON "temp_table"."typeName" = "UniqueTypeNames"."typeName"
    #     JOIN "UniqueRegisterNumbers" ON "temp_table"."registerNumber" = "UniqueRegisterNumbers"."registerNumber"
    #     JOIN "UniqueTypes" ON "temp_table"."type" = "UniqueTypes"."type"
    #     JOIN "UniqueModifications" ON "temp_table"."modification" = "UniqueModifications"."modification"
    #     WHERE EXTRACT(YEAR FROM "poverkaDate") > 2018
    #     """)
    #     writtenRowsCount = 0
    #     for chunk in df:
    #         readRowsCount += chunk.shape[0] 
    #         self.rowCount += chunk.shape[0]
            


    #         chunk = chunk.drop(columns=['Number', 'Date1', 'Date2', 'Pusto', 'rabbish'])

    #          # Применяем преобразование к колонкам с датами
    #         chunk['poverkaDate'] = chunk['poverkaDate'].map(convert_to_date)
    #         chunk['konecDate'] = chunk['konecDate'].map(convert_to_date)

    #         # Преобразуем даты в тип datetime для pandas с errors='coerce'
    #         chunk['poverkaDate'] = pd.to_datetime(chunk['poverkaDate'], errors='coerce')
    #         chunk['konecDate'] = pd.to_datetime(chunk['konecDate'], errors='coerce')

    #         # Сохраним строки с некорректными датами
    #         invalid_rows_poverka = chunk[chunk['poverkaDate'].isna()]
    #         invalid_rows_konec = chunk[chunk['konecDate'].isna()]

    #         allColumns = ['poveritelOrg', 'registerNumber', 'typeName', 'type', 'modification',
    #         'serialNumber', 'poverkaDate', 'konecDate', 'svidetelstvoNumber', 'isPrigodno', 'vri_id']

    #         chunk[allColumns].replace('  ', ' ')
    #         # chunk['poverkaDate'] = pd.to_datetime(chunk['poverkaDate'], format='%Y-%m-%d', errors='coerce')
    #         # chunk['konecDate'] = pd.to_datetime(chunk['konecDate'], format='%Y-%m-%d', errors='coerce')
    #         chunk['isPrigodno'] = chunk['isPrigodno'].astype('bool')
    #         chunk['vri_id'] = pd.to_numeric(chunk['vri_id'], errors='coerce').astype('Int64')

    #         #invalid_rows_vri_id = chunk[chunk['vri_id'].isna()]

    #         # Проверяем строки с некорректным "svidetelstvoNumber" (только дефисы и пробелы)
    #         invalid_rows_svidetelstvo = chunk[chunk['svidetelstvoNumber'].str.fullmatch(r'[-\s]+', na=False)]
            

    #         # Исключаем пустые DataFrame перед объединением
    #         invalid_rows_list = [df for df in [invalid_rows_poverka, invalid_rows_konec, invalid_rows_svidetelstvo] if not df.empty]

    #         # Собираем все некорректные строки, если есть что объединять
    #         if invalid_rows_list:
    #             invalid_rows = pd.concat(invalid_rows_list).drop_duplicates()

    #             # Записываем некорректные строки в CSV файл
    #             invalid_rows.to_csv('invalid_rows.csv', mode='a', header=False, index=False)



    #         # # Собираем все некорректные строки
    #         # invalid_rows = pd.concat([invalid_rows_poverka, invalid_rows_konec, invalid_rows_vri_id, invalid_rows_svidetelstvo]).drop_duplicates()

    #         # # Записываем некорректные строки в CSV файл
    #         # if not invalid_rows.empty:
    #         #     invalid_rows.to_csv('invalid_rows.csv', mode='a', header=False, index=False)

    #         # Отбрасываем строки с некорректными датами (где NaT)
    #         #chunk = chunk.dropna(subset=['poverkaDate'])

    #         chunk = chunk.dropna(subset=['poverkaDate', 'konecDate', 'vri_id'])

    #         # chunk['svidetelstvoNumber'].str.fullmatch(r'[-\s]+', na=False) проверяет, состоит ли значение только из дефисов и пробелов.
    #         # ~ перед вызовом метода означает "не", то есть отбрасываются строки, которые содержат только дефисы и пробелы.
    #         # na=False обрабатывает значения NaN, исключая их из проверки.
    #         # Удаляем строки, где "svidetelstvoNumber" состоит только из дефисов и пробелов
    #         chunk = chunk[~chunk['svidetelstvoNumber'].str.fullmatch(r'[-\s]+', na=False)]

    #         chunk = chunk.where(pd.notnull(chunk), None) # Новый код



    #         # Будем добавлять данные в соответствующие колонки этих двух таблиц 
    #         table_column = {'UniquePoveritelOrgs' : 'poveritelOrg', 'UniqueTypeNames' : 'typeName', 'UniqueRegisterNumbers' : 'registerNumber', 'UniqueTypes' : 'type', 'UniqueModifications' : 'modification'}
    #         for table, col in table_column.items():

    #             # print('UniqueTable:')

    #             old = pd.read_sql_table(table, self.__engine)[col].to_frame()
    #             # print(old)
    #             new = chunk[col].to_frame()
    #             # print('Новые данные:')
    #             # print(new)

    #             # Определяем отсутствующие строки
    #             #merge(): Выполняет левое соединение (left join) между новыми (new) и старыми (old) данными по ключевому столбцу (col).
    #             #indicator=True: Добавляет в результат столбец _merge, который указывает, откуда пришли данные (both, left_only, right_only).
    #             #missing_data: Извлекает только те строки, которые присутствуют в новых данных (new), но отсутствуют в старых данных (old). Это строки, где _merge == 'left_only'.
    #             #drop(columns=['_merge']): Убирает вспомогательный столбец _merge, который использовался для фильтрации данных.

    #             merged_data = new.merge(old, on=col, how='left', indicator=True)

    #             # print(merged_data)
    #             # print('-----------------------')

    #             missing_data = merged_data[merged_data['_merge'] == 'left_only'].drop(columns=['_merge'])
    #             unique_missing_data = missing_data.drop_duplicates(subset=col)


    #             unique_missing_data = unique_missing_data.dropna()

    #             # print(unique_missing_data)
    #             # print('-----------------------')

    #             if not unique_missing_data.empty:
    #                 # Добавляем в столбец categoryName первое слово из строки typeName (для каждой строки typeName)
    #                 #if table == 'UniqueTypeNames':
    #                 #    unique_missing_data['categoryName'] = unique_missing_data['typeName'].apply(lambda x: x.split(' ')[0])

    #                 #self.logger.info(f'Было добавлено {len(unique_missing_data)} уникальных(-е) строк(-и)')
    #                 # Добавляем в таблицы наименования новых организаций и типов устройств 
    #                 unique_missing_data.to_sql(name=table, con=self.__engine, if_exists='append', index=False)

    #         # Типы данных для временной таблицы
    #         # dtype2 = {
    #         # 'serialNumber': VARCHAR(256),
    #         # 'svidetelstvoNumber': VARCHAR(256),
    #         # 'registerNumber': VARCHAR(16),
    #         # 'typeName': VARCHAR(512),
    #         # 'type': VARCHAR(4096),
    #         # 'modification': VARCHAR(2048),
    #         # 'poveritelOrg': VARCHAR(256), # ???????????????
    #         # 'poverkaDate': Date,
    #         # 'konecDate': Date,
    #         # 'vri_id': BigInteger,
    #         # 'isPrigodno': Boolean,
    #         # 'year': SmallInteger
    #         # }
    #         dtype2 = {
    #         'serialNumber': TEXT,
    #         'svidetelstvoNumber': TEXT,
    #         'registerNumber': TEXT,
    #         'typeName': TEXT,
    #         'type': TEXT,
    #         'modification': TEXT,
    #         'poveritelOrg': TEXT,
    #         'poverkaDate': Date,
    #         'konecDate': Date,
    #         'vri_id': BigInteger,
    #         'isPrigodno': Boolean,
    #         'year': SmallInteger
    #         }
    #         if firstTime:
    #             chunk.to_sql('temp_table', self.__engine, if_exists='replace', index=False, dtype=dtype2)
    #         else:
    #             chunk.to_sql('temp_table', self.__engine, if_exists='append', index=False, dtype=dtype2)
    #             firstTime = False
            
    #         writtenRowsCount += chunk.shape[0]

    #         metadata = MetaData()
    #         metadata.reflect(bind=self.__engine, only=['temp_table'])



    #     # WHERE EXTRACT(YEAR FROM "poverkaDate") > 2018;
    #         self.__session.execute(stmt)
    #         self.__session.commit()

    #     self.logger.info(f'Прочитано {readRowsCount} строк')
    #     self.logger.info(f'Записано {writtenRowsCount} строк')
    #     #    print()
    #     #self.logger.info()
