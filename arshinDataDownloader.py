import requests
import zipfile
import os
from tqdm import tqdm  # Для отображения прогресса загрузки
import pandas as pd
from sqlalchemy import create_engine, text
import requests, re
import logging
from os import path
from sqlalchemy import Column, Integer, BigInteger, VARCHAR, Boolean, SmallInteger, Date, create_engine, Table, MetaData
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

class UniquePoveritelOrgs(Base):
	__tablename__ = 'UniquePoveritelOrgs'
	id = Column(BigInteger(), primary_key=True)
	poveritelOrg = Column(VARCHAR(256), unique=True)


class UniqueTypeNames(Base):
    __tablename__ = 'UniqueTypeNames'
    id = Column(BigInteger(), primary_key=True)
    typeName = Column(VARCHAR(512))
    typeName_tsvector = Column(TSVECTOR())

class UniqueRegisterNumbers(Base):
	__tablename__ = 'UniqueRegisterNumbers'
	id = Column(BigInteger(), primary_key=True)
	registerNumber = Column(VARCHAR(16), unique=True)

class UniqueTypes(Base):
    __tablename__ = 'UniqueTypes'
    id = Column(BigInteger(), primary_key=True)
    type = Column(VARCHAR(512))
    type_tsvector = Column(TSVECTOR())
     
     
class UniqueModifications(Base):
    __tablename__ = 'UniqueModifications'
    id = Column(BigInteger(), primary_key=True)
    modification = Column(VARCHAR(512))
    modification_tsvector = Column(TSVECTOR())


class EquipmentInfoPartitioned(Base):
    __tablename__ = 'EquipmentInfoPartitioned'
    id = Column(BigInteger(), primary_key=True)
    serialNumber = Column(VARCHAR(256))
    svidetelstvoNumber = Column(VARCHAR(256))
    poverkaDate = Column(Date())
    konecDate = Column(Date())
    vri_id = Column(BigInteger())
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


temp_table = Table('temp_table', metadata,
    Column('serialNumber', VARCHAR(256)),
    Column('svidetelstvoNumber', VARCHAR(256)),
    Column('poverkaDate', Date()),
    Column('poveritelOrg', VARCHAR(256)),
    Column('typeName', VARCHAR(512)),
    Column('registerNumber', VARCHAR(16)),
    Column('type', VARCHAR(512)),
    Column('modification', VARCHAR(512)),
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

        return False



    def __unzip_file(self, zip_filename, extract_to):
        '''Распаковывает zip архив'''
        with zipfile.ZipFile(zip_filename, 'r') as zip_ref:
            zip_ref.extractall(extract_to)
            # Удаляем распакованный архив
        os.remove(zip_filename)

    def __add_file_to_db(self, path):
        '''Читает содержимое файла, валидирует и записывает в БД. Запись ведётся в 3 таблицы.'''

        firstTime = True

        names = ['Number', 'poveritelOrg', 'registerNumber', 'typeName', 'type', 'modification',
                     'serialNumber', 'poverkaDate', 'konecDate', 'svidetelstvoNumber', 'isPrigodno',
                     'Date1', 'Date2', 'Pusto', 'vri_id', 'rabbish']
        df = pd.read_csv(path, chunksize = 100000, on_bad_lines='skip', delimiter=';',quotechar='"', header=None, names=names)#, header = 0)

        for chunk in df:
            
            chunk = chunk.drop(columns=['Number', 'Date1', 'Date2', 'Pusto', 'rabbish'])
            # search = lambda x: x if re.search(r"\d{4}-\d{2}-\d{2}", str(x)) else 'not found'
            search = lambda x: x if re.search(r"\d{4}-\d{2}-\d{2}", str(x)) or re.search(r"\d{4},\d{2},\d{2}", str(x)) \
                 or re.search(r"\d{4}.\d{2}.\d{2}", str(x)) or re.search(r"\d{4}:\d{2}:\d{2}", str(x)) or re.search(r"\d{4};\d{2};\d{2}", str(x)) else 'not found'

            chunk['poverkaDate'] = chunk['poverkaDate'].map(search)
            chunk['konecDate'] = chunk['konecDate'].map(search)
            chunk = chunk.drop(chunk.query('poverkaDate == "not found"').index)
            chunk = chunk.drop(chunk.query('konecDate == "not found"').index)

            chunk['poverkaDate'].replace(to_replace=['.', ',', ':', ';'], value='-')
            chunk['konecDate'].replace(to_replace=['.', ',', ':', ';'], value='-')
            

            allColumns = ['poveritelOrg', 'registerNumber', 'typeName', 'type', 'modification',
            'serialNumber', 'poverkaDate', 'konecDate', 'svidetelstvoNumber', 'isPrigodno', 'vri_id']

            chunk[allColumns].replace('  ', ' ')
            chunk['poverkaDate'] = pd.to_datetime(chunk['poverkaDate'], format='%Y-%m-%d', errors='coerce')
            chunk['konecDate'] = pd.to_datetime(chunk['konecDate'], format='%Y-%m-%d', errors='coerce')
            chunk['isPrigodno'] = chunk['isPrigodno'].astype('bool')
            chunk['vri_id'] = pd.to_numeric(chunk['vri_id'], errors='coerce').astype('Int64')
            chunk = chunk.where(pd.notnull(chunk), None) # Новый код

            # Будем добавлять данные в соответствующие колонки этих двух таблиц 
            table_column = {'UniquePoveritelOrgs' : 'poveritelOrg', 'UniqueTypeNames' : 'typeName', 'UniqueRegisterNumbers' : 'registerNumber', 'UniqueTypes' : 'type', 'UniqueModifications' : 'modification'}
            for table, col in table_column.items():
                old = pd.read_sql_table(table, self.__engine)[col].to_frame()
                new = chunk[col].to_frame()

                # Определяем отсутствующие строки
                merged_data = new.merge(old, on=col, how='left', indicator=True)
                for c in merged_data:
                    print(c)
                print('---------------------------')
                missing_data = merged_data[merged_data['_merge'] == 'left_only'].drop(columns=['_merge'])
                for c in missing_data:
                    print(c)
                print('---------------------------')
                unique_missing_data = missing_data.drop_duplicates(subset=col)

                # Убираем все строки с нулевыми значениями
                unique_missing_data = unique_missing_data.dropna()
                for c in unique_missing_data:
                    print(c)
                print('---------------------------')
                if not unique_missing_data.empty:
                    # Добавляем в столбец categoryName первое слово из строки typeName (для каждой строки typeName)
                    #if table == 'UniqueTypeNames':
                    #    unique_missing_data['categoryName'] = unique_missing_data['typeName'].apply(lambda x: x.split(' ')[0])

                    #self.logger.info(f'Было добавлено {len(unique_missing_data)} уникальных(-е) строк(-и)')
                    # Добавляем в таблицы наименования новых организаций и типов устройств 
                    unique_missing_data.to_sql(name=table, con=self.__engine, if_exists='append', index=False)

            # table_columns = {'UniquePoveritelOrgs' : ['poveritelOrg'], 'UniqueTypeNames' : ['typeName', 'typeName_tsvector'],
            #                   'UniqueRegisterNumbers' : ['registerNumber'], 'UniqueTypes' : ['type', 'type_tsvector'],
            #                     'UniqueModifications' : ['modification', 'modification_tsvector']}
            
            # for table, cols in table_columns.items():
            #     for col in cols:
            #         old = pd.read_sql_table(table, self.__engine)[col].to_frame()
            #         new = chunk[col].to_frame()

            #         # Определяем отсутствующие строки
            #         merged_data = new.merge(old, on=col, how='left', indicator=True)
            #         missing_data = merged_data[merged_data['_merge'] == 'left_only'].drop(columns=['_merge'])
            #         unique_missing_data = missing_data.drop_duplicates(subset=col)

            #         # Убираем все строки с нулевыми значениями
            #         unique_missing_data = unique_missing_data.dropna()
                    
            #         if not unique_missing_data.empty:
            #             #self.logger.info(f'Было добавлено {len(unique_missing_data)} уникальных(-е) строк(-и)')
            #             # Добавляем в таблицы наименования новых организаций и типов устройств 
            #             unique_missing_data.to_sql(name=table, con=self.__engine, if_exists='append', index=False)



            # Типы данных для временной таблицы
            dtype2 = {
            'serialNumber': VARCHAR(256),
            'svidetelstvoNumber': VARCHAR(256),
            'registerNumber': VARCHAR(16),
            'typeName': VARCHAR(512),
            'type': VARCHAR(4096),
            'modification': VARCHAR(2048),
            'poveritelOrg': VARCHAR(256), # ???????????????
            'poverkaDate': Date,
            'konecDate': Date,
            'vri_id': BigInteger,
            'isPrigodno': Boolean,
            'year': SmallInteger
            }

            chunk.to_sql('temp_table', self.__engine, if_exists='replace', index=False, dtype=dtype2)
            metadata = MetaData()
            metadata.reflect(bind=self.__engine, only=['temp_table'])

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
                WHERE "poverkaDate" IS NOT NULL AND EXTRACT(YEAR FROM "poverkaDate") > 2018;
                """)

        self.__session.execute(stmt)
        self.__session.commit()

        #self.logger.info("Файл добавлен в БД")

        
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

    def extract_and_add_to_db_old_files(self, rootPath):
        """ Разархивирует все вложенные архивы, добавляет в БД и удаляет все вложенные в них файлы"""
        for z, dirs, files in os.walk(rootPath):
            # Проверяем содержит ли папка файлы
            if len(files) > 0:
                for filename in files:
                    fileSpec = os.path.join(z, filename)
                    if filename[-4:] == '.zip':
                        if 'snapshot' in fileSpec.split('\\')[-1]:
                            self.__unzip_file(os.path.join(z, filename), self.rootPath)
                            self.extract_and_add_to_db_old_files(self.rootPath)
                        self.__unzip_file(fileSpec, z)
                        self.__add_file_to_db(fileSpec[:-4])
                        self.logger.info(f'Файл {filename[:-4]} добавлен в БД')
                        os.remove(os.path.join(z, filename[:-4]))
                    elif filename[-4:] == '.csv':
                        self.__add_file_to_db(fileSpec)
                        self.logger.info(f'Файл {filename} добавлен в БД')
                        os.remove(fileSpec)
                    self.extract_and_add_to_db_old_files(z)
            else:
                for dir in dirs:
                    if (self.is_folder_empty(os.path.join(z, dir)) == True):
                        os.rmdir(os.path.join(z, dir))
                        self.extract_and_add_to_db_old_files(rootPath)

                

    def __add_old_data(self, responseData):
        '''Добавляет в БД все вложенные в архив файлы и удаляет'''

        # Запрос для получения содержимого файла
        newFileName = responseData['snapshots'][-1]['relativeUri']
        correctFileHashSum = responseData['snapshots'][-1]['md5sum']

        startAbsUrl = os.path.join(os.path.abspath(newFileName)).replace(newFileName, '')
        downloadedZipPath = startAbsUrl + newFileName

        # Если файл скачен без ошибок и пройдена проверка целостности
        if self.download_file_with_retry(self.__startUrl + newFileName, downloadedZipPath) and self.calculate_md5(downloadedZipPath) == correctFileHashSum:
            self.logger.info(f'Файл {newFileName} был скачан успешно')
            downloadedFolderPath = downloadedZipPath[:-4]

            self.__unzip_file(downloadedZipPath, downloadedFolderPath)
            
            self.rootPath = downloadedFolderPath
            self.extract_and_add_to_db_old_files(self.rootPath)
            os.rmdir(self.rootPath)
        else:
            self.logger.info(f'Файл {newFileName} не был скачан')

        # self.rootPath = 'C:\\Users\\LIKORIS001\\Desktop\\NewCreateScript\\poverki.snapshot.20240801.csv'
        # self.extract_and_add_to_db_old_files(self.rootPath)


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
                #os.rmdir(filePath)
                countFiles += 1
        self.__session.close()
        self.logger.info(f"Количество добавленных файлов = {countFiles}")

    def Main(self):

        #downloadOldData = int(input('Если хотите скачать даные за прошлые месяца, введите 1, иначе 0: '))
        downloadOldData = 2
        #for downloadOldData in range():
        response = requests.get(self.__startUrl + 'manifest.json')

        self.logger.info(f"Статус код ответа от Аршин: {response.status_code}")

        # Проверка успешности запроса
        if response.status_code == 200:

            # Преобразование содержимого в JSON
            data = response.json()
            if downloadOldData == 1:
                self.__add_old_data(data)
            else:
                self.__add_new_data(data)

ekz = ArshinDataDownloader()
ekz.Main()