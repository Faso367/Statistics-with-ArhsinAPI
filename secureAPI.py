#from flask import Flask, request, jsonify
from datetime import datetime
from sqlalchemy import Column, Integer, BigInteger, VARCHAR, Boolean, SmallInteger, Date, create_engine, and_, or_,desc, select, FromGrouping, alias
from sqlalchemy.schema import ForeignKey
from sqlalchemy.orm import sessionmaker, declarative_base, class_mapper, aliased
import logging, os
from flask_talisman import Talisman
from marshmallow import Schema, fields, validates, ValidationError, validate, error_store
from flask import Flask, request, jsonify, make_response, render_template, session, flash
import jwt
from datetime import datetime, timedelta
from functools import wraps
import bleach
import psycopg2
import os
from dotenv import load_dotenv
from flask_cors import CORS
from sqlalchemy import func
current_year =  datetime.now().year


# dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
# if os.path.exists(dotenv_path):
#     load_dotenv(dotenv_path)

#load_dotenv()

app = Flask(__name__)
# app.config['SECRET_KEY'] = 'KEEP_IT_A_SECRET'
# app.config['CLIENT_KEY'] = '123'

CORS(app)

# SECRET_KEY = os.getenv("SECRET_KEY")
# CLIENT_KEY = os.getenv("CLIENT_KEY")
# print(SECRET_KEY)
# print(CLIENT_KEY)

# Включаем принудительное использование HTTPS
#app.config['SESSION_COOKIE_SECURE'] = True

# Защищаем от атак CSP.
# Подробнее на https://superset-bi.ru/superset-3-talisman-security-considerations-csp-requirements/#%D0%9A%D0%BE%D0%BD%D1%84%D0%B8%D0%B3%D1%83%D1%80%D0%B0%D1%86%D0%B8%D1%8F_Talisman_%D0%BF%D0%BE_%D1%83%D0%BC%D0%BE%D0%BB%D1%87%D0%B0%D0%BD%D0%B8%D1%8E

# Настройка политики CSP
# csp = {
#     'default-src': "'none'",  # Запрещает выполнение любых скриптов по умолчанию
#     'script-src': "'none'",   # Запрещает выполнение JavaScript !!!!!!!!!!!!!!!!!!!
#     'style-src': "'none'",    # Запрещает загрузку стилей !!!!!!!!!!!!!!!!!!!!!
#     'img-src': "'none'",      # Запрещает загрузку изображений
#     'font-src': "'self'",     # Разрешает шрифты только с текущего домена
# }


#talisman = Talisman(app)

#app.config['DEBUG'] = True
#app.config['ENV'] = 'development'
engine = create_engine('postgresql://postgres:password@localhost:5432/Arshindb')
Session = sessionmaker(bind=engine)
session = Session()

Base = declarative_base()

# ----------------------- Модели базы данных

# УНИКАЛЬНЫЕ ТАБЛИЦЫ --------------------
class UniquePoveritelOrgs(Base):
	__tablename__ = 'UniquePoveritelOrgs'
	id = Column(BigInteger(), primary_key=True)
	poveritelOrg = Column(VARCHAR(256))

class UniqueTypeNames(Base):
    __tablename__ = 'UniqueTypeNames'
    id = Column(BigInteger(), primary_key=True)
    typeName = Column(VARCHAR(512))

class UniqueRegisterNumbers(Base):
	__tablename__ = 'UniqueRegisterNumbers'
	id = Column(BigInteger(), primary_key=True)
	registerNumber = Column(VARCHAR(16))

class UniqueTypes(Base):
    __tablename__ = 'UniqueTypes'
    id = Column(BigInteger(), primary_key=True)
    type = Column(VARCHAR(4096))
     
class UniqueModifications(Base):
    __tablename__ = 'UniqueModifications'
    id = Column(BigInteger(), primary_key=True)
    modification = Column(VARCHAR(2048))
    # modification_tsvector = Column(TSVECTOR())
# --------------------------------------

# СТАТИСТИКА ---------------------------
class ModificationStatistics(Base):
    __tablename__ = 'ModificationStatistics'
    id = Column(BigInteger(), primary_key=True)
    modification = Column(VARCHAR(2048))


class TypeStatistics(Base):
    __tablename__ = 'TypeStatistics'
    id = Column(BigInteger(), primary_key=True)
    type = Column(VARCHAR(4096))


class RegisterNumberStatistics(Base):
	__tablename__ = 'RegisterNumberStatistics'
	id = Column(BigInteger(), primary_key=True)
	registerNumber = Column(VARCHAR(16))


class TypeNameStatistics(Base):
    __tablename__ = 'TypeNameStatistics'
    id = Column(BigInteger(), primary_key=True)
    typeName = Column(VARCHAR(512))

# -------------------------------



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


# Будем создавать партиции с такими полями
def create_base_attributes():
    return {
    'id': Column(BigInteger(), primary_key=True),
    'serialNumber': Column(VARCHAR(256)),
    'svidetelstvoNumber': Column(VARCHAR(256)),
    'poverkaDate': Column(Date()),
    'konecDate': Column(Date()),
    'vri_id': Column(BigInteger()),
    'isPrigodno': Column(Boolean()),
    'poveritelOrgId': Column(Integer(), ForeignKey('UniquePoveritelOrgs.id')),
    'typeNameId': Column(Integer(), ForeignKey('UniqueTypeNames.id')),
    'registerNumberId' : Column(Integer(), ForeignKey('UniqueRegisterNumbers.id')),
    'typeId' : Column(Integer(), ForeignKey('UniqueTypes.id')),
    'modificationId' : Column(Integer(), ForeignKey('UniqueModifications.id')),
    'year': Column(SmallInteger())
    }

# Динамическое создание классов-моделей для партиций
for year in range(2019, current_year + 1):
    class_name = f'EquipmentInfo_{year}'
    tablename = f'EquipmentInfo_{year}'
    # Создаём словарь для полей класса. Так как tablename разный, то мы добавляли сначала его
    attributes = {'__tablename__': tablename}
    # Преобразуем остальные поля в пары и добавляем их
    attributes.update(create_base_attributes())
    # Создаём классы с указанным именем, входным параметром и полями
    globals()[class_name] = type(class_name, (Base,), attributes)



# Настройка логгера
logger = logging.getLogger('arshinAPIlogger')
logger.setLevel(logging.ERROR)  # Установите уровень логирования на DEBUG

# Определение пути к файлу логирования
script_directory = os.path.dirname(os.path.abspath(__file__))
log_file_path = os.path.join(script_directory, 'arshinAPI.log')

# Создание обработчика для записи в файл
file_handler = logging.FileHandler(log_file_path)
file_handler.setLevel(logging.ERROR)

# Установка формата для обработчиков
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
file_handler.setFormatter(formatter)

# Добавление обработчиков к логгеру
logger.addHandler(file_handler)

def token_required(func):
    '''Проводит аутентификацию на основе JWT токена'''
    @wraps(func)
    def decorated(*args, **kwargs):
        # Получаем токен из HTTP заголовка
        token = request.headers.get('Authorization')
        # Если токен не найден
        if not token:
            return jsonify({'message': 'Токен был утерян или время его существования закончилось'}), 401
        try:
            # Декодируем токен
            data = jwt.decode(token.split(" ")[1], SECRET_KEY, algorithms=["HS256"])
        except Exception as e:
            logger.error(f'Ошибка: {e}')
            return jsonify({'message': 'Неверный токен', 'error': str(e)}), 403

        return func(*args, **kwargs)
    return decorated


#@app.route('/login', methods=['POST'])
#def login():
    '''Отвечает за получение пользователем JWT токена по его ключу'''
    auth = request.form
    # Если передан параметр key и он принадлежит конкретному пользователю
    if auth.get('key') == CLIENT_KEY:
        # Генерируем токен, он существует 1 день
        token = jwt.encode({'user': auth.get('username'), 'exp': datetime.utcnow() + timedelta(days=1)}, SECRET_KEY, algorithm='HS256')
        # Возвращаем пользователю токен
        return jsonify({'token': token})
    return make_response('Ваш ключ недействителен', 403, {'WWW-Authenticate': 'Basic realm="Login required!"'})


impreciseSearchParams = ['year', 'rows', 'start', 'sort']
preciseSearchParams = ['poveritelOrg', 'registerNumber', 'typeName', 'serialNumber', 'svidetelstvoNumber', 'poverkaDate', 'konecDate', 'isPrigodno']
correctParams = ['vri_id', 'poveritelOrg', 'registerNumber', 'serialNumber', 'svidetelstvoNumber',
                 'poverkaDate', 'konecDate', 'typeName', 'isPrigodno',
                 'year', 'sort', 'start', 'rows', 'search']


class VriParamsSchema(Schema):
    sort = fields.Str()
    vri_id = fields.Int()
    year = fields.Int(validate=validate.Range(min = 2019, max = current_year, error = f'Параметр year может принимать значения от 2019 до {current_year}'))
    rows = fields.Int(validate=validate.Range(min = 1, max = 100, error = 'Параметр rows может принимать значения от 1 до 100'))
    start = fields.Int(validate=validate.Range(min = 0, max = 99999, error = 'Параметр start может принимать значения от 0 до 99999'))
    isPrigodno = fields.Str(validate=validate.OneOf(choices=['true', 'false'], error = 'Параметр isPrigodno может принимать значение true или false'))
    svidetelstvoNumber = fields.Str()
    registerNumber = fields.Str()
    serialNumber = fields.Str()
    poverkaDate = fields.Str() # !!!!!!!!!!!
    konecDate = fields.Str() # !!!!!!!!!!
    poveritelOrg = fields.Str()
    typeName = fields.Str()
    type = fields.Str()
    modification = fields.Str()

    @validates('sort')
    def validate_sort(self, value):
        value = value.replace('%20', ' ').replace('+', ' ')
        parts = value.split(' ')

        if len(parts) != 2:
            raise ValidationError("Invalid format for sort parameter.")
        else:
            if parts[-1] not in ['asc', 'desc']:
                raise ValidationError("Order must be 'asc' or 'desc'.")


class StatisticsSchema(Schema):
    typeName = fields.Str()
    type = fields.Str()
    modification = fields.Str()
    typeName = fields.Str()
    year = fields.Int(validate=validate.Range(min = 2019, max = current_year, error = f'Параметр year может принимать значения от 2019 до {current_year}'))

def sanitize_input(inputDict):
    '''Очищает строку от вредоносного кода'''
    res = dict()
    for k, v in inputDict.items():
        # Добавляем форматированую строку
        res[k] = bleach.clean(v)
    return res

def validation(paramsAndValues):
    '''Валидирует параметры и их значения'''

    params = paramsAndValues.keys()
    invalidParams = {key for key in params if key not in correctParams}

    if len(invalidParams) > 0:
        raise ValidationError(dict=invalidParams, message="Были найдены некорректные названия параметров")

    # Очищаем значения от потенциально опасных скриптов
    clearedDict = sanitize_input(paramsAndValues)
    # Валидируем значения параметров
    schema.load(clearedDict)       

    if len(set(paramsAndValues)) != len(paramsAndValues):
        raise ValidationError(dict=invalidParams, message="Некоторые параметры повторяются, используйте & для перечисления значений")

    # Если задан параметр search и ещё один из четких параметров
    elif len([key for key in params if key == 'search' or key in preciseSearchParams]) > 1:
        raise ValidationError(list=[key for key in params if key == 'search' or key in preciseSearchParams], message="Нельзя использовать поиск по одному параметру и по всем одновременно")
    
    else:
        return True

correctStatisticsParams = ['type', 'modification', 'typeName', 'year', 'registerNumber']

@app.route('/imreciseSearch', methods=['POST', 'GET'])
def imreciseSearch():
    paramAndValue = request.json
    #validParams = dict()

    #return jsonify({'typeName': ['HELLO', 'BYE']})

    result = SearchFullParamValue(**paramAndValue)
    response = jsonify(result) 
    response.headers.add('Access-Control-Allow-Origin', '*')
    return response


def SearchFullParamValue(**kwargs):


    tables_mainCols = {'UniqueTypeNames' : 'typeName', 'UniqueTypes' : 'type',
                    'UniqueModifications' : 'modification', 'UniqueRegisterNumbers' : 'registerNumber'} 


    uniqueColumnName = [key for key in kwargs.keys()][0]

    for tableName, colName in tables_mainCols.items():
        if uniqueColumnName == colName:
            uniqueTable = globals()[tableName]
            uniqueColumn = getattr(uniqueTable, uniqueColumnName)
            searchVal = [val for val in kwargs.values()][0]
            query = session.query(uniqueColumn).filter(uniqueColumn.ilike(f'%{searchVal}%'))

            #print(str(query))
            res = query.limit(10)
            # for r in res:
            #     print(r[0])
            result = []
            # Преобразуем данные в удобочитаемый формат
            for valArr in res:
                result.append(valArr[0])
            #result = [queryToRow(query) for query in res]
            return {uniqueColumnName: result}
            #return session.query(uniqueColumn).filter(uniqueColumn == kwargs.values()[0])


#statisticsSchema = StatisticsSchema()
#@app.route('/statistics', methods=['GET'])
@app.route('/statistics', methods=['POST', 'GET'])
def statistics():
    #print('\nHello\n')

    paramsAndValues = request.json
    print(paramsAndValues)
    validParams = dict()
    # Для НЕСКОЛЬКИХ значений
    # for k, v in paramsAndValues.items():
    #     if v != '':
    #         validParams[k] = [v]


    # Для НЕСКОЛЬКИХ значений
    for k, v in paramsAndValues.items():
        if v != '':
            validParams[k] = v

    print(validParams)

    # Запрос к БД
    result = SelectStatisticsForOneValue(**validParams)
    response = jsonify(result) 
    response.headers.add('Access-Control-Allow-Origin', '*')
    return response

    # return jsonify([{'typeName': '111', 'type': '222', 'registerNumber': '333', 'modification': '444', 'year': '2024'},
    #                 {'typeName': '555', 'type': '666', 'registerNumber': '777', 'modification': '888', 'year': '2024'},
    #                 {'typeName': 'aaa', 'type': 'bbb', 'registerNumber': 'ccc', 'modification': 'ddd', 'year': '2024'}])


#SELECT "UniqueTypes"."type", count(*), "EquipmentInfoPartitioned"."year" FROM "UniqueTypes"
#JOIN "EquipmentInfoPartitioned" ON "UniqueTypes"."id" = "EquipmentInfoPartitioned"."typeId"
#GROUP BY "UniqueTypes"."type", "EquipmentInfoPartitioned"."year";


def SelectStatisticsForOneValue(**kwargs):

    ANDexpressions = []
    ORexpressions = []
    
    items = dict()
    # Итерируем список кортежей
    for item in kwargs.items():
        key = item[0]
        valList = item[1]
        items[key] = valList 

    tables_mainCols = {'UniqueTypeNames' : 'typeName', 'UniqueTypes' : 'type',
                        'UniqueModifications' : 'modification', 'UniqueRegisterNumbers' : 'registerNumber'} 

    q = session.query(
        func.count(),
        EquipmentInfoPartitioned.year
    )

    # Пробегаемся по полученным параметрам
    joinConditions = {}
    groupByCols = []
    for key, val in items.items():
        if key in correctStatisticsParams:
            # Добавляем столбец в SELECT, инфу для JOIN и WHERE
            for tableName, colName in tables_mainCols.items():
                if key == colName:
                    uniqueTable = globals()[tableName]
                    mainColumn = getattr(uniqueTable, key)
                    ANDexpressions.append(mainColumn == val)
                    joinConditions[tableName] = colName + 'Id'

    # Добавляем JOIN
    for tableName, idcolName in joinConditions.items():
        uniqueTable = globals()[tableName]
        idColumn = getattr(EquipmentInfoPartitioned, idcolName)
        q = q.join(uniqueTable, uniqueTable.id == idColumn)

    # Составляем тело Where условия
    combined_expression1 = and_(*ANDexpressions)
    combined_expression2 = or_(*ORexpressions)
    combined_expression = and_(combined_expression1, combined_expression2)

    # Добавляем WHERE
    q = q.filter(combined_expression)

    q = q.group_by(EquipmentInfoPartitioned.year)

    print(str(q))
    res = q.all()

    colNames = ['count', 'year'] 

    result = []
    for valArr in res:
        result.append(dict(zip(colNames, valArr)))

    return result


def SelectStatisticsForManyValues(**kwargs):
    #print(kwargs)
    #partitionTable = globals()["EquipmentInfo_{0}".format(kwargs['year'][0])]
    #kwargs.__delitem__('year') # Удаляю год, тк я уже выбрал нужную партицию для поиска

    # Создаём условия для WHERE

    def replaceSymbols(elList):
        '''Заменяет одни символы на другие'''
        resList = []
        replace_dict = {'*': '%', '?': ' '}
        translation_table = str.maketrans(replace_dict)
        for el in elList:
            resList.append(el.translate(translation_table))
        return resList

    ANDexpressions = []
    ORexpressions = []
    
    items = dict()
    # Итерируем список кортежей
    for item in kwargs.items():
        key = item[0]
        valList = item[1]
        items[key] = valList 
        # items[key] = replaceSymbols(valList) !!!!!!!!!!!!!!!!

    # keysWithoutJoin = ['serialNumber', 'svidetelstvoNumber', 'poverkaDate', 'konecDate', 'isPrigodno']
    # keysWithJoin = ['poveritelOrg', 'registerNumber', 'typeName']

    #years = ['']


    #SELECT "UniqueTypes"."type", count(*), "EquipmentInfoPartitioned"."year" FROM "UniqueTypes"

    tables_mainCols = {'UniqueTypeNames' : 'typeName', 'UniqueTypes' : 'type',
                        'UniqueModifications' : 'modification', 'UniqueRegisterNumbers' : 'registerNumber'} 

    #eq = aliased(EquipmentInfoPartitioned, name="eq")

    # subq = (
    #     session.query(EquipmentInfoPartitioned.year.func.count())
    #     .scalar_subquery()
    # )
    # print(subq)
    # #q = session.query(A, subq.label('cnt')).select_from(A)

    selectQuery = session.query(
        func.count(),
        EquipmentInfoPartitioned.year
    )

    #query = session.query(EquipmentInfoPartitioned)
    # #db.func.count()
    # query = query.add_column(EquipmentInfoPartitioned.year)
    # #query = None
    # #first = True
    #print(str(query))
    # Пробегаемся по полученным параметрам
    joinConditions = {}
    groupByCols = []
    for key, valueArr in items.items():
        if key in correctStatisticsParams:
            # Добавляем столбец в SELECT, инфу для JOIN и WHERE
            for tableName, colName in tables_mainCols.items():
                if key == colName:
                    for val in valueArr:
                        uniqueTable = globals()[tableName]
                        mainColumn = getattr(uniqueTable, key)
                        ANDexpressions.append(mainColumn == val)
                        groupByCols.append(mainColumn)
                        #if first:
                        #    query = session.query(EquipmentInfoPartitioned)
                        #    first = False
                        #else:
                        #query = query.add_entity(statisticsTable)
                        selectQuery = selectQuery.add_columns(mainColumn)

                        #print(str(selectQuery))
                        #query = query.join(uniqueTable, partitionTableCol == uniqueTableCol)
                    joinConditions[tableName] = colName + 'Id'
        #break

            #query = query.limit(10)
    #print(str(selectQuery))
    query = None
    first = True
    # Добавляем JOIN
    for tableName, idcolName in joinConditions.items():
        uniqueTable = globals()[tableName]
        idColumn = getattr(EquipmentInfoPartitioned, idcolName)
        selectQuery = selectQuery.join(uniqueTable, uniqueTable.id == idColumn)
        #break
    #print(str(selectQuery))

    # selectQuery = selectQuery.group_by(
    #     UniqueTypes.type, 
    #     EquipmentInfoPartitioned.year, 
    #     UniqueTypeNames.typeName,
    #     UniqueModifications.modification)


    # res = selectQuery.group_by(
    #     UniqueTypes.type, 
    #     EquipmentInfoPartitioned.year, 
    #     UniqueRegisterNumbers.registerNumber
    # ).all()

    #print(str(selectQuery))

    # Составляем тело Where условия
    combined_expression1 = and_(*ANDexpressions)
    combined_expression2 = or_(*ORexpressions)
    combined_expression = and_(combined_expression1, combined_expression2)

    # Добавляем WHERE
    selectQuery = selectQuery.filter(combined_expression)

    
    selectQuery = selectQuery.group_by(EquipmentInfoPartitioned.year)
    #res = query.limit(10)
    for groupCol in groupByCols:
        selectQuery = selectQuery.group_by(groupCol)

    print('-----------------------')
    print(str(selectQuery))
    # Получаем результат запроса
    res = selectQuery.all()

    #print(str(query))

    # Получаем результат запроса
    #res = query.limit(10).with_entities(func.count()).scalar()
    #res = query.with_entities(func.count()).scalar()
    #res = res.limit(10)

    colNames = ['count', 'year'] 
    for enterKey in items.keys():
        colNames.append(enterKey)

    result = []
    for valArr in res:
        result.append(dict(zip(colNames, valArr)))

        # for v in valArr:
        #     for key in items.keys():
        #         result.append({key, v})


    # Преобразуем данные в удобочитаемый формат
    #result = [query for query in res]
    return result
    # query = session.query(partitionTable, UniqueTypeNames, UniquePoveritelOrgs, UniqueRegisterNumbers) \
    #     .join(UniqueTypeNames, partitionTable.typeNameId == UniqueTypeNames.id) \
    #     .join(UniquePoveritelOrgs, partitionTable.poveritelOrgId == UniquePoveritelOrgs.id) \
    #     .join(UniqueRegisterNumbers, partitionTable.registerNumberId == UniqueRegisterNumbers.id) \
    #     .filter(combined_expression) 

    # Добавляем сортировку, если такой параметр был задан
    # if 'sort' in kwargs:
    #     col = getattr(partitionTable, kwargs['sort'][0])
    #     # Сортируем по убыванию
    #     if kwargs['sort'][1] == 'desc':
    #         query = query.order_by(desc(col))
    #     # Сортируем по возрастанию
    #     else:
    #         query = query.order_by(col)

    # # Дополняем условия ограничения выборки
    # query = query.limit(items['rows'][0]) \
    #     .offset(items['start'][0])



#def SelectStatistics(**kwargs):
    print(kwargs)
    #partitionTable = globals()["EquipmentInfo_{0}".format(kwargs['year'][0])]
    #kwargs.__delitem__('year') # Удаляю год, тк я уже выбрал нужную партицию для поиска

    # Создаём условия для WHERE

    def replaceSymbols(elList):
        '''Заменяет одни символы на другие'''
        resList = []
        replace_dict = {'*': '%', '?': ' '}
        translation_table = str.maketrans(replace_dict)
        for el in elList:
            resList.append(el.translate(translation_table))
        return resList

    ANDexpressions = []
    ORexpressions = []
    
    items = dict()
    # Итерируем список кортежей
    for item in kwargs.items():
        key = item[0]
        valList = item[1]
        items[key] = valList 
        # items[key] = replaceSymbols(valList) !!!!!!!!!!!!!!!!

    # keysWithoutJoin = ['serialNumber', 'svidetelstvoNumber', 'poverkaDate', 'konecDate', 'isPrigodno']
    # keysWithJoin = ['poveritelOrg', 'registerNumber', 'typeName']

    #years = ['']


    #SELECT "UniqueTypes"."type", count(*), "EquipmentInfoPartitioned"."year" FROM "UniqueTypes"

    tables_mainCols = {'TypeNameStatistics' : 'typeName', 'TypeStatistics' : 'type', 'ModificationStatistics' : 'modification', 'RegisterNumberStatistics' : 'registerNumber'} 

    #eq = aliased(EquipmentInfoPartitioned, name="eq")

    stmt = 'SELECT COUNT(*), "EquipmentInfoPartitioned"."year"'

    # Пробегаемся по полученным параметрам
    for key, valueArr in items.items():
        if key in correctStatisticsParams:
            joinConditions = {}
            # Добавляем столбец в SELECT, инфу для JOIN и WHERE
            for tableName, colName in tables_mainCols.items():
                if key == colName:
                    for val in valueArr:

                        ANDexpressions.append(mainColumn == val)

                        stmt += f', "{tableName}"."{colName}"'

                        print(str(query))
                        joinConditions[tableName] = colName + 'Id'

            #query = query.limit(10)
            # Добавляем JOIN
            for tableName, idcolName in joinConditions.items():
                statisticsTable = globals()[tableName]
                idColumn = getattr(EquipmentInfoPartitioned, idcolName)
                query = query.join(EquipmentInfoPartitioned, statisticsTable.id == idColumn)
            print(str(query))

schema = VriParamsSchema()
@app.route('/vri', methods=['GET'])
#@token_required
def vri():
    '''Вызывается пользователем с заданными параметрами'''

    #try:
    paramsAndValues = request.args.to_dict()

    if validation(paramsAndValues) == True:
        newparamsDict = dict()
        defaultValues = {'year': [current_year], 'rows': [10], 'start': [0]}

        # Добавляем дефолтные пары ключ-значение, если их значения не заданы 
        for key, value in defaultValues.items():       
            if key not in paramsAndValues:
                newparamsDict[key] = value

        # Вытаскиваем данные из списков-значений словаря !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        for key, value in paramsAndValues.items():
            newparamsDict[key] = [to_int_if_possible(value)]

        # Запрос к БД
        result = SelectFromDb(**newparamsDict)
        return jsonify(result)
        
    
    # except ValidationError as err:
    #     return jsonify({"errors": err.messages}), 400

    # except Exception as e:
    #    logger.error(f'Ошибка: {e}')
    #    print(e)
    #    return jsonify(Error = 'Произошла непредвиденная ошибка'), 400


def SelectFromDb(**kwargs):
    '''Корректирует входные значения и даёт SELECT в БД'''
    print(kwargs)
    partitionTable = globals()["EquipmentInfo_{0}".format(kwargs['year'][0])]
    kwargs.__delitem__('year') # Удаляю год, тк я уже выбрал нужную партицию для поиска

    # Создаём условия для WHERE
    ANDexpressions = []
    ORexpressions = []

    def replaceSymbols(elList):
        '''Заменяет одни символы на другие'''
        resList = []
        replace_dict = {'*': '%', '?': ' '}
        translation_table = str.maketrans(replace_dict)
        for el in elList:
            resList.append(el.translate(translation_table))
        return resList

    
    items = dict()
    # Итерируем список кортежей
    for item in kwargs.items():
        key = item[0]
        valList = item[1]
        if key != 'start' and key != 'rows':
            # Заменяем спецсимволы на те, что используются в postgresql 
            items[key] = replaceSymbols(valList)
        elif key != 'start' or key != 'rows':
            items[key] = valList

    keysWithoutJoin = ['serialNumber', 'svidetelstvoNumber', 'poverkaDate', 'konecDate', 'isPrigodno']
    keysWithJoin = ['poveritelOrg', 'registerNumber', 'typeName']

    # Пробегаемся по полученным параметрам
    for key, valueArr in items.items():
        if key in keysWithoutJoin:
            column = getattr(partitionTable, key)
            # Пробегаемся по значениям параметра
            for v in valueArr:
                if '*' in v or ' ' in v:
                    ANDexpressions.append(column == v)
                else:
                    ANDexpressions.append(column.ilike(f"{v}"))


        # Если используется неточный поиск по неопределенным параметрам
        elif key == 'search':
            for v in valueArr:
                # Добавляем поиск только по параметрам, которые доступны для неточного поиска
                ORexpressions.append(UniqueTypeNames.typeName.ilike(f"{v}"))
                ORexpressions.append(UniquePoveritelOrgs.poveritelOrg.ilike(f"{v}"))
                ORexpressions.append(UniqueRegisterNumbers.registerNumber.ilike(f"{v}"))
                serCol = getattr(partitionTable, 'serialNumber')
                ORexpressions.append(serCol.ilike(f"{v}"))
                svidCol = getattr(partitionTable, 'svidetelstvoNumber')
                ORexpressions.append(svidCol.ilike(f"{v}"))


        elif key in keysWithJoin:
            if key == 'typeName':
                ANDexpressions.append(UniqueTypeNames.typeName == kwargs['typeName'][0])

            elif key == 'poveritelOrg':
                ANDexpressions.append(UniquePoveritelOrgs.poveritelOrg == kwargs['poveritelOrg'][0])

            elif key == 'registerNumber':
                ANDexpressions.append(UniqueRegisterNumbers.registerNumber == kwargs['registerNumber'][0])

        elif key in ['rows', 'start', 'sort']:
            continue  # rows и start обработаны ранее, остальные будут обработаны в JOIN и FILTER
        else:
            raise AttributeError(f"Некорректный параметр: {key}")

    # Составляем тело Where условия
    combined_expression1 = and_(*ANDexpressions)
    combined_expression2 = or_(*ORexpressions)
    combined_expression = and_(combined_expression1, combined_expression2)

    query = session.query(partitionTable, UniqueTypeNames, UniquePoveritelOrgs, UniqueRegisterNumbers) \
        .join(UniqueTypeNames, partitionTable.typeNameId == UniqueTypeNames.id) \
        .join(UniquePoveritelOrgs, partitionTable.poveritelOrgId == UniquePoveritelOrgs.id) \
        .join(UniqueRegisterNumbers, partitionTable.registerNumberId == UniqueRegisterNumbers.id) \
        .filter(combined_expression) 

    # Добавляем сортировку, если такой параметр был задан
    if 'sort' in kwargs:
        col = getattr(partitionTable, kwargs['sort'][0])
        # Сортируем по убыванию
        if kwargs['sort'][1] == 'desc':
            query = query.order_by(desc(col))
        # Сортируем по возрастанию
        else:
            query = query.order_by(col)

    # Дополняем условия ограничения выборки
    query = query.limit(items['rows'][0]) \
        .offset(items['start'][0])

    # Получаем результат запроса
    res = query.all()
    # Преобразуем данные в удобочитаемый формат
    result = [queryToRow(query) for query in res]
    return result


def queryToRow(query):
    '''Преобразует полученный объект в словарь'''
    result = {}
    # Объект может содержать несколько строк, пробегаемся по ним
    for item in query:
        result.update(to_dict(item))
    return result


def to_dict(instance):
    '''Преобразует строку таблицы в словарь'''
    if not instance:
        return {}
    # Получаем типы по их названию, при этом исключаем лишние колонки из выборки
    columns = [column.key for column in class_mapper(instance.__class__).columns
               if column.key != 'poveritelOrgId' and column.key != 'poveritelOrg' and column.key != 'typeId' and column.key != 'poveritelOrgId' and column.key != 'registerNumberId' and column.key != 'typeNameId']
    return {column: getattr(instance, column) for column in columns}

def to_int_if_possible(s):
    if s.isdigit():
        return int(s)
    return s

def try_to_int(val):
    try:
        return int(val)
    except ValueError:
        logger.error('Не удалось конвертировать строку в целое число')
        return -1


if __name__ == "__main__":
    app.run(debug=True)