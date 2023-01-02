from sqlalchemy import create_engine, inspect
from flask_restful import reqparse
from sqlalchemy.ext.automap import automap_base
from loguru import logger
import pandas as pd
import copy


# Функция, проверяющая, что по запросу в БД найдена хоть одна запись,
# иначе ошибка 404. Опционально можно добавить флаг на поднятие ошибки 400
# в случае, если запрос возвращает больше одной строки.
# Это сделано для запросов на редактирование строчек, где все поля, образующие primary key
# являются обязательными и редактироваться должна только одна строка.
# Но в случае ошибки, эта проверка не даст отредактировать несколько строк.
def check_for_empty_table(q, multiple_records_abort=False):
    c = q.count()
    if c == 0:
        abort(404, message='Record is not found')
    elif c > 1 and multiple_records_abort:
        abort(400, message='Multiple records found. Ask developers to check indexes in database and required parameters in API')


# Функция возвращает датафрейм из таблицы в БД.
# Можно опционально передать фильтры и поля, которые нужно оставить в ДФ
def get_df_from_db(eng, session, tables, table_name, filters={}, remain_cols=None):
    t = tables[table_name]
    q = session.query(t)
    if remain_cols is not None:
        fields = [t.c[col] for col in remain_cols]
        q = q.with_entities(*fields)
    filters = [
        t.c[col].in_(
            [v] if not isinstance(v, (list, tuple, set, pd.Series)) else v
        ) for col, v in filters.items()
    ]
    q = q.filter(*filters)
    t = pd.read_sql(q.statement, eng)
    return t


# Функция возвращает список словарей из таблицы в БД.
# Можно опционально передать фильтры и поля, которые нужно оставить в ДФ,
# и флаг на добавления префикса из названия таблицы к названию полей (имя_таблицы_поле)
def get_table_from_db(session, tables, table_name, filters={}, remain_cols=None, add_prefix=False):
    t = tables[table_name]
    columns = t.columns.keys()
    if add_prefix:
        columns = [table_name + '_' + c for c in columns]
        if remain_cols is not None:
            remain_cols = [table_name + '_' + c for c in remain_cols]
    filters = [
        t.c[col].in_(
            [v] if not isinstance(v, (list, tuple, set, pd.Series)) else v
        ) for col, v in filters.items()
    ]
    result = session.query(t).filter(*filters)
    if remain_cols is not None:
        t = [{c: v for c, v in zip(columns, row) if c in remain_cols} for row in result]
    else:
        t = [{c: v for c, v in zip(columns, row)} for row in result]
    return t


# Парсеры объединены в структуру словаря, как creds для доступа в БД
# Если парсер находится по ключу COMMON, например в управленческом учете, это
# значит, что действие, для которого он предназначен может быть выполнено над любой БД
# в этом продукте.
def build_actions_argparsers(creds):
    actions_parsers = copy.deepcopy(creds)
    for product, dbs in creds.items():
        for db in dbs.keys():
            actions_parsers[product][db] = {}
            # Ключ словаря для хранения парсеров, общих для всех БД в рамках одного продукта, например для всех баз данных УУ
            actions_parsers[product]['COMMON'] = {}
    # Ключ словаря для хранения парсеров, общих для всех БД во всех продуктах
    actions_parsers['COMMON'] = {}

    ps = reqparse.RequestParser()
    ps.add_argument(
        'ek_ids', required=True, nullable=False, store_missing=False, type=int, action='append')
    ps.add_argument(
        'clc_id', required=False, nullable=True, store_missing=True, type=int, action='store')
    actions_parsers['clc']['COMMON']['give_clc_id_to_ek'] = ps

    ps = reqparse.RequestParser()
    ps.add_argument(
        'r_ek_basic_mats_ids', required=False, nullable=True, store_missing=True, type=int, action='append')
    ps.add_argument(
        'r_ek_add_mats_ids', required=False, nullable=True, store_missing=True, type=int, action='append')
    ps.add_argument(
        'spc_id', required=False, nullable=True, store_missing=True, type=int, action='store')
    actions_parsers['clc']['COMMON']['give_spc_id_to_material'] = ps

    ps = reqparse.RequestParser()
    ps.add_argument(
        'email', required=True, nullable=False, store_missing=False, type=str)
    ps.add_argument(
        'password', required=True, nullable=False, store_missing=False, type=str)
    actions_parsers['auth']['COMMON']['check_pwd'] = ps

    # Специальные удаления ЕК, спецификаций и калькуляций написаны так, чтобы можно было передать несколько айди сущностей для удаления списком
    ps = reqparse.RequestParser()
    ps.add_argument(
        'ek_ids', required=True, nullable=False, store_missing=False, type=int, action='append')
    actions_parsers['clc']['COMMON']['delete_ek_with_mats'] = ps

    ps = reqparse.RequestParser()
    ps.add_argument(
        'clc_ids', required=True, nullable=False, store_missing=False, type=int, action='append')
    actions_parsers['clc']['COMMON']['delete_clc_with_eks'] = ps

    ps = reqparse.RequestParser()
    ps.add_argument(
        'spc_ids', required=True, nullable=False, store_missing=False, type=int, action='append')
    actions_parsers['clc']['COMMON']['delete_spc_with_mats'] = ps

    ps = reqparse.RequestParser()
    ps.add_argument(
        'est_id', required=True, nullable=False, store_missing=False, type=int)
    actions_parsers['clc']['production']['format_estimation_json'] = ps

    ps = reqparse.RequestParser()
    ps.add_argument(
        'clc_id', required=True, nullable=False, store_missing=False, type=int)
    actions_parsers['clc']['production']['format_clc_json'] = ps

    ps = reqparse.RequestParser()
    ps.add_argument(
        'spc_id', required=True, nullable=False, store_missing=False, type=int)
    actions_parsers['clc']['production']['format_spc_json'] = ps

    ps = reqparse.RequestParser()
    ps.add_argument(
        'query', required=True, nullable=False, store_missing=False, type=str, action='append')
    actions_parsers['COMMON']['sql'] = ps

    ps = reqparse.RequestParser()
    ps.add_argument(
        'pr_ids', required=True, nullable=False, store_missing=False, type=int, action='append')
    ps.add_argument(
        'approve_by', required=True, nullable=False, store_missing=False,
        choices=('finmanager', 'director', 'bank'))
    actions_parsers['uu']['COMMON']['approve_payment_requests'] = ps

    ps = reqparse.RequestParser()
    ps.add_argument(
        'pr_ids', required=True, nullable=False, store_missing=False, type=int, action='append')
    ps.add_argument(
        'decline_by', required=True, nullable=False, store_missing=False,
        choices=('finmanager'))
    actions_parsers['uu']['COMMON']['decline_payment_requests'] = ps

    ps = reqparse.RequestParser()
    ps.add_argument(
        'pr_ids', required=True, nullable=False, store_missing=False, type=int, action='append')
    ps.add_argument(
        'pack_id', required=False, nullable=True, store_missing=True, type=int)
    actions_parsers['uu']['COMMON']['set_payment_requests_into_pack'] = ps

    ps = reqparse.RequestParser()
    ps.add_argument(
        'pr_ids', required=True, nullable=False, store_missing=False, type=int, action='append')
    ps.add_argument(
        'number', required=True, nullable=False, store_missing=False, type=int)
    ps.add_argument(
        'date', required=True, nullable=False, store_missing=False)
    actions_parsers['uu']['COMMON']['create_pack_with_payment_requests'] = ps

    ps = reqparse.RequestParser()
    ps.add_argument(
        'pack_id', required=True, nullable=False, store_missing=False, type=int)
    actions_parsers['uu']['COMMON']['delete_pack_with_payment_requests'] = ps
    
    return actions_parsers


# Парсеры для специальных составных таблиц
def build_spec_argparsers(creds):
    spec_parsers = copy.deepcopy(creds)
    for product, dbs in creds.items():
        for db in dbs.keys():
            spec_parsers[product][db] = {}
            # Ключ словаря для хранения парсеров, общих для всех БД в рамках одного продукта, например для всех баз данных УУ
            spec_parsers[product]['COMMON'] = {}
    # Все материалы по расчету
    ps = reqparse.RequestParser()
    ps.add_argument(
        'est_id', required=True, nullable=False, store_missing=False, type=int)
    spec_parsers['clc']['COMMON']['est_mats'] = ps

    return spec_parsers


# Функция создает объекты таблиц, объекты движков и объекты инспекторов для каждой БД
def create_db_resources_v3(creds):
    engines = copy.deepcopy(creds)
    tables = copy.deepcopy(creds)
    inspectors = copy.deepcopy(creds)
    for product, dbs in creds.items():
        # ___________________
        # if product not in ['clc']:
        #     continue
        # ___________________
        for db, data in dbs.items():
            # if product != 'clc' or db != 'production':
            #     continue
            conn_str = "mysql+pymysql://{username}:{password}@{hostname}/{dbname}".format(**data)
            eng = create_engine(conn_str, echo=False)
            logger.debug(eng.url.database)
            Base = automap_base()
            Base.prepare(eng, reflect=True)
            engines[product][db] = eng
            tables[product][db] = Base.metadata.tables
            inspectors[product][db] = inspect(eng)
    return engines, tables, inspectors


# Парсеры для работы с таблицами в БД
def build_init_tables_argparsers(engines, tables, creds):
    tables_fields_argparsers = copy.deepcopy(creds)
    for product, dbs in engines.items():
        # ___________________
        # if product not in ['clc']:
        #     continue
        # ___________________
        for db, eng in dbs.items():
            # if product != 'clc' or db != 'production':
            #     continue
            tables_fields_argparsers[product][db] = {}
            # Дефолтные парсеры для ообращения непосредственно к таблицам
            inspector = inspect(eng)
            for table_name in inspector.get_table_names(schema=eng.url.database):
                table_parsers = {k: reqparse.RequestParser() for k in [
                    'PUT', # Для добавления обязательны поля, которые не могут быть пустыми и не имеют автозаполнения либо значения по умолчанию
                    'DELETE', # Для удаления обязательны те поля, которые образуют уникальный ключ.
                    'GET', # Для фильтрации все поля опциональны
                    'POST'  # Для обновления обязательны те поля, которые образуют уникальный ключ, остальные опциональны
                ]}
                table = tables[product][db][table_name]
                primary_keys = []
                for column in inspect(table).primary_key:
                    # column.type - тип данных в колонке
                    primary_keys.append(column.name)
                    table_parsers['DELETE'].add_argument(column.name, required=True, nullable=False, store_missing=False)
                    table_parsers['PUT'].add_argument(column.name, required=True, nullable=False, store_missing=False)
                for column in inspector.get_columns(table_name, schema=eng.url.database):
                    # FIXME
                    # Добавить проверку по типу данных ОБЯЗАТЕЛЬНО!
                    table_parsers['POST'].add_argument(
                        column['name'],
                        # type = # Доделать сопоставлением типов данных возвращаемых схемой SQL с питоновыми типами
                        required=not column["nullable"] and \
                            ((not column["autoincrement"]) if "autoincrement" in column else True) and \
                                column['default'] is None,
                        nullable=True,
                        store_missing=False
                        # default=column['default'] # бесполезная штука, потому что все равно тип данных не тот, конвертировать не за чем если БД сразу нужное значение вставит
                    )
                    table_parsers['GET'].add_argument(column['name'], required=False, nullable=True, store_missing=False)
                    if column['name'] not in primary_keys:
                        table_parsers['PUT'].add_argument(
                            column['name'],
                            required=False, # Если не передан, то возвращает ошибку
                            nullable=True, # Если передано None, то возвращает ошибку
                            store_missing=False # Если False, то парсит только переданные значения, остальные нет.
                            # Если True (по дефолту), то все непереданные аргументы парсятся со значениями None
                            )
                tables_fields_argparsers[product][db][table_name] = table_parsers
    return tables_fields_argparsers
