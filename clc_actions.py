# Специальные действия для калькулятора

from api_modules import get_df_from_db, get_table_from_db
import pandas as pd


def make_est_materials_table(eng, session, tables, est_id=None, est=None, ek=None, ek_ids=None, objects_id=None):
    if est_id is None and (est is None or ek is None):
        raise Exception('Если не передан id расчета, нужно передать словарь со свойствами расчета и датафрейм с ЕК')
    if est is None or ek is None:
        est = get_table_from_db(session, tables, 'estimations', {'id': est_id})[0]
        ek = get_df_from_db(eng, session, tables, 'ek', {'estimation_id': est_id}, ['id', 'work_types_id', 'volume', 'clc_id'])

    r_work_types_basic_materials = get_df_from_db(eng, session, tables, 'r_work_types_basic_materials', {'work_types_id': ek.work_types_id})
    r_ek_basic_materials = get_df_from_db(eng, session, tables, 'r_ek_basic_materials', {'ek_id': ek.id})
    r_ek_basic_materials['is_basic'] = True
    r_ek_add_materials = get_df_from_db(eng, session, tables, 'r_ek_add_materials', {'ek_id': ek.id})
    r_ek_add_materials['is_basic'] = False
    df = pd.concat([r_ek_basic_materials, r_ek_add_materials], axis=0)
    materials = get_df_from_db(eng, session, tables, 'materials', {'id': df.materials_id})
    df = df.merge(materials, how='left', left_on='materials_id', right_on='id', suffixes=[None, '_mat'])
    df = df.merge(ek, how='left', left_on='ek_id', right_on='id', suffixes=[None, '_ek'])
    df = df.merge(r_work_types_basic_materials, how='left', left_on=['work_types_id', 'materials_id'], right_on=['work_types_id', 'materials_id'])
    df['volume'].loc[df['is_basic']] = df['consumption_rate'] * df['volume_ek']
    
    # spc = get_df_from_db(eng, session, tables, 'spc', {'id': df.spc_id})
    spc_materials_prices = get_df_from_db(eng, session, tables, 'spc_materials_prices', {'spc_id': df.spc_id.dropna()})
    spc = get_df_from_db(eng, session, tables, 'spc', {'id': df.spc_id.dropna()}, remain_cols=['id', 'print_contractor', 'contracts_id'])
    contracts = get_df_from_db(eng, session, tables, 'contracts', {'id': spc.contracts_id.dropna()}, ['id', 'contractors_id'])
    spc_materials_prices = spc_materials_prices.merge(spc, how='left', left_on='spc_id', right_on='id')
    spc_materials_prices = spc_materials_prices.merge(contracts, how='left', left_on='contracts_id', right_on='id', suffixes=['_spc', '_contract'])
    
    logger.debug(spc_materials_prices)
    
    df = df.merge(spc_materials_prices, how='left', on=['spc_id', 'materials_id'])

    clc_materials_prices = get_df_from_db(eng, session, tables, 'clc_materials_prices', {'clc_id': df.clc_id.dropna()})
    df = df.merge(clc_materials_prices, how='left', on=['clc_id', 'materials_id'])
    # logger.debug(df)
    # raise Exception
    mats = df.materials_id.unique().tolist()
    prices_history = tables['materials_prices_history']
    fields = [prices_history.c[col] for col in ['id', 'materials_id', 'contractors_id', 'price']]
    prices_history = session.query(prices_history).with_entities(*fields).filter(
        and_(
            prices_history.c['materials_id'].in_(mats),
            or_(
                prices_history.c['objects_id'] == est['objects_id'],
                prices_history.c['objects_id'].is_(None)
            )
        )
    )
    prices_history = pd.read_sql(prices_history.statement, eng)
    # Логику цен переделать!
    # logger.debug(prices_history.loc[prices_history.objects_id.isna()])
    prices_history = pd.concat([prices_history, prices_history.loc[prices_history.id.isin()]])
    prices_history.drop_duplicates(subset=['materials_id'], keep='last', inplace=True)

    df = df.merge(prices_history, how='left', on='materials_id', suffixes=['_spc', '_mph'])
    df['contractors_id'] = df['contractors_id_spc'].fillna(df['contractors_id_mph'])
    contractors = get_df_from_db(eng, session, tables, 'contractors', {'id': df.contractors_id}, ['id', 'name'])
    df = df.merge(contractors, how='left', right_on='id', left_on='contractors_id', suffixes=[None, '_contractor'])
    df['price'] = df['price_spc'].fillna(df['price_mph'])
    df['cost'] = df.price * df.volume
    df['overconsumption'] = 1
    return df


def update_eks_clc_id(session, tables, args):
    ek = tables['ek']
    try:
        session.query(ek).filter(ek.c['id'].in_(args['ek_ids'])).update({'clc_id': args['clc_id']})
        session.commit()
        return '', 204
    except Exception as error:
        session.rollback()
        response = make_response(jsonify(
            {'error': str(error)}
        ), 403)
        return response


def update_mats_spc_id(session, tables, args):
    r_ek_basic_materials = tables['r_ek_basic_materials']
    r_ek_add_materials = tables['r_ek_add_materials']
    if args['r_ek_add_mats_ids'] is None and args['r_ek_basic_mats_ids'] is None:
        abort(400, message='At least one basic or additional material must be passed in request')
    try:
        if args['r_ek_basic_mats_ids'] is not None:
            session.query(r_ek_basic_materials).filter(r_ek_basic_materials.c['id'].in_(args['r_ek_basic_mats_ids'])).update({'spc_id': args['spc_id']})
        if args['r_ek_add_mats_ids'] is not None:
            session.query(r_ek_add_materials).filter(r_ek_add_materials.c['id'].in_(args['r_ek_add_mats_ids'])).update({'spc_id': args['spc_id']})
        session.commit()
        return '', 204
    except Exception as error:
        session.rollback()
        response = make_response(jsonify(
            {'error': str(error)}
        ), 403)
        return response


def delete_ek_with_mats(session, tables, ek_ids):
    ek = tables['ek']
    r_ek_basic_mats = tables['r_ek_basic_materials']
    r_ek_add_mats = tables['r_ek_add_materials']
    try:
        session.query(r_ek_add_mats).filter(r_ek_add_mats.c['ek_id'].in_(ek_ids)).delete()
        session.query(r_ek_basic_mats).filter(r_ek_basic_mats.c['ek_id'].in_(ek_ids)).delete()
        session.query(ek).filter(ek.c['id'].in_(ek_ids)).delete()
        session.commit()
        return '', 204
    except Exception as error:
        session.rollback()
        response = make_response(jsonify(
            {'error': str(error)}
        ), 403)
        return response


def delete_clc_with_eks(session, tables, clc_ids):
    clc = tables['clc']
    ek = tables['ek']
    try:
        session.query(ek).filter(ek.c['clc_id'].in_(clc_ids)).update({'clc_id': None})
        session.query(clc).filter(clc.c['id'].in_(clc_ids)).delete()
        session.commit()
        return '', 204
    except Exception as error:
        session.rollback()
        response = make_response(jsonify(
            {'error': str(error)}
        ), 403)
        return response


def delete_spc_with_mats(session, tables, spc_ids):
    spc = tables['spc']
    r_ek_basic_mats = tables['r_ek_basic_materials']
    r_ek_add_mats = tables['r_ek_add_materials']
    try:
        session.query(r_ek_basic_mats).filter(r_ek_basic_mats.c['spc_id'].in_(spc_ids)).update({'spc_id': None})
        session.query(r_ek_add_mats).filter(r_ek_add_mats.c['spc_id'].in_(spc_ids)).update({'spc_id': None})
        session.query(spc).filter(spc.c['id'].in_(spc_ids)).delete()
        session.commit()
        return '', 204
    except Exception as error:
        session.rollback()
        response = make_response(jsonify(
            {'error': str(error)}
        ), 403)
        return response