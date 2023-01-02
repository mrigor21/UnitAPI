# Специальные действия для управленческого учета

from api_modules import get_df_from_db, get_table_from_db
import pandas as pd


def delete_pack_with_payment_requests(session, tables, args):
    pr = tables['payment_requests']
    packs = tables['payment_requests_packs']
    try:
        session.query(pr).filter(pr.c['payment_requests_packs_id'] == args['pack_id']).update({'payment_requests_packs_id': None})
        session.query(packs).filter(packs.c['id'] == args['pack_id']).delete()
        session.commit()
        return '', 204
    except Exception as error:
        session.rollback()
        response = make_response(jsonify(
            {'error': str(error)}
        ), 403)
        return response


def create_pack_with_payment_requests(session, tables, args):
    pr = tables['payment_requests']
    packs = tables['payment_requests_packs']
    try:
        ans = session.execute(packs.insert({'date': args['date'], 'number': args['number']}), )
        last_id = session.query(packs).order_by(packs.c['id'].desc()).first()[0]
        session.query(pr).filter(pr.c['id'].in_(args['pr_ids'])).update({'payment_requests_packs_id': last_id})
        session.commit()
        return make_response(jsonify({'created_pack_id': last_id}), 200)
    except Exception as error:
        session.rollback()
        response = make_response(jsonify(
            {'error': str(error)}
        ), 403)
        return response


def set_payment_requests_into_pack(session, tables, args):
    pr = tables['payment_requests']
    try:
        session.query(pr).filter(pr.c['id'].in_(args['pr_ids'])).update({'payment_requests_packs_id': args['pack_id']})
        session.commit()
        return '', 204
    except Exception as error:
        session.rollback()
        response = make_response(jsonify(
            {'error': str(error)}
        ), 403)
        return response


def decline_payment_requests(session, tables, args):
    pr = tables['payment_requests']
    try:
        session.query(pr).filter(pr.c['id'].in_(args['pr_ids'])).update({'approved_by_' + args['decline_by']: 0})
        session.commit()
        return '', 204
    except Exception as error:
        session.rollback()
        response = make_response(jsonify(
            {'error': str(error)}
        ), 403)
        return response

    
def approve_payment_requests(session, tables, args):
    pr = tables['payment_requests']
    try:
        session.query(pr).filter(pr.c['id'].in_(args['pr_ids'])).update({'approved_by_' + args['approve_by']: 1})
        session.commit()
        return '', 204
    except Exception as error:
        session.rollback()
        response = make_response(jsonify(
            {'error': str(error)}
        ), 403)
        return response


    