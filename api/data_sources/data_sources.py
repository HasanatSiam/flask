from datetime import datetime
from flask import request, jsonify, make_response 
from flask_jwt_extended import jwt_required, get_jwt_identity
from sqlalchemy import or_
from executors.extensions import db
from executors.models import (
    DefDataSource
)

from . import data_sources_bp


#Def_Data_Sources
@data_sources_bp.route('/def_data_sources', methods=['POST'])
@jwt_required()
def create_def_data_source():
    try:
        new_datasource = DefDataSource(
            datasource_name=request.json.get('datasource_name'),
            description=request.json.get('description'),
            application_type=request.json.get('application_type'),
            application_type_version=request.json.get('application_type_version'),
            last_access_synchronization_date=request.json.get('last_access_synchronization_date'),
            last_access_synchronization_status=request.json.get('last_access_synchronization_status'),
            last_transaction_synchronization_date=request.json.get('last_transaction_synchronization_date'),
            last_transaction_synchronization_status=request.json.get('last_transaction_synchronization_status'),
            default_datasource=request.json.get('default_datasource'),
            created_by=get_jwt_identity(),
            last_updated_by=get_jwt_identity(),
            creation_date=datetime.utcnow(),
            last_update_date=datetime.utcnow()
        )
        db.session.add(new_datasource)
        db.session.commit()
        return make_response(jsonify({'message': 'Added successfully'}), 201)
    except Exception as e:
        return make_response(jsonify({'message': 'Error creating data source', 'error': str(e)}), 500)


@data_sources_bp.route('/def_data_sources', methods=['GET'])
@jwt_required()
def get_def_data_sources():
    try:
        def_data_source_id = request.args.get('def_data_source_id', type=int)
        datasource_name = request.args.get('datasource_name', type=str)
        page = request.args.get('page', type=int)
        limit = request.args.get('limit', type=int)

        # Case 1: Get by ID
        if def_data_source_id:
            ds = DefDataSource.query.filter_by(def_data_source_id=def_data_source_id).first()
            if ds:
                return make_response(jsonify({'result': ds.json()}), 200)
            return make_response(jsonify({'message': 'Data source not found'}), 404)

        query = DefDataSource.query

        # Case 2: Search
        if datasource_name:
            search_query = datasource_name.strip()
            search_underscore = search_query.replace(' ', '_')
            search_space = search_query.replace('_', ' ')
            query = query.filter(
                or_(
                    DefDataSource.datasource_name.ilike(f'%{search_query}%'),
                    DefDataSource.datasource_name.ilike(f'%{search_underscore}%'),
                    DefDataSource.datasource_name.ilike(f'%{search_space}%')
                )
            )

        # Case 3: Pagination (Search or just List)
        if page and limit:
            paginated = query.order_by(DefDataSource.def_data_source_id.desc()).paginate(page=page, per_page=limit, error_out=False)
            return make_response(jsonify({
                "result": [ds.json() for ds in paginated.items],
                "total": paginated.total,
                "pages": 1 if paginated.total == 0 else paginated.pages,
                "page": paginated.page
            }), 200)

        # Case 4: Get All (if no ID and no pagination)
        data_sources = query.order_by(DefDataSource.def_data_source_id.desc()).all()
        return make_response(jsonify({'result': [ds.json() for ds in data_sources]}), 200)

    except Exception as e:
        return make_response(jsonify({'message': 'Error fetching data sources', 'error': str(e)}), 500)

@data_sources_bp.route('/def_data_sources', methods=['PUT'])
@jwt_required()
def update_def_data_source():
    try:
        def_data_source_id = request.args.get('def_data_source_id', type=int)
        if not def_data_source_id:
            return make_response(jsonify({'message': 'def_data_source_id is required'}), 400)

        ds = DefDataSource.query.filter_by(def_data_source_id=def_data_source_id).first()
        if ds:
            ds.datasource_name = request.json.get('datasource_name', ds.datasource_name)
            ds.description = request.json.get('description', ds.description)
            ds.application_type = request.json.get('application_type', ds.application_type)
            ds.application_type_version = request.json.get('application_type_version', ds.application_type_version)
            ds.last_access_synchronization_date = request.json.get('last_access_synchronization_date', ds.last_access_synchronization_date)
            ds.last_access_synchronization_status = request.json.get('last_access_synchronization_status', ds.last_access_synchronization_status)
            ds.last_transaction_synchronization_date = request.json.get('last_transaction_synchronization_date', ds.last_transaction_synchronization_date)
            ds.last_transaction_synchronization_status = request.json.get('last_transaction_synchronization_status', ds.last_transaction_synchronization_status)
            ds.default_datasource = request.json.get('default_datasource', ds.default_datasource)
            ds.last_updated_by = get_jwt_identity()
            ds.last_update_date = datetime.utcnow()
            db.session.commit()
            return make_response(jsonify({'message': 'Edited successfully'}), 200)
        return make_response(jsonify({'message': 'Data source not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error editing data source', 'error': str(e)}), 500)


@data_sources_bp.route('/def_data_sources', methods=['DELETE'])
@jwt_required()
def delete_def_data_source():
    try:
        def_data_source_id = request.args.get('def_data_source_id', type=int)
        if not def_data_source_id:
            return make_response(jsonify({'message': 'def_data_source_id is required'}), 400)

        ds = DefDataSource.query.filter_by(def_data_source_id=def_data_source_id).first()
        if ds:
            db.session.delete(ds)
            db.session.commit()
            return make_response(jsonify({'message': 'Deleted successfully'}), 200)
        return make_response(jsonify({'message': 'Data source not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error deleting data source', 'error': str(e)}), 500)

