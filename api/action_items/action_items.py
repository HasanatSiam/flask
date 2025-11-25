from flask import request, jsonify, make_response
from flask_jwt_extended import jwt_required, get_jwt_identity
from datetime import datetime
from sqlalchemy import or_, func

from utils.auth import role_required
from executors.extensions import db
from executors.models import (
    DefActionItem,
    DefActionItemAssignment,
    DefNotifications,
    DefActionItemsV

)
from . import action_items_bp


# Create a DefActionItem
@action_items_bp.route('/def_action_items', methods=['POST'])
@jwt_required()
def create_action_item():
    try:
        action_item_name = request.json.get('action_item_name')
        description = request.json.get('description')
        notification_id = request.json.get('notification_id')
        user_ids = request.json.get('user_ids')
        action = request.json.get('action')

        created_by = get_jwt_identity()

        if not action_item_name:
            return make_response(jsonify({"message": "Action item name is required"}), 400)

        # existing_item = DefActionItem.query.filter_by(action_item_name=action_item_name).first()
        # if existing_item:
        #     return make_response(jsonify({"message": "Action item name already exists"}), 400)

        # if notification_id:
        #     # Optionally validate if notification exists
        #     notification = db.session.query(DefNotification.notification_id).filter_by(notification_id=notification_id).first()
        #     if not notification:
        #         return make_response(jsonify({"message": "Invalid notification_id"}), 400)

        new_action_item = DefActionItem(
            action_item_name = action_item_name,
            description = description,
            created_by = created_by,
            creation_date = datetime.utcnow(),
            last_updated_by = created_by,
            last_update_date = datetime.utcnow(),
            notification_id = notification_id
        )

        db.session.add(new_action_item)
        db.session.flush()  # so we get the ID

        # Add assignments
        if new_action_item:
            for uid in user_ids:
                assignment = DefActionItemAssignment(
                    action_item_id = new_action_item.action_item_id,
                    user_id = uid,
                    status = 'NEW',
                    created_by = get_jwt_identity(),
                    # creation_date = datetime.utcnow(),
                    last_updated_by = get_jwt_identity(),
                    # last_update_date = datetime.utcnow()
                )
                db.session.add(assignment)

    #update action_item_id in def_notifications table
        if new_action_item:
            notification = DefNotifications.query.filter_by(notification_id=notification_id).first()
            if notification:
                notification.action_item_id = new_action_item.action_item_id
            


        db.session.commit()

        if action == 'DRAFT':
            return make_response(jsonify({
                'message': 'Action item saved successfully',
                'result': new_action_item.json()

            }), 201)

        if action == 'SENT':
            return make_response(jsonify({
                'message': 'Action item sent successfully',
                'result': new_action_item.json()
            }), 201)


    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({"message": "Error creating action item", "error": str(e)}), 500)




# Get all DefActionItems (Consolidated Endpoint)
@action_items_bp.route('/def_action_items', methods=['GET'])
@jwt_required()
def get_action_items():
    try:
        # Query Parameters
        action_item_id = request.args.get('action_item_id', type=int)
        user_id = request.args.get('user_id', type=int)
        page = request.args.get('page', type=int)
        limit = request.args.get('limit', type=int)
        status = request.args.get('status')
        action_item_name = request.args.get('action_item_name', '').strip()

        # 1. Single Item by ID
        if action_item_id:
            action_item = DefActionItem.query.filter_by(action_item_id=action_item_id).first()
            if action_item:
                return make_response(jsonify({"result": action_item.json()}), 200)
            else:
                return make_response(jsonify({"message": "Action item not found"}), 404)

        # 2. List Items (User View or Admin/General View)
        if user_id:
            # User View: Filter by user_id and notification_status='sent'
            query = DefActionItemsV.query.filter_by(user_id=user_id).filter(
                func.lower(func.trim(DefActionItemsV.notification_status)) == "sent"
            )
            
            # Additional filters for User View
            if status:
                query = query.filter(
                    func.lower(func.trim(DefActionItemsV.status)) == func.lower(func.trim(status))
                )

            if action_item_name:
                search_underscore = action_item_name.replace(' ', '_')
                search_space = action_item_name.replace('_', ' ')
                query = query.filter(
                    or_(
                        DefActionItemsV.action_item_name.ilike(f'%{action_item_name}%'),
                        DefActionItemsV.action_item_name.ilike(f'%{search_underscore}%'),
                        DefActionItemsV.action_item_name.ilike(f'%{search_space}%')
                    )
                )
            
            query = query.order_by(DefActionItemsV.action_item_id.desc())
            
        else:
            # General View (Admin or all items)
            query = DefActionItemsV.query.order_by(DefActionItemsV.action_item_id.desc())

        # 3. Pagination
        if page and limit:
            paginated = query.paginate(page=page, per_page=limit, error_out=False)
            return make_response(jsonify({
                "result": [item.json() for item in paginated.items],
                "total": paginated.total,
                "pages": paginated.pages,
                "page": paginated.page
            }), 200)

        # 4. Return All (No Pagination)
        items = query.all()
        return make_response(jsonify({
            "result": [item.json() for item in items]
        }), 200)

    except Exception as e:
        return make_response(jsonify({"message": "Error retrieving action items", "error": str(e)}), 500)







@action_items_bp.route('/def_action_items/upsert', methods=['POST'])
@jwt_required()
def upsert_action_item():
    data = request.get_json()
    if not data:
        return jsonify({"message": "Invalid request: No JSON data provided"}), 400

    try:
        action_item_id = data.get('action_item_id')
        user_ids = data.get('user_ids', [])
        status = data.get('status')
        current_user = get_jwt_identity()

        if action_item_id:
            # --- UPDATE ---
            action_item = db.session.get(DefActionItem, action_item_id)
            if not action_item:
                return jsonify({"message": f"Action Item with ID {action_item_id} not found"}), 404

            #Check duplicate name if changing
            # new_name = data.get('action_item_name')
            # if new_name and new_name != action_item.action_item_name:
            #     duplicate = DefActionItem.query.filter_by(action_item_name=new_name).first()
            #     if duplicate:
            #         return jsonify({"message": "Action item name already exists"}), 400
            #     action_item.action_item_name = new_name

            if "action_item_name" in data:
                action_item.action_item_name = data["action_item_name"]

            if "description" in data:
                action_item.description = data["description"]
            if "notification_id" in data:
                action_item.notification_id = data["notification_id"]

            action_item.last_updated_by = current_user
            message = "Edited successfully"
            status_code = 200

        else:
            # --- CREATE ---
            action_item_name = data.get('action_item_name')
            if not action_item_name:
                return jsonify({"message": "Missing required field: action_item_name"}), 400

            # Check duplicate name before insert
            # duplicate = DefActionItem.query.filter_by(action_item_name=action_item_name).first()
            # if duplicate:
            #     return jsonify({"message": "Action item name already exists"}), 400

            action_item = DefActionItem(
                action_item_name = action_item_name,
                description = data.get('description'),
                created_by = current_user,
                creation_date = datetime.utcnow(),
                last_updated_by = current_user,
                last_update_date = datetime.utcnow(),
                notification_id = data.get('notification_id')
            )
            db.session.add(action_item)
            db.session.flush()  # get the ID before assignments
            message = "Added successfully"
            status_code = 201

        db.session.commit()

        # Handle user assignments
        if user_ids:
            # Remove old assignments if updating
            if action_item_id:
                DefActionItemAssignment.query.filter_by(action_item_id=action_item.action_item_id).delete()

            for uid in user_ids:
                assignment = DefActionItemAssignment(
                    action_item_id=action_item.action_item_id,
                    user_id=uid,
                    status=status,
                    created_by=current_user,
                    last_updated_by=current_user
                )
                db.session.add(assignment)

            db.session.commit()

        return make_response(jsonify({
            "message": message,
            "action_item_id": action_item.action_item_id
        }), status_code)

    except Exception as e:
        db.session.rollback()
        return jsonify({"message": "An unexpected error occurred", "error": str(e)}), 500



@action_items_bp.route('/def_action_items/<int:action_item_id>', methods=['PUT'])
@jwt_required()
def update_action_item(action_item_id):
    try:
        data = request.get_json()
        current_user = get_jwt_identity()

        action_item_name = data.get('action_item_name')
        description = data.get('description')
        notification_id = data.get('notification_id')
        user_ids = data.get('user_ids', [])
        action = data.get('action')
        

        # --- Update DefActionItem main record ---
        action_item = DefActionItem.query.get(action_item_id)
        if not action_item:
            return make_response(jsonify({'message': 'Action Item not found'}), 404)

        if action_item_name:
            action_item.action_item_name = action_item_name
        if description:
            action_item.description = description
        if notification_id:
            action_item.notification_id = notification_id

        action_item.last_updated_by = current_user
        action_item.last_update_date = datetime.utcnow()

        db.session.commit()

        # --- Handle DefActionItemAssignment sync ---
        existing_assignments = DefActionItemAssignment.query.filter_by(
            action_item_id=action_item_id
        ).all()
        existing_user_ids = {a.user_id for a in existing_assignments}
        incoming_user_ids = set(map(int, user_ids))

        # Find differences
        users_to_add = incoming_user_ids - existing_user_ids
        users_to_update = incoming_user_ids & existing_user_ids
        users_to_delete = existing_user_ids - incoming_user_ids

        # Add new recipients
        for uid in users_to_add:
            new_assignment = DefActionItemAssignment(
                action_item_id=action_item_id,
                user_id=uid,
                status='NEW',
                created_by=current_user,
                last_updated_by=current_user
            )
            db.session.add(new_assignment)

        # Update existing recipients
        for uid in users_to_update:
            assignment = DefActionItemAssignment.query.filter_by(
                action_item_id=action_item_id,
                user_id=uid
            ).first()
            if assignment:
                assignment.last_updated_by = current_user
                assignment.last_update_date = datetime.utcnow()

        # Delete removed recipients
        if users_to_delete:
            DefActionItemAssignment.query.filter(
                DefActionItemAssignment.action_item_id == action_item_id,
                DefActionItemAssignment.user_id.in_(users_to_delete)
            ).delete(synchronize_session=False)

        db.session.commit()

        if action == 'DRAFT':
            return make_response(jsonify({
                'message': 'Action item saved successfully',
                'result': action_item.json()
            }), 200)

        if action == 'SENT':
            return make_response(jsonify({
                'message': 'Action item sent successfully',
                'result': action_item.json()
            }), 200)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({'error': str(e)}), 500)




# Delete a DefActionItem
@action_items_bp.route('/def_action_items/<int:action_item_id>', methods=['DELETE'])
@jwt_required()
def delete_action_item(action_item_id):
    try:
        action_item = DefActionItem.query.filter_by(action_item_id=action_item_id).first()
        if not action_item:
            return make_response(jsonify({"message": "Action item not found"}), 404)

        # First delete all related assignments
        DefActionItemAssignment.query.filter_by(action_item_id=action_item_id).delete()

        # Then delete the main action item
        db.session.delete(action_item)
        db.session.commit()

        return make_response(jsonify({"message": "Deleted successfully"}), 200)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({"message": "Error deleting action item", "error": str(e)}), 500)



# Update DefActionItemAssignments (replace user_ids for given action_item_id)
@action_items_bp.route('/def_action_items/update_status/<int:user_id>/<int:action_item_id>', methods=['PUT'])
@jwt_required()
def update_action_item_assignment_status(user_id, action_item_id):
    try:
        data = request.get_json()
        if not data or 'status' not in data:
            return make_response(jsonify({"message": "Missing required field: status"}), 400)

        # Fetch the assignment
        assignment = DefActionItemAssignment.query.filter_by(
            action_item_id=action_item_id,
            user_id=user_id
        ).first()

        if not assignment:
            return make_response(jsonify({"message": "Assignment not found"}), 404)

        # Update only the status
        assignment.status = data['status']
        assignment.last_updated_by = get_jwt_identity()
        assignment.last_update_date = datetime.utcnow()

        db.session.commit()
        return make_response(jsonify({"message": "Status Updated Successfully"}), 200)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({"message": "Error updating status", "error": str(e)}), 500)



@action_items_bp.route('/def_action_items_view/<int:user_id>/<int:page>/<int:limit>', methods=['GET'])
@jwt_required()
def get_paginated_action_items_view(user_id, page, limit):
    try:
        status = request.args.get('status')
        action_item_name = request.args.get('action_item_name', '').strip()
        search_underscore = action_item_name.replace(' ', '_')
        search_space = action_item_name.replace('_', ' ')

        # Validate pagination
        if page < 1 or limit < 1:
            return make_response(jsonify({
                "message": "Page and limit must be positive integers"
            }), 400)

        # Base 
        # query = DefActionItemsV.query.filter_by(user_id=user_id)

        # Base query: filter by user_id and only SENT status
        query = DefActionItemsV.query.filter_by(user_id=user_id).filter(
            func.lower(func.trim(DefActionItemsV.notification_status)) == "sent"
        )

        #Apply status filter if provided
        if status:
            query = query.filter(
                func.lower(func.trim(DefActionItemsV.status)) == func.lower(func.trim(status))
            )

        if action_item_name:
            query = query.filter(
                or_(
                    DefActionItemsV.action_item_name.ilike(f'%{action_item_name}%'),
                    DefActionItemsV.action_item_name.ilike(f'%{search_underscore}%'),
                    DefActionItemsV.action_item_name.ilike(f'%{search_space}%')
                )
            )

        query = query.order_by(DefActionItemsV.action_item_id.desc())

        
        # paginated
        paginated = query.paginate(page=page, per_page=limit, error_out=False)
        return make_response(jsonify({
            "items": [item.json() for item in paginated.items],
            "total": paginated.total,
            "pages": paginated.pages,
            "page": paginated.page
        }), 200)

        # Without pagination
        
        # items = query.all()
        # return make_response(jsonify({
        #     "items": [item.json() for item in items],
        #     "total": len(items)
        # }), 200)

    except Exception as e:
        return make_response(jsonify({
            'message': 'Error fetching action items view',
            'error': str(e)
        }), 500)

