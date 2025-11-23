import json
from sqlalchemy import desc
from flask import request, jsonify, make_response
from werkzeug.security import generate_password_hash
from flask_jwt_extended import jwt_required, get_jwt_identity

from executors.extensions import db
from utils.auth import role_required
from executors.models import( Message )
from . import messages_bp







@messages_bp.route('/messages/<string:id>', methods=['GET'])
def get_reply_message(id):
    try:
        # Query the database for messages with the given parentid
        messages = Message.query.filter_by(parentid=id).order_by(desc(Message.date)).all()
        
        # Check if any messages were found
        if messages:
            return make_response(jsonify([msg.json() for msg in messages]), 200)
        else:
            return jsonify({'message': 'MessageID not found.'}), 404
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    

@messages_bp.route('/messages', methods=['GET'])
def get_messages():
    try:
        # Query the database for messages with the given parentid
        messages = Message.query.all()
        
        # Check if any messages were found
        if messages:
            return make_response(jsonify([msg.json() for msg in messages]), 200)
        else:
            return jsonify({'message': 'MessageID not found.'}), 404
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    
    
@messages_bp.route('/messages', methods=['POST'])
def create_message():
    try:
        data = request.get_json()
        id            = data['id']
        sender        = data['sender']
        recivers      = data['recivers']
        subject       = data['subject']
        body          = data['body']
        date          = data['date'],
        status        = data['status']
        parentid      = data['parentid'],
        involvedusers = data['involvedusers']
        readers       = data['readers']
        
        receiver_json = json.dumps(recivers)
        involvedusers = json.dumps(involvedusers)
        
        new_message = Message(
            id            = id,
            sender        = sender,
            recivers      = recivers,
            subject       = subject,
            body          = body,
            date          = date,
            status        = status,
            parentid      = parentid,
            involvedusers = involvedusers,
            readers       = readers
        )   
        
        db.session.add(new_message)
        db.session.commit()
        
        return make_response(jsonify({"Message": "Message sent Successfully"}, 201))    
    except Exception as e:
        return make_response(jsonify({"Message": f"Error: {str(e)}"}), 500)
    

    
@messages_bp.route('/messages/<string:id>', methods=['PUT'])
def update_messages(id):
    try:
        message = Message.query.filter_by(id=id).first()
        if message:
            data = request.get_json()
            message.subject  = data['subject']
            message.body = data['body']
            db.session.commit()
            return make_response(jsonify({"message": "Message updated successfully"}), 200)
        return make_response(jsonify({"message": "Message not found"}), 404)
    except Exception as e:
        return make_response(jsonify({"message": "Error updating Message", "error": str(e)}), 500)
    

@messages_bp.route('/messages/<string:id>', methods=['DELETE'])
def delete_message(id):
    try:
        message = Message.query.filter_by(id=id).first()
        if message:
            db.session.delete(message)
            db.session.commit()
            return make_response(jsonify({"message": "Message deleted successfully"}), 200)
        return make_response(jsonify({"message": "Message not found"}), 404)
    except Exception as e:
        return make_response(jsonify({"message": "Error deleting message", "error": str(e)}), 500)


