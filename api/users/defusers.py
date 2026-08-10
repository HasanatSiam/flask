import os
import io
import shutil
from datetime import datetime
from flask import request, jsonify, make_response, send_from_directory
from flask_jwt_extended import jwt_required, get_jwt_identity
from werkzeug.utils import secure_filename

from sqlalchemy import or_

from utils.auth import role_required
from executors.extensions import db
from executors.models import DefUser

from . import users_bp


ALLOWED_EXTENSIONS = {'png', 'jpg', 'jpeg', 'gif', 'webp', 'bmp', 'svg'}


def get_project_root() -> str:
    """Return the absolute normalized path to the project root directory, cross-platform."""
    return os.path.abspath(os.path.normpath(os.path.join(os.path.dirname(__file__), '..', '..')))


def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


def generate_thumbnail(source_path, thumbnail_path, size=(200, 200), max_size_bytes=10 * 1024):
    try:
        from PIL import Image
        with Image.open(source_path) as img:
            if getattr(img, "is_animated", False):
                img.seek(0)

            # Handle RGBA/LA/P transparency for clean JPEG output
            if img.mode in ("RGBA", "LA", "P"):
                rgba_img = img.convert("RGBA")
                background = Image.new("RGB", rgba_img.size, (255, 255, 255))
                background.paste(rgba_img, mask=rgba_img.split()[3])
                img = background
            elif img.mode != "RGB":
                img = img.convert("RGB")

            # Resize without enlargement if already smaller
            if img.width > size[0] or img.height > size[1]:
                img.thumbnail(size, Image.Resampling.LANCZOS)

            # Progressive compression starting at quality 80 down to 10 to keep size <= 10 KB
            quality = 80
            buffer = io.BytesIO()
            img.save(buffer, format="JPEG", quality=quality, optimize=True)

            while buffer.tell() > max_size_bytes and quality > 10:
                quality -= 5
                buffer.seek(0)
                buffer.truncate(0)
                img.save(buffer, format="JPEG", quality=quality, optimize=True)

            # If still exceeding 10 KB at minimum quality, scale dimensions down
            while buffer.tell() > max_size_bytes and (img.width > 60 and img.height > 60):
                new_size = (int(img.width * 0.8), int(img.height * 0.8))
                img = img.resize(new_size, Image.Resampling.LANCZOS)
                buffer.seek(0)
                buffer.truncate(0)
                img.save(buffer, format="JPEG", quality=quality, optimize=True)

            # Write compressed thumbnail buffer to target path
            with open(thumbnail_path, "wb") as f:
                f.write(buffer.getvalue())

            return True
    except Exception:
        try:
            shutil.copyfile(source_path, thumbnail_path)
            return True
        except Exception:
            return False





@users_bp.route('/defusers', methods=['POST'])
@jwt_required()
@role_required()
def create_def_user():
    try:
        # Parse data from the request body
        data = request.get_json()
        # user_id         = generate_user_id()
        user_name       = data['user_name']
        user_type       = data['user_type']
        email_address   = data['email_address']
        tenant_id       = data['tenant_id']
        profile_picture = data.get('profile_picture') or {
            "original": "uploads/profiles/default/profile.jpg",
            "thumbnail": "uploads/profiles/default/thumbnail.jpg"
        }
        user_invitation_id = data.get('user_invitation_id')
        date_of_birth     = data.get('date_of_birth')

        # Duplicate check
        existing_user = DefUser.query.filter_by(email_address=email_address).first()
        if existing_user:
            return make_response(jsonify({"message": "Email address already exists"}), 409)
        

       # Convert the list of email addresses to a JSON-formatted string
       # email_addresses_json = json.dumps(email_addresses)  # Corrected variable name

       # Create a new ArcUser object
        new_user = DefUser(
        #   user_id         = user_id,
          user_name       = user_name,
          user_type       = user_type,
          email_address   = email_address,  # Corrected variable name
          created_by      = get_jwt_identity(),
          creation_date   = datetime.utcnow(),
          last_updated_by = get_jwt_identity(),
          last_update_date= datetime.utcnow(),
          tenant_id       = tenant_id,
          profile_picture = profile_picture,
          user_invitation_id = user_invitation_id,
          date_of_birth   = date_of_birth
        )
        # Add the new user to the database session
        db.session.add(new_user)
        # Commit the changes to the database
        db.session.commit()

        # Return a success response
        return make_response(jsonify({"message": "Added successfully",
                                       "User Id": new_user.user_id}), 201)

    except Exception as e:
        return make_response(jsonify({"message": f"Error: {str(e)}"}), 500)
    

@users_bp.route('/defusers', methods=['GET'])
@jwt_required()
@role_required()
def get_users():
    try:
        users = DefUser.query.all()
        return make_response(jsonify([user.json() for user in users]), 200)
    except Exception as e:
        return make_response(jsonify({'message': 'Error getting users', 'error': str(e)}), 500)
    

@users_bp.route('/defusers/<int:page>/<int:limit>', methods=['GET'])
@jwt_required()
@role_required()
def get_paginated_def_users(page, limit):
    try:
        query = DefUser.query.order_by(DefUser.user_id.desc())
        paginated = query.paginate(page=page, per_page=limit, error_out=False)

        return make_response(jsonify({
            "items": [user.json() for user in paginated.items],
            "total": paginated.total,
            "pages": paginated.pages,
            "page": paginated.page
        }), 200)
    except Exception as e:
        return make_response(jsonify({'message': 'Error getting users', 'error': str(e)}), 500)



@users_bp.route('/defusers/search/<int:page>/<int:limit>', methods=['GET'])
@jwt_required()
@role_required()
def search_def_users(page, limit):
    try:
        search_query = request.args.get('user_name', '').strip().lower()
        search_underscore = search_query.replace(' ', '_')
        search_space = search_query.replace('_', ' ')
        query = DefUser.query

        if search_query:
            query = query.filter(
                or_(
                    DefUser.user_name.ilike(f'%{search_query}%'),
                    DefUser.user_name.ilike(f'%{search_underscore}%'),
                    DefUser.user_name.ilike(f'%{search_space}%')
                )
            )

        paginated = query.order_by(DefUser.user_id.desc()).paginate(page=page, per_page=limit, error_out=False)

        return make_response(jsonify({
            "items": [user.json() for user in paginated.items],
            "total": paginated.total,
            "pages": 1 if paginated.total == 0 else paginated.pages,
            "page":  paginated.page
        }), 200)
    except Exception as e:
        return make_response(jsonify({"message": "Error searching users", "error": str(e)}), 500)


# get a user by id
@users_bp.route('/defusers/<int:user_id>', methods=['GET'])
@jwt_required()
@role_required()
def get_user(user_id):
    try:
        user = DefUser.query.filter_by(user_id=user_id).first()
        if user:
            return make_response(jsonify({'user': user.json()}), 200)
        return make_response(jsonify({'message': 'User not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error getting user', 'error': str(e)}), 500)
    
    
@users_bp.route('/defusers/<int:user_id>', methods=['PUT'])
@jwt_required()
@role_required()
def update_user(user_id):
    try:
        user = DefUser.query.filter_by(user_id=user_id).first()
        if user:
            data = request.get_json()
            if 'user_name' in data:
                user.user_name = data['user_name']
            if 'email_address' in data:
                user.email_address = data['email_address']
            if 'tenant_id' in data:
                user.tenant_id = data['tenant_id']
            if 'date_of_birth' in data:
                user.date_of_birth = data['date_of_birth']
            user.last_updated_by = get_jwt_identity()
            user.last_update_date = datetime.utcnow()
            db.session.commit()
            return make_response(jsonify({'message': 'Edited successfully'}), 200)
        return make_response(jsonify({'message': 'User not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error updating user', 'error': str(e)}), 500)


@users_bp.route('/defusers/<int:user_id>', methods=['DELETE'])
@jwt_required()
@role_required()
def delete_user(user_id):
    try:
        user = DefUser.query.filter_by(user_id=user_id).first()
        if user:
            db.session.delete(user)
            db.session.commit()
            return make_response(jsonify({'message': 'Deleted successfully'}), 200)
        return make_response(jsonify({'message': 'User not found'}), 404)
    except Exception:
        return make_response(jsonify({'message': 'Error deleting user'}), 500)


@users_bp.route('/users/profile_picture', methods=['POST', 'PUT'])
@jwt_required()
def upsert_profile_picture():
    try:
        # Get user_id automatically from JWT identity (loaded from cookies / Authorization header)
        jwt_id = get_jwt_identity()
        user_id = None

        if jwt_id is not None:
            try:
                user_id = int(jwt_id)
            except (ValueError, TypeError):
                user_id = jwt_id

        # Fallback to direct cookie if jwt_id is not set
        if not user_id:
            cookie_user_id = request.cookies.get("user_id") or request.cookies.get("id")
            if cookie_user_id and str(cookie_user_id).isdigit():
                user_id = int(cookie_user_id)

        if not user_id:
            return make_response(jsonify({'message': 'Authentication required. Could not identify user from cookies/token'}), 401)

        # Check if user exists in database
        user = DefUser.query.filter_by(user_id=user_id).first()
        if not user:
            return make_response(jsonify({'message': f'User with id {user_id} not found'}), 404)

        # Check if files were provided in the request
        file = None
        for key in ['file', 'profile_picture', 'image', 'picture', 'photo']:
            if key in request.files and request.files[key].filename:
                file = request.files[key]
                break

        if not file:
            if request.files:
                first_key = next(iter(request.files.keys()))
                if request.files[first_key].filename:
                    file = request.files[first_key]

        if not file or not file.filename:
            return make_response(jsonify({'message': 'No image file uploaded in request'}), 400)

        if not allowed_file(file.filename):
            return make_response(jsonify({
                'message': f'Invalid file format. Allowed formats: {", ".join(sorted(ALLOWED_EXTENSIONS))}'
            }), 400)

        # Determine extension and standardized filename: profile_{user_id}.{ext} (lowercase for cross-platform case-sensitivity)
        ext = file.filename.rsplit('.', 1)[1].lower() if '.' in file.filename else 'jpg'
        original_filename = f"profile_{user_id}.{ext}"

        # Target directory: uploads/profiles/{user_id} (normalized for Windows & Linux)
        project_root = get_project_root()
        upload_dir = os.path.normpath(os.path.join(project_root, 'uploads', 'profiles', str(user_id)))
        os.makedirs(upload_dir, exist_ok=True)

        # Clean up any existing old profile_{user_id}.* files to prevent stale files with different extensions
        if os.path.exists(upload_dir):
            for existing_file in os.listdir(upload_dir):
                if existing_file.startswith(f"profile_{user_id}."):
                    try:
                        os.remove(os.path.join(upload_dir, existing_file))
                    except OSError:
                        pass

        original_file_path = os.path.normpath(os.path.join(upload_dir, original_filename))
        thumbnail_file_path = os.path.normpath(os.path.join(upload_dir, 'thumbnail.jpg'))

        # Save original file
        file.save(original_file_path)

        # Generate thumbnail
        generate_thumbnail(original_file_path, thumbnail_file_path, size=(200, 200))

        # Relative paths stored in def_users table (always standard forward slashes for URLs and DB JSONB)
        profile_picture_data = {
            "original": f"uploads/profiles/{user_id}/{original_filename}".replace("\\", "/"),
            "thumbnail": f"uploads/profiles/{user_id}/thumbnail.jpg".replace("\\", "/")
        }

        # Update DefUser record
        user.profile_picture = profile_picture_data
        user.last_updated_by = user_id
        user.last_update_date = datetime.utcnow()

        db.session.commit()

        return make_response(jsonify({
            "message": "Profile picture uploaded successfully",
            "user_id": user_id,
            "profile_picture": profile_picture_data
        }), 200)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({'message': 'Error uploading profile picture', 'error': str(e)}), 500)


@users_bp.route('/users/profile_picture', methods=['GET'])
@users_bp.route('/users/profile_picture/thumbnail', methods=['GET'])
@jwt_required(optional=True)
def get_profile_picture():
    try:
        # Get user_id automatically from JWT identity or cookies
        jwt_id = get_jwt_identity()
        user_id = None

        if jwt_id is not None:
            try:
                user_id = int(jwt_id)
            except (ValueError, TypeError):
                user_id = jwt_id

        if not user_id:
            cookie_user_id = request.cookies.get("user_id") or request.cookies.get("id")
            if cookie_user_id and str(cookie_user_id).isdigit():
                user_id = int(cookie_user_id)

        # Fallback to query param user_id if provided
        if not user_id and request.args.get('user_id', type=int):
            user_id = request.args.get('user_id', type=int)

        if not user_id:
            return make_response(jsonify({'message': 'Authentication required. Could not identify user from cookies/token'}), 401)

        user = DefUser.query.filter_by(user_id=user_id).first()
        if not user:
            return make_response(jsonify({'message': f'User with id {user_id} not found'}), 404)

        # If JSON response is requested (e.g. ?json=true or Accept: application/json)
        if request.args.get('json', '').lower() in ('true', '1') or request.headers.get('Accept') == 'application/json':
            return make_response(jsonify({
                'user_id': user_id,
                'profile_picture': user.profile_picture
            }), 200)

        project_root = get_project_root()

        # Check if thumbnail is requested (via route /thumbnail or query param ?thumbnail=true or ?type=thumbnail)
        is_thumbnail = request.path.endswith('/thumbnail') or request.args.get('thumbnail', '').lower() in ('true', '1') or request.args.get('type') == 'thumbnail'

        if is_thumbnail:
            user_folder = os.path.normpath(os.path.join(project_root, 'uploads', 'profiles', str(user_id)))
            thumbnail_file = 'thumbnail.jpg'
            if os.path.exists(os.path.join(user_folder, thumbnail_file)):
                return send_from_directory(user_folder, thumbnail_file)

            # Default thumbnail fallback
            default_folder = os.path.normpath(os.path.join(project_root, 'uploads', 'profiles', 'default'))
            if os.path.exists(os.path.join(default_folder, 'thumbnail.jpg')):
                return send_from_directory(default_folder, 'thumbnail.jpg')

        # Serve original profile picture
        pic_data = user.profile_picture or {}
        orig_path = pic_data.get('original')
        if orig_path:
            # Normalize path slashes from DB (supports both / and \)
            norm_rel_path = orig_path.replace("/", os.sep).replace("\\", os.sep)
            full_orig_path = os.path.normpath(os.path.join(project_root, norm_rel_path))
            if os.path.exists(full_orig_path):
                folder = os.path.dirname(full_orig_path)
                filename = os.path.basename(full_orig_path)
                return send_from_directory(folder, filename)

        # Search for profile_{user_id}.* in user's directory
        user_folder = os.path.normpath(os.path.join(project_root, 'uploads', 'profiles', str(user_id)))
        if os.path.exists(user_folder):
            for fname in os.listdir(user_folder):
                if fname.startswith(f"profile_{user_id}."):
                    return send_from_directory(user_folder, fname)

        # Fallback to default profile image
        default_folder = os.path.normpath(os.path.join(project_root, 'uploads', 'profiles', 'default'))
        if os.path.exists(os.path.join(default_folder, 'profile.jpg')):
            return send_from_directory(default_folder, 'profile.jpg')

        return make_response(jsonify({'message': 'Profile picture not found'}), 404)

    except Exception as e:
        return make_response(jsonify({'message': 'Error retrieving profile picture', 'error': str(e)}), 500)


@users_bp.route('/uploads/profiles/<int:user_id>/<path:filename>', methods=['GET'])
def get_profile_picture_file(user_id, filename):
    try:
        project_root = get_project_root()
        folder = os.path.normpath(os.path.join(project_root, 'uploads', 'profiles', str(user_id)))
        safe_filename = os.path.basename(filename)
        return send_from_directory(folder, safe_filename)
    except Exception as e:
        return make_response(jsonify({'message': 'File not found', 'error': str(e)}), 404)



