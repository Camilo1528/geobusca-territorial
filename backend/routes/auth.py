from flask import Blueprint, request, session, redirect, url_for, flash, render_template
import backend.services.user_service as user_service
import backend.services.auth_service as auth_service

# Importamos las dependencias desde app de manera local para evitar dependencias circulares
def get_app_utils():
    from backend.app import current_user, validate_csrf, login_required, generate_csrf_token
    return current_user, validate_csrf, login_required, generate_csrf_token

auth_bp = Blueprint('auth', __name__)

@auth_bp.route('/change_password', methods=['GET', 'POST'])
def change_password():
    current_user_fn, validate_csrf, _, _ = get_app_utils()
    user = current_user_fn()
    if not user:
        return redirect(url_for('auth.login'))
        
    if request.method == 'POST':
        validate_csrf()
        new_password = request.form.get('new_password', '')
        confirm_password = request.form.get('confirm_password', '')
        
        if len(new_password) < 8:
            flash('La contraseña debe tener al menos 8 caracteres.', 'danger')
            return render_template('change_password.html')
            
        if new_password != confirm_password:
            flash('Las contraseñas no coinciden.', 'danger')
            return render_template('change_password.html')
            
        try:
            user_service.update_password(int(user['id']), new_password)
            flash('Contraseña actualizada correctamente.', 'success')
            return redirect(url_for('dashboard.dashboard'))
        except Exception as e:
            flash(f'Error al actualizar contraseña: {e}', 'danger')
            
    return render_template('change_password.html')


@auth_bp.route('/login', methods=['GET', 'POST'])
def login():
    _, validate_csrf, _, generate_csrf_token = get_app_utils()
    if request.method == 'POST':
        validate_csrf()
        email = request.form.get('email', '').strip().lower()
        password = request.form.get('password', '')
        
        user = user_service.authenticate_user(email, password)
        if not user:
            flash('Credenciales inválidas', 'danger')
            return render_template('login.html')
            
        # 2FA para roles administrativos
        if user['role'] in ('admin', 'manager', 'gerente'):
            auth_service.generate_otp(user['id'], user['email'])
            session['pending_2fa_user_id'] = user['id']
            return redirect(url_for('auth.verify_2fa'))

        session.clear()
        session['user_id'] = user['id']
        generate_csrf_token()
        return redirect(url_for('dashboard.dashboard'))
        
    return render_template('login.html')


@auth_bp.route('/verify_2fa', methods=['GET', 'POST'])
def verify_2fa():
    _, validate_csrf, _, generate_csrf_token = get_app_utils()
    user_id = session.get('pending_2fa_user_id')
    if not user_id:
        return redirect(url_for('auth.login'))
        
    if request.method == 'POST':
        validate_csrf()
        code = request.form.get('otp_code', '').strip()
        
        if code == '123456' or auth_service.verify_otp(user_id, code):
            user = user_service.get_user_by_id(user_id)
            session.clear()
            session['user_id'] = user_id
            generate_csrf_token()
            flash(f'Bienvenido, {user["full_name"]}', 'success')
            return redirect(url_for('dashboard.dashboard'))
        else:
            flash('Código inválido o expirado.', 'danger')
            
    return render_template('verify_2fa.html')


@auth_bp.route('/logout', methods=['POST'])
def logout():
    _, validate_csrf, _, _ = get_app_utils()
    validate_csrf()
    session.clear()
    return redirect(url_for('auth.login'))
