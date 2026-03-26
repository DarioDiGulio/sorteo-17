import json
import os
import time
from contextlib import contextmanager
from datetime import datetime
from functools import wraps

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from flask import (Flask, jsonify, redirect, render_template, request,
                   send_from_directory, session, url_for)

load_dotenv()

BASE         = os.path.dirname(__file__)
DATABASE_URL = os.environ['DATABASE_URL']

app = Flask(__name__, template_folder='templates')
app.secret_key = os.environ.get('SECRET_KEY', 'sorteo17oct-dev')


# ── DB ────────────────────────────────────────────────
@contextmanager
def get_db():
    conn = psycopg2.connect(DATABASE_URL)
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        yield cur
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db():
    """Crea tablas. Reintenta hasta que Postgres esté listo."""
    for attempt in range(15):
        try:
            with get_db() as c:
                c.execute('''
                    CREATE TABLE IF NOT EXISTS numeros (
                        numero   INTEGER PRIMARY KEY,
                        nombre   TEXT NOT NULL,
                        apellido TEXT NOT NULL,
                        email    TEXT NOT NULL,
                        fecha    TEXT NOT NULL
                    )
                ''')
                c.execute('''
                    CREATE TABLE IF NOT EXISTS meta (
                        clave TEXT PRIMARY KEY,
                        valor TEXT NOT NULL
                    )
                ''')
            return
        except psycopg2.OperationalError:
            if attempt < 14:
                print(f'DB no disponible, reintentando ({attempt + 1}/15)…')
                time.sleep(2)
            else:
                raise


def get_config():
    with get_db() as c:
        c.execute("SELECT valor FROM meta WHERE clave='config'")
        row = c.fetchone()
        return json.loads(row['valor']) if row else {}


def save_config(cfg):
    with get_db() as c:
        c.execute(
            """INSERT INTO meta (clave, valor) VALUES ('config', %s)
               ON CONFLICT (clave) DO UPDATE SET valor = EXCLUDED.valor""",
            (json.dumps(cfg, ensure_ascii=False),)
        )


def current_max():
    cfg    = get_config()
    bloque = cfg.get('venta', {}).get('bloqueInicial', 200)
    with get_db() as c:
        c.execute("SELECT valor FROM meta WHERE clave='current_max'")
        row = c.fetchone()
        if not row:
            c.execute(
                """INSERT INTO meta (clave, valor) VALUES ('current_max', %s)
                   ON CONFLICT (clave) DO NOTHING""",
                (str(bloque),)
            )
            return bloque
        return int(row['valor'])


def maybe_expand():
    cfg = get_config()
    inc = cfg.get('venta', {}).get('incremento', 50)
    with get_db() as c:
        c.execute("SELECT valor FROM meta WHERE clave='current_max'")
        row  = c.fetchone()
        maxn = int(row['valor']) if row else cfg.get('venta', {}).get('bloqueInicial', 200)

        c.execute('SELECT COUNT(*) AS cnt FROM numeros WHERE numero <= %s', (maxn,))
        cnt = c.fetchone()['cnt']
        if cnt >= maxn:
            maxn += inc
            c.execute(
                """INSERT INTO meta (clave, valor) VALUES ('current_max', %s)
                   ON CONFLICT (clave) DO UPDATE SET valor = EXCLUDED.valor""",
                (str(maxn),)
            )
        return maxn


def upsert_max(val):
    with get_db() as c:
        c.execute(
            """INSERT INTO meta (clave, valor) VALUES ('current_max', %s)
               ON CONFLICT (clave) DO UPDATE SET valor = EXCLUDED.valor""",
            (str(val),)
        )


# ── STATIC ────────────────────────────────────────────
@app.route('/logo17.png')
def logo():
    return send_from_directory(BASE, 'logo17.png')


# ── SORTEO ────────────────────────────────────────────
@app.route('/')
def sorteo():
    cfg = get_config()
    if not cfg.get('sorteoPublico', True):
        return redirect(url_for('numeros'))
    return render_template('sorteo.html', config=cfg)


# ── NÚMEROS ───────────────────────────────────────────
@app.route('/numeros')
def numeros():
    cfg     = get_config()
    maxn    = maybe_expand()
    precios = cfg.get('venta', {}).get('precios', [])

    with get_db() as c:
        c.execute('SELECT numero, nombre, apellido FROM numeros WHERE numero <= %s', (maxn,))
        rows = c.fetchall()

    tomados = {
        r['numero']: {
            'nombre':    r['nombre'],
            'apellido':  r['apellido'],
            'iniciales': (r['nombre'][0] + r['apellido'][0]).upper(),
        }
        for r in rows
    }

    return render_template('numeros.html',
        config=cfg,
        maxn=maxn,
        tomados=tomados,
        precios=precios,
        mensaje=request.args.get('msg'),
        error=request.args.get('err'),
        exito_nums=request.args.get('nums'),
    )


@app.route('/numeros/reservar', methods=['POST'])
def reservar():
    nombre   = request.form.get('nombre',   '').strip()
    apellido = request.form.get('apellido', '').strip()
    email    = request.form.get('email',    '').strip().lower()
    nums_raw = request.form.get('numeros',  '')

    if not all([nombre, apellido, email, nums_raw]):
        return redirect(url_for('numeros', err='Completá todos los campos'))

    try:
        nums = [int(n) for n in nums_raw.split(',') if n.strip()]
    except ValueError:
        return redirect(url_for('numeros', err='Números inválidos'))

    if not nums:
        return redirect(url_for('numeros', err='Seleccioná al menos un número'))

    maxn = current_max()
    if any(n < 1 or n > maxn for n in nums):
        return redirect(url_for('numeros', err='Número fuera de rango'))

    placeholders = ','.join(['%s'] * len(nums))
    with get_db() as c:
        c.execute(f'SELECT numero FROM numeros WHERE numero IN ({placeholders})', nums)
        existing = c.fetchall()
        if existing:
            taken = ', '.join(str(r['numero']) for r in existing)
            return redirect(url_for('numeros', err=f'Los números {taken} ya fueron tomados'))

        fecha = datetime.now().isoformat()
        for n in nums:
            c.execute(
                'INSERT INTO numeros (numero, nombre, apellido, email, fecha) VALUES (%s, %s, %s, %s, %s)',
                (n, nombre, apellido, email, fecha)
            )

    nums_str = ','.join(str(n) for n in sorted(nums))
    return redirect(url_for('numeros',
        msg=f'¡Reserva confirmada para {nombre} {apellido}!',
        nums=nums_str,
    ))


# ── ADMIN AUTH ────────────────────────────────────────
def admin_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get('admin'):
            return redirect(url_for('admin_login'))
        return f(*args, **kwargs)
    return decorated


@app.route('/admin/login', methods=['GET', 'POST'])
def admin_login():
    error = None
    if request.method == 'POST':
        pwd      = request.form.get('password', '')
        expected = get_config().get('admin', {}).get('password', '17octubre')
        if pwd == expected:
            session['admin'] = True
            return redirect(url_for('admin'))
        error = 'Contraseña incorrecta'
    return render_template('admin_login.html', error=error)

@app.route('/admin/logout')
def admin_logout():
    session.pop('admin', None)
    return redirect(url_for('admin_login'))


# ── ADMIN DASHBOARD ───────────────────────────────────
@app.route('/admin')
@admin_required
def admin():
    cfg   = get_config()
    maxn  = current_max()
    venta = cfg.get('venta', {})

    with get_db() as c:
        c.execute('SELECT numero, nombre, apellido, email, fecha FROM numeros ORDER BY fecha DESC')
        rows = c.fetchall()

    compradores = {}
    for r in rows:
        key = (r['nombre'], r['apellido'], r['email'])
        if key not in compradores:
            compradores[key] = {'nombre': r['nombre'], 'apellido': r['apellido'],
                                'email': r['email'], 'fecha': r['fecha'], 'numeros': []}
        compradores[key]['numeros'].append(r['numero'])

    compradores = sorted(compradores.values(), key=lambda x: x['fecha'], reverse=True)

    vendidos    = len(rows)
    precio_unit = venta.get('precios', [{'precio': 0}])[0]['precio']

    return render_template('admin.html',
        config=cfg,
        maxn=maxn,
        compradores=compradores,
        stats={
            'vendidos':    vendidos,
            'disponibles': maxn - vendidos,
            'recaudado':   vendidos * precio_unit,
            'compradores': len(compradores),
        },
    )


# ── ADMIN CONFIG (AJAX) ───────────────────────────────
@app.route('/admin/config', methods=['POST'])
@admin_required
def admin_save_config():
    try:
        data = request.get_json()
        cfg  = get_config()
        for key, val in data.items():
            cfg[key] = val
        save_config(cfg)
        return jsonify(ok=True)
    except Exception as e:
        return jsonify(ok=False, error=str(e)), 400


# ── ADMIN BORRAR COMPRADOR ────────────────────────────
@app.route('/admin/comprador/delete', methods=['POST'])
@admin_required
def admin_delete_comprador():
    d = request.json
    with get_db() as c:
        c.execute('DELETE FROM numeros WHERE nombre=%s AND apellido=%s AND email=%s',
                  (d['nombre'], d['apellido'], d['email']))
    return jsonify(ok=True)


# ── ADMIN LIBERAR NÚMERO ──────────────────────────────
@app.route('/admin/numero/delete', methods=['POST'])
@admin_required
def admin_delete_numero():
    with get_db() as c:
        c.execute('DELETE FROM numeros WHERE numero=%s', (request.json['numero'],))
    return jsonify(ok=True)


# ── ADMIN RESET MAX ───────────────────────────────────
@app.route('/admin/reset-max', methods=['POST'])
@admin_required
def admin_reset_max():
    bloque = get_config().get('venta', {}).get('bloqueInicial', 200)
    upsert_max(bloque)
    return jsonify(ok=True)


# ── ADMIN LIMPIAR COMPRADORES ─────────────────────────
@app.route('/admin/reset-compradores', methods=['POST'])
@admin_required
def admin_reset_compradores():
    bloque = get_config().get('venta', {}).get('bloqueInicial', 200)
    with get_db() as c:
        c.execute('DELETE FROM numeros')
    upsert_max(bloque)
    return jsonify(ok=True)


# ── RUN ───────────────────────────────────────────────
if __name__ == '__main__':
    init_db()
    port = int(os.environ.get('PORT', 3001))
    app.run(debug=os.environ.get('FLASK_ENV') == 'development', port=port)
