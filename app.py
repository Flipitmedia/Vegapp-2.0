"""
Sistema de Gestión de Pedidos - La Vega
Procesa exportaciones de Shopify y genera listas de compras y armado.

Diseñado e implementado por Flipit.media
"""

from fastapi import FastAPI, UploadFile, File, Request, Form, HTTPException, BackgroundTasks
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import csv
import io
import re
from datetime import datetime, date, timedelta
from typing import Optional
import json
import os
from pathlib import Path
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders

from openpyxl import Workbook, load_workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side

import sqlite3

app = FastAPI(title="Sistema Gestión La Vega - by Flipit.media")

BASE_DIR = Path(__file__).resolve().parent
app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")
templates = Jinja2Templates(directory=BASE_DIR / "templates")

OUTPUT_DIR = BASE_DIR / "outputs"
OUTPUT_DIR.mkdir(exist_ok=True)

DB_PATH = BASE_DIR / "vega.db"

# ============================================
# BASE DE DATOS
# ============================================

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db()
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS categorias (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nombre TEXT UNIQUE NOT NULL,
            orden INTEGER DEFAULT 0
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS producto_categoria (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            producto TEXT UNIQUE NOT NULL,
            categoria_id INTEGER,
            FOREIGN KEY (categoria_id) REFERENCES categorias(id)
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS pedidos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_number TEXT UNIQUE NOT NULL,
            email TEXT,
            comuna TEXT,
            fecha_entrega DATE,
            fecha_original DATE,
            direccion TEXT,
            telefono TEXT,
            nombre_cliente TEXT,
            total REAL,
            created_at TIMESTAMP,
            imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            status TEXT DEFAULT 'pendiente'
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS lineas_pedido (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pedido_id INTEGER,
            producto TEXT NOT NULL,
            cantidad INTEGER NOT NULL,
            precio REAL,
            sku TEXT,
            FOREIGN KEY (pedido_id) REFERENCES pedidos(id)
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS configuracion (
            clave TEXT PRIMARY KEY,
            valor TEXT
        )
    ''')
    
    categorias_default = [
        ('Frutas', 1), ('Verduras', 2), ('Congelados', 3),
        ('Abarrotes', 4), ('Lácteos', 5), ('Carnes', 6), ('Otros', 99)
    ]
    
    for nombre, orden in categorias_default:
        cursor.execute('INSERT OR IGNORE INTO categorias (nombre, orden) VALUES (?, ?)', (nombre, orden))
    
    config_default = [
        ('backup_email', ''), ('backup_frecuencia', '3'),
        ('backup_hora', '08:00'), ('smtp_host', 'smtp.gmail.com'),
        ('smtp_port', '587'), ('smtp_user', ''), ('smtp_pass', ''),
    ]
    
    for clave, valor in config_default:
        cursor.execute('INSERT OR IGNORE INTO configuracion (clave, valor) VALUES (?, ?)', (clave, valor))
    
    conn.commit()
    conn.close()

init_db()

# ============================================
# UTILIDADES
# ============================================

def parse_note_attributes(note_attrs: str) -> dict:
    result = {'comuna': None, 'fecha_entrega': None}
    if not note_attrs:
        return result
    
    comuna_match = re.search(r'Comuna de Entrega:\s*([^\n]+)', note_attrs)
    if comuna_match:
        result['comuna'] = comuna_match.group(1).strip()
    
    fecha_match = re.search(r'Fecha de Entrega:\s*(\d{4}-\d{2}-\d{2})', note_attrs)
    if fecha_match:
        result['fecha_entrega'] = fecha_match.group(1)
    
    return result

def parse_shopify_csv(content: str) -> list:
    reader = csv.DictReader(io.StringIO(content))
    orders = {}
    
    for row in reader:
        order_number = row.get('Name', '')
        if not order_number:
            continue
            
        if order_number not in orders:
            note_attrs = parse_note_attributes(row.get('Note Attributes', ''))
            created_at = None
            if row.get('Created at'):
                try:
                    created_at = datetime.strptime(row['Created at'].split(' -')[0].split(' +')[0], '%Y-%m-%d %H:%M:%S')
                except:
                    pass
            
            orders[order_number] = {
                'order_number': order_number,
                'email': row.get('Email', ''),
                'comuna': note_attrs['comuna'],
                'fecha_entrega': note_attrs['fecha_entrega'],
                'nombre_cliente': row.get('Shipping Name', '') or row.get('Billing Name', ''),
                'direccion': row.get('Shipping Address1', ''),
                'telefono': row.get('Phone', '') or row.get('Shipping Phone', ''),
                'total': float(row.get('Total', 0) or 0),
                'created_at': created_at,
                'items': []
            }
        
        if row.get('Lineitem name'):
            orders[order_number]['items'].append({
                'producto': row['Lineitem name'],
                'cantidad': int(row.get('Lineitem quantity', 1) or 1),
                'precio': float(row.get('Lineitem price', 0) or 0),
                'sku': row.get('Lineitem sku', '')
            })
    
    return list(orders.values())

def get_config(clave: str) -> str:
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT valor FROM configuracion WHERE clave = ?", (clave,))
    row = cursor.fetchone()
    conn.close()
    return row[0] if row else ''

def set_config(clave: str, valor: str):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("INSERT OR REPLACE INTO configuracion (clave, valor) VALUES (?, ?)", (clave, valor))
    conn.commit()
    conn.close()

# ============================================
# SISTEMA DE BACKUP
# ============================================

def generar_backup_excel() -> Path:
    conn = get_db()
    cursor = conn.cursor()
    
    wb = Workbook()
    header_fill = PatternFill(start_color="2E5C46", end_color="2E5C46", fill_type="solid")
    header_font = Font(bold=True, color="FFFFFF")
    
    # Hoja Pedidos
    ws = wb.active
    ws.title = "Pedidos"
    ws.append(['ID', 'Número Orden', 'Email', 'Cliente', 'Comuna', 'Dirección', 'Teléfono', 'Fecha Entrega', 'Fecha Original', 'Total', 'Status', 'Creado'])
    for cell in ws[1]:
        cell.fill = header_fill
        cell.font = header_font
    
    cursor.execute("SELECT * FROM pedidos ORDER BY fecha_entrega")
    for row in cursor.fetchall():
        ws.append([row['id'], row['order_number'], row['email'], row['nombre_cliente'], row['comuna'], 
                   row['direccion'], row['telefono'], row['fecha_entrega'], row['fecha_original'], 
                   row['total'], row['status'], str(row['created_at']) if row['created_at'] else ''])
    
    # Hoja Líneas
    ws2 = wb.create_sheet("Líneas de Pedido")
    ws2.append(['Pedido ID', 'Número Orden', 'Producto', 'Cantidad', 'Precio', 'SKU'])
    for cell in ws2[1]:
        cell.fill = header_fill
        cell.font = header_font
    
    cursor.execute('SELECT lp.pedido_id, p.order_number, lp.producto, lp.cantidad, lp.precio, lp.sku FROM lineas_pedido lp JOIN pedidos p ON lp.pedido_id = p.id')
    for row in cursor.fetchall():
        ws2.append(list(row))
    
    # Hoja Categorías
    ws3 = wb.create_sheet("Categorías")
    ws3.append(['ID', 'Nombre', 'Orden'])
    for cell in ws3[1]:
        cell.fill = header_fill
        cell.font = header_font
    cursor.execute("SELECT * FROM categorias ORDER BY orden")
    for row in cursor.fetchall():
        ws3.append([row['id'], row['nombre'], row['orden']])
    
    # Hoja Producto-Categoría
    ws4 = wb.create_sheet("Producto-Categoría")
    ws4.append(['Producto', 'Categoría'])
    for cell in ws4[1]:
        cell.fill = header_fill
        cell.font = header_font
    cursor.execute('SELECT pc.producto, c.nombre FROM producto_categoria pc JOIN categorias c ON pc.categoria_id = c.id')
    for row in cursor.fetchall():
        ws4.append(list(row))
    
    # Hoja Resumen
    ws5 = wb.create_sheet("Resumen")
    cursor.execute("SELECT COUNT(*) FROM pedidos WHERE status = 'pendiente'")
    pendientes = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM pedidos WHERE status = 'postergado'")
    postergados = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM pedidos WHERE status = 'completado'")
    completados = cursor.fetchone()[0]
    
    ws5.append(['Backup Sistema La Vega'])
    ws5.append(['Generado:', datetime.now().strftime('%Y-%m-%d %H:%M:%S')])
    ws5.append([])
    ws5.append(['Pedidos pendientes:', pendientes])
    ws5.append(['Pedidos postergados:', postergados])
    ws5.append(['Pedidos completados:', completados])
    ws5.append(['Total:', pendientes + postergados + completados])
    ws5.append([])
    ws5.append(['Desarrollado por Flipit.media'])
    
    conn.close()
    
    filepath = OUTPUT_DIR / f"backup_vega_{datetime.now().strftime('%Y-%m-%d_%H%M')}.xlsx"
    wb.save(filepath)
    return filepath

def enviar_backup_email(filepath: Path):
    email_dest = get_config('backup_email')
    smtp_host = get_config('smtp_host')
    smtp_port = int(get_config('smtp_port') or 587)
    smtp_user = get_config('smtp_user')
    smtp_pass = get_config('smtp_pass')
    
    if not all([email_dest, smtp_user, smtp_pass]):
        return False
    
    try:
        msg = MIMEMultipart()
        msg['From'] = smtp_user
        msg['To'] = email_dest
        msg['Subject'] = f'Backup Sistema La Vega - {datetime.now().strftime("%Y-%m-%d")}'
        
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM pedidos WHERE status = 'pendiente'")
        pendientes = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM pedidos WHERE status = 'postergado'")
        postergados = cursor.fetchone()[0]
        conn.close()
        
        body = f"""Backup automático del Sistema La Vega

Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M')}

Resumen:
• Pedidos pendientes: {pendientes}
• Pedidos postergados: {postergados}

El archivo de backup está adjunto.

---
Diseñado e implementado por Flipit.media"""
        
        msg.attach(MIMEText(body, 'plain'))
        
        with open(filepath, 'rb') as f:
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(f.read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', f'attachment; filename={filepath.name}')
            msg.attach(part)
        
        server = smtplib.SMTP(smtp_host, smtp_port)
        server.starttls()
        server.login(smtp_user, smtp_pass)
        server.send_message(msg)
        server.quit()
        return True
    except Exception as e:
        print(f"Error enviando backup: {e}")
        return False

# ============================================
# RUTAS PRINCIPALES
# ============================================

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    conn = get_db()
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM pedidos WHERE status = 'pendiente'")
    pedidos_pendientes = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM pedidos WHERE status = 'postergado'")
    pedidos_postergados = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(DISTINCT fecha_entrega) FROM pedidos WHERE status IN ('pendiente', 'postergado')")
    fechas_pendientes = cursor.fetchone()[0]
    
    hoy = date.today().isoformat()
    cursor.execute("SELECT COUNT(*) FROM pedidos WHERE fecha_entrega = ? AND status IN ('pendiente', 'postergado')", (hoy,))
    pedidos_hoy = cursor.fetchone()[0]
    
    cursor.execute('''SELECT COUNT(DISTINCT lp.producto) FROM lineas_pedido lp
                      LEFT JOIN producto_categoria pc ON lp.producto = pc.producto WHERE pc.id IS NULL''')
    sin_categoria = cursor.fetchone()[0]
    
    conn.close()
    
    return templates.TemplateResponse("index.html", {
        "request": request,
        "pedidos_pendientes": pedidos_pendientes,
        "pedidos_postergados": pedidos_postergados,
        "fechas_pendientes": fechas_pendientes,
        "pedidos_hoy": pedidos_hoy,
        "sin_categoria": sin_categoria,
        "fecha_hoy": hoy,
        "backup_email": get_config('backup_email'),
        "backup_frecuencia": get_config('backup_frecuencia'),
    })

@app.post("/upload")
async def upload_csv(file: UploadFile = File(...)):
    if not file.filename.endswith('.csv'):
        raise HTTPException(400, "El archivo debe ser CSV")
    
    content = await file.read()
    content = content.decode('utf-8-sig')
    orders = parse_shopify_csv(content)
    
    conn = get_db()
    cursor = conn.cursor()
    
    nuevos = 0
    duplicados = 0
    sin_fecha = 0
    
    for order in orders:
        cursor.execute("SELECT id FROM pedidos WHERE order_number = ?", (order['order_number'],))
        if cursor.fetchone():
            duplicados += 1
            continue
        
        if not order['fecha_entrega']:
            sin_fecha += 1
            continue
        
        cursor.execute('''INSERT INTO pedidos (order_number, email, comuna, fecha_entrega, fecha_original, direccion, telefono, nombre_cliente, total, created_at)
                          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                       (order['order_number'], order['email'], order['comuna'], order['fecha_entrega'], order['fecha_entrega'],
                        order['direccion'], order['telefono'], order['nombre_cliente'], order['total'], order['created_at']))
        
        pedido_id = cursor.lastrowid
        for item in order['items']:
            cursor.execute('INSERT INTO lineas_pedido (pedido_id, producto, cantidad, precio, sku) VALUES (?, ?, ?, ?, ?)',
                          (pedido_id, item['producto'], item['cantidad'], item['precio'], item['sku']))
        nuevos += 1
    
    conn.commit()
    conn.close()
    
    return {"success": True, "nuevos": nuevos, "duplicados": duplicados, "sin_fecha": sin_fecha}

@app.get("/api/categorias")
async def get_categorias():
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT id, nombre, orden FROM categorias ORDER BY orden")
    categorias = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return categorias

@app.post("/api/categorias")
async def create_categoria(nombre: str = Form(...)):
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT MAX(orden) FROM categorias")
        max_orden = cursor.fetchone()[0] or 0
        cursor.execute("INSERT INTO categorias (nombre, orden) VALUES (?, ?)", (nombre, max_orden + 1))
        conn.commit()
        return {"success": True, "id": cursor.lastrowid}
    except sqlite3.IntegrityError:
        raise HTTPException(400, "La categoría ya existe")
    finally:
        conn.close()

@app.get("/api/productos-sin-categoria")
async def get_productos_sin_categoria():
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute('''SELECT DISTINCT lp.producto FROM lineas_pedido lp
                      LEFT JOIN producto_categoria pc ON lp.producto = pc.producto
                      WHERE pc.id IS NULL ORDER BY lp.producto''')
    productos = [row[0] for row in cursor.fetchall()]
    conn.close()
    return productos

@app.post("/api/asignar-categoria")
async def asignar_categoria(producto: str = Form(...), categoria_id: int = Form(...)):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute('INSERT OR REPLACE INTO producto_categoria (producto, categoria_id) VALUES (?, ?)', (producto, categoria_id))
    conn.commit()
    conn.close()
    return {"success": True}

@app.get("/api/pedidos")
async def get_pedidos(fecha: Optional[str] = None, status: Optional[str] = None):
    conn = get_db()
    cursor = conn.cursor()
    
    query = "SELECT * FROM pedidos WHERE 1=1"
    params = []
    
    if fecha:
        query += " AND fecha_entrega = ?"
        params.append(fecha)
    
    if status:
        if status == 'activos':
            query += " AND status IN ('pendiente', 'postergado')"
        else:
            query += " AND status = ?"
            params.append(status)
    
    query += " ORDER BY fecha_entrega, order_number"
    cursor.execute(query, params)
    pedidos = [dict(row) for row in cursor.fetchall()]
    
    for pedido in pedidos:
        cursor.execute("SELECT * FROM lineas_pedido WHERE pedido_id = ?", (pedido['id'],))
        pedido['items'] = [dict(row) for row in cursor.fetchall()]
    
    conn.close()
    return pedidos

@app.get("/api/fechas-pendientes")
async def get_fechas_pendientes():
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute('''SELECT fecha_entrega, COUNT(*) as cantidad,
                      SUM(CASE WHEN status = 'postergado' THEN 1 ELSE 0 END) as postergados
                      FROM pedidos WHERE status IN ('pendiente', 'postergado')
                      GROUP BY fecha_entrega ORDER BY fecha_entrega''')
    fechas = [{"fecha": row[0], "cantidad": row[1], "postergados": row[2]} for row in cursor.fetchall()]
    conn.close()
    return fechas

@app.get("/api/lista-compras/{fecha}")
async def get_lista_compras(fecha: str):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute('''SELECT lp.producto, SUM(lp.cantidad) as cantidad_total,
                      COALESCE(c.nombre, 'Sin Categoría') as categoria, COALESCE(c.orden, 999) as categoria_orden
                      FROM lineas_pedido lp JOIN pedidos p ON lp.pedido_id = p.id
                      LEFT JOIN producto_categoria pc ON lp.producto = pc.producto
                      LEFT JOIN categorias c ON pc.categoria_id = c.id
                      WHERE p.fecha_entrega = ? AND p.status IN ('pendiente', 'postergado')
                      GROUP BY lp.producto ORDER BY categoria_orden, lp.producto''', (fecha,))
    
    items = [dict(row) for row in cursor.fetchall()]
    conn.close()
    
    por_categoria = {}
    for item in items:
        cat = item['categoria']
        if cat not in por_categoria:
            por_categoria[cat] = []
        por_categoria[cat].append({'producto': item['producto'], 'cantidad': item['cantidad_total']})
    
    return por_categoria

@app.get("/descargar/lista-compras/{fecha}")
async def descargar_lista_compras(fecha: str):
    lista = await get_lista_compras(fecha)
    
    wb = Workbook()
    ws = wb.active
    ws.title = "Lista de Compras"
    
    header_fill = PatternFill(start_color="2E5C46", end_color="2E5C46", fill_type="solid")
    header_font = Font(bold=True, color="FFFFFF", size=12)
    cat_fill = PatternFill(start_color="E8F5E9", end_color="E8F5E9", fill_type="solid")
    border = Border(left=Side(style='thin'), right=Side(style='thin'), top=Side(style='thin'), bottom=Side(style='thin'))
    
    ws.merge_cells('A1:C1')
    ws['A1'] = f"Lista de Compras - {fecha}"
    ws['A1'].font = Font(bold=True, size=14)
    ws['A1'].alignment = Alignment(horizontal='center')
    
    for col, header in enumerate(['Producto', 'Cantidad', '✓'], 1):
        cell = ws.cell(row=3, column=col, value=header)
        cell.font = header_font
        cell.fill = header_fill
        cell.border = border
        cell.alignment = Alignment(horizontal='center')
    
    row = 4
    for categoria, productos in lista.items():
        ws.merge_cells(f'A{row}:C{row}')
        ws[f'A{row}'] = categoria
        ws[f'A{row}'].font = Font(bold=True)
        ws[f'A{row}'].fill = cat_fill
        row += 1
        
        for prod in productos:
            ws.cell(row=row, column=1, value=prod['producto']).border = border
            ws.cell(row=row, column=2, value=prod['cantidad']).border = border
            ws.cell(row=row, column=2).alignment = Alignment(horizontal='center')
            ws.cell(row=row, column=3, value="☐").border = border
            ws.cell(row=row, column=3).alignment = Alignment(horizontal='center')
            row += 1
    
    ws.column_dimensions['A'].width = 50
    ws.column_dimensions['B'].width = 12
    ws.column_dimensions['C'].width = 8
    
    filepath = OUTPUT_DIR / f"lista_compras_{fecha}.xlsx"
    wb.save(filepath)
    
    return FileResponse(filepath, filename=filepath.name, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

@app.get("/descargar/pedidos-armado/{fecha}")
async def descargar_pedidos_armado(fecha: str):
    pedidos = await get_pedidos(fecha=fecha, status='activos')
    
    wb = Workbook()
    ws = wb.active
    ws.title = "Pedidos para Armar"
    
    header_fill = PatternFill(start_color="2E5C46", end_color="2E5C46", fill_type="solid")
    header_font = Font(bold=True, color="FFFFFF")
    order_fill = PatternFill(start_color="E8F5E9", end_color="E8F5E9", fill_type="solid")
    postergado_fill = PatternFill(start_color="FFF3E0", end_color="FFF3E0", fill_type="solid")
    border = Border(left=Side(style='thin'), right=Side(style='thin'), top=Side(style='thin'), bottom=Side(style='thin'))
    
    ws.merge_cells('A1:C1')
    ws['A1'] = f"Pedidos para Armar - {fecha}"
    ws['A1'].font = Font(bold=True, size=14)
    ws['A2'] = f"Total: {len(pedidos)} pedidos"
    
    row = 4
    for pedido in pedidos:
        ws.merge_cells(f'A{row}:C{row}')
        status_text = " [POSTERGADO]" if pedido['status'] == 'postergado' else ""
        ws[f'A{row}'] = f"{pedido['order_number']} | {pedido['nombre_cliente']} | {pedido['comuna']}{status_text}"
        ws[f'A{row}'].font = Font(bold=True)
        ws[f'A{row}'].fill = postergado_fill if pedido['status'] == 'postergado' else order_fill
        row += 1
        
        if pedido['direccion']:
            ws[f'A{row}'] = f"📍 {pedido['direccion']}"
            row += 1
        
        for col, header in enumerate(['Producto', 'Cant.', '✓'], 1):
            cell = ws.cell(row=row, column=col, value=header)
            cell.font = header_font
            cell.fill = header_fill
            cell.border = border
        row += 1
        
        for item in pedido['items']:
            ws.cell(row=row, column=1, value=item['producto']).border = border
            ws.cell(row=row, column=2, value=item['cantidad']).border = border
            ws.cell(row=row, column=2).alignment = Alignment(horizontal='center')
            ws.cell(row=row, column=3, value="☐").border = border
            row += 1
        row += 1
    
    ws.column_dimensions['A'].width = 50
    ws.column_dimensions['B'].width = 8
    ws.column_dimensions['C'].width = 6
    
    filepath = OUTPUT_DIR / f"pedidos_armado_{fecha}.xlsx"
    wb.save(filepath)
    
    return FileResponse(filepath, filename=filepath.name, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

@app.post("/api/pedidos/{pedido_id}/completar")
async def completar_pedido(pedido_id: int):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("UPDATE pedidos SET status = 'completado' WHERE id = ?", (pedido_id,))
    conn.commit()
    conn.close()
    return {"success": True}

@app.post("/api/pedidos/{pedido_id}/postergar")
async def postergar_pedido(pedido_id: int, nueva_fecha: str = Form(...)):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("UPDATE pedidos SET fecha_entrega = ?, status = 'postergado' WHERE id = ?", (nueva_fecha, pedido_id))
    conn.commit()
    conn.close()
    return {"success": True}

@app.delete("/api/pedidos/{pedido_id}")
async def eliminar_pedido(pedido_id: int):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM lineas_pedido WHERE pedido_id = ?", (pedido_id,))
    cursor.execute("DELETE FROM pedidos WHERE id = ?", (pedido_id,))
    conn.commit()
    conn.close()
    return {"success": True}

# ============================================
# RUTAS DE BACKUP
# ============================================

@app.get("/descargar/backup")
async def descargar_backup():
    filepath = generar_backup_excel()
    return FileResponse(filepath, filename=filepath.name, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

@app.post("/api/backup/enviar")
async def enviar_backup(background_tasks: BackgroundTasks):
    filepath = generar_backup_excel()
    background_tasks.add_task(enviar_backup_email, filepath)
    return {"success": True, "message": "Enviando backup por email..."}

@app.post("/api/backup/config")
async def guardar_config_backup(email: str = Form(''), frecuencia: str = Form('3'), smtp_user: str = Form(''), smtp_pass: str = Form('')):
    set_config('backup_email', email)
    set_config('backup_frecuencia', frecuencia)
    if smtp_user:
        set_config('smtp_user', smtp_user)
    if smtp_pass:
        set_config('smtp_pass', smtp_pass)
    return {"success": True}

@app.post("/restaurar/backup")
async def restaurar_backup(file: UploadFile = File(...)):
    if not file.filename.endswith('.xlsx'):
        raise HTTPException(400, "El archivo debe ser Excel (.xlsx)")
    
    content = await file.read()
    wb = load_workbook(io.BytesIO(content))
    
    conn = get_db()
    cursor = conn.cursor()
    hoy = date.today().isoformat()
    
    restaurados = 0
    completados_auto = 0
    
    # Restaurar categorías
    if "Categorías" in wb.sheetnames:
        for row in wb["Categorías"].iter_rows(min_row=2, values_only=True):
            if row[1]:
                cursor.execute('INSERT OR IGNORE INTO categorias (nombre, orden) VALUES (?, ?)', (row[1], row[2] or 99))
    
    # Restaurar producto-categoría
    if "Producto-Categoría" in wb.sheetnames:
        for row in wb["Producto-Categoría"].iter_rows(min_row=2, values_only=True):
            if row[0] and row[1]:
                cursor.execute("SELECT id FROM categorias WHERE nombre = ?", (row[1],))
                cat = cursor.fetchone()
                if cat:
                    cursor.execute('INSERT OR IGNORE INTO producto_categoria (producto, categoria_id) VALUES (?, ?)', (row[0], cat[0]))
    
    # Restaurar pedidos
    if "Pedidos" in wb.sheetnames:
        for row in wb["Pedidos"].iter_rows(min_row=2, values_only=True):
            if not row[1]:
                continue
            
            cursor.execute("SELECT id FROM pedidos WHERE order_number = ?", (row[1],))
            if cursor.fetchone():
                continue
            
            fecha_entrega = str(row[7]) if row[7] else None
            status = row[10] or 'pendiente'
            
            if fecha_entrega and fecha_entrega < hoy and status == 'pendiente':
                status = 'completado'
                completados_auto += 1
            
            cursor.execute('''INSERT INTO pedidos (order_number, email, nombre_cliente, comuna, direccion, telefono, fecha_entrega, fecha_original, total, status, created_at)
                              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                          (row[1], row[2], row[3], row[4], row[5], row[6], fecha_entrega, row[8], row[9], status, row[11]))
            restaurados += 1
    
    # Restaurar líneas
    if "Líneas de Pedido" in wb.sheetnames:
        for row in wb["Líneas de Pedido"].iter_rows(min_row=2, values_only=True):
            if not row[1]:
                continue
            cursor.execute("SELECT id FROM pedidos WHERE order_number = ?", (row[1],))
            pedido = cursor.fetchone()
            if pedido:
                cursor.execute('INSERT INTO lineas_pedido (pedido_id, producto, cantidad, precio, sku) VALUES (?, ?, ?, ?, ?)',
                              (pedido[0], row[2], row[3], row[4], row[5]))
    
    conn.commit()
    conn.close()
    
    return {"success": True, "restaurados": restaurados, "completados_auto": completados_auto}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
