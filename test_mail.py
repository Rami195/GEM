#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, re, smtplib, unicodedata, argparse
from datetime import date, timedelta
from email.mime.text import MIMEText
from email.utils import formatdate
from html import escape
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

# =============== CONFIG ===============
# ⚠️ URL por defecto (podés pasar --url en CLI o usar secret URL_TABLA en Actions)
DEFAULT_URL = "https://educacionales.mendoza.edu.ar/"

TABLE_WAIT_MS = 30_000
AFTER_FILTER_WAIT_MS = 4_000
MAX_PAGES = 200  # cortafuegos anti-loop

# Columnas preferidas para mostrar (si existen)
PREFERRED_COLS = [
    "Llamado","Nivel","Departamento","Localidad",
    "Escuela","Cargo","Horas","Turno","Materia","Publicado"
]

# Departamentos válidos (normalizados SIN acento y en mayúsculas)
ALLOWED_DEPTOS = {"SAN MARTIN","JUNIN","RIVADAVIA","CAPITAL","GODOY CRUZ","MAIPU","GUAYMALLEN"}

# Bloqueos por materia/cargo (normalizados SIN acento y en mayúsculas)
# Coincide con LENGUA (incluye “LENGUA EXTRANJERA”), QUIMICA/QUÍMICA, PRECEPTOR/ES
BLOCK_PATTERNS =("POLITICA","AMBIENTALES","MICROEMPRENDIMIENTOS","ARTISTICA","LENGUA", "QUIMICA", "PRECEPT", "ORIENTADOR PSICOPEDAGOGICO","CIENCIAS SOCIALES","EDUCACION FISICA","BIOLOGIA", "FORMACION PARA LA VIDA Y EL TRABAJO","RECURSOS TURISTICOS","TEATRO","SOCIAL","MARCO JURIDICO","TURISMO","CONTABLE","REGENTE","VICEDIRECTOR","DIRECTOR")  # coincide con LENGUA EXTRANJERA, QUIMICA, PRECEPTOR/ES

# =============== EMAIL (.env) ===============
load_dotenv()

def getenv_stripped(name, default=None):
    v = os.getenv(name, default)
    return v.strip() if isinstance(v, str) else v

MAIL_HOST = getenv_stripped("MAIL_HOST")
MAIL_PORT = int(getenv_stripped("MAIL_PORT", "465"))
MAIL_USER = getenv_stripped("MAIL_USER")
MAIL_PASS = getenv_stripped("MAIL_PASS")
MAIL_TO   = getenv_stripped("MAIL_TO")

def send_email_html(subject: str, html_body: str):
    miss = [k for k,v in [("MAIL_HOST",MAIL_HOST),("MAIL_PORT",MAIL_PORT),("MAIL_USER",MAIL_USER),
                          ("MAIL_PASS",MAIL_PASS),("MAIL_TO",MAIL_TO)] if not v]
    if miss:
        raise RuntimeError(f"Faltan variables en .env: {', '.join(miss)}")
    if "=" in MAIL_HOST:  # error típico cuando se mete "MAIL_HOST=smtp..."
        raise RuntimeError(f"MAIL_HOST luce mal: {MAIL_HOST!r}. Debe ser solo 'smtp.gmail.com'.")
    msg = MIMEText(html_body, "html", "utf-8")
    msg["Subject"] = subject
    msg["From"] = MAIL_USER
    msg["To"] = MAIL_TO
    msg["Date"] = formatdate(localtime=True)
    with smtplib.SMTP_SSL(MAIL_HOST, MAIL_PORT) as s:
        s.login(MAIL_USER, MAIL_PASS)
        s.sendmail(MAIL_USER, [MAIL_TO], msg.as_string())

# =============== HELPERS ===============
def normalize(s: str) -> str:
    s = (s or "").strip()
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    return s.upper()

def parse_publicado_to_date(s: str):
    """Extrae dd/mm/(yy|yyyy) y devuelve date()."""
    m = re.search(r"(\d{2})/(\d{2})/(\d{2,4})", s or "")
    if not m: return None
    d, mth, y = map(int, m.groups())
    if y < 100: y += 2000
    try:
        return date(y, mth, d)
    except ValueError:
        return None

def _try_global_search(page, query: str):
    for sel in ("input[type='search']",
                "input[placeholder*='Buscar']",
                "input[aria-label*='Buscar']"):
        el = page.query_selector(sel)
        if el:
            el.fill("")
            el.type(query)
            page.wait_for_timeout(AFTER_FILTER_WAIT_MS)
            return True
    return False

def _get_headers(page):
    page.wait_for_selector("table thead", timeout=TABLE_WAIT_MS)
    headers = page.eval_on_selector_all(
        "table thead tr:first-child th",
        "nodes => nodes.map(n => (n.innerText || n.textContent || '').trim())"
    )
    if not headers:
        raise RuntimeError("No se detectaron encabezados de tabla.")
    return headers

def _find_col(headers, name_part):
    name_part = name_part.lower()
    for i, h in enumerate(headers):
        if name_part in h.lower():
            return i
    return None

def _select_page_length(page, debug=False):
    # 1) Intento con API de DataTables
    ok = page.evaluate("""
        () => {
          const $ = window.jQuery || window.$;
          if (!$) return false;
          const t = $('table').DataTable ? $('table').DataTable() : null;
          if (!t) return false;
          t.page.len(100).draw('page');
          return true;
        }
    """)
    if ok:
        if debug: print("· Usando DataTables API para len=100")
        page.wait_for_timeout(1200)
        return
    # 2) Fallback: <select>
    for sel in ("select[name$='_length']", ".dataTables_length select", "select"):
        if page.query_selector(sel):
            try:
                page.select_option(sel, "100")
                if debug: print("· Usando <select> para len=100")
                page.wait_for_timeout(1200)
                return
            except Exception:
                pass
    if debug: print("· No se pudo forzar len=100 (seguimos con default)")

def _is_blocked(materia_norm: str, cargo_norm: str) -> bool:
    return any(p in materia_norm for p in BLOCK_PATTERNS) or any(p in cargo_norm for p in BLOCK_PATTERNS)

def _collect_rows_current_page(page, headers, idx_nivel, idx_depto, idx_publi, idx_materia, idx_cargo,
                               today, yesterday, stop_threshold):
    """
    Devuelve (filas_match, stop_old):
      - filas_match: filas de la página ACTUAL que pasan filtros.
      - stop_old: True si se detecta una fila con Publicado <= stop_threshold (2 días o más),
                  y se detiene la búsqueda/paginación.
    """
    out, stop_old = [], False
    try:
        page.wait_for_selector("table tbody tr:first-child td", timeout=10_000)
    except PWTimeout:
        return out, False  # no hay filas visibles

    rows = page.query_selector_all("table tbody tr")
    for r in rows:
        tds = r.query_selector_all("td")
        if not tds:
            continue
        values = [((c.text_content()) or "").strip() for c in tds]
        row = { (headers[i] if i < len(headers) else f"col_{i+1}"): v
                for i, v in enumerate(values) }

        # 1) Corte anticipado por antigüedad (2 días o más)
        publi_dt = parse_publicado_to_date(row.get(headers[idx_publi], ""))
        if publi_dt and publi_dt <= stop_threshold:
            stop_old = True
            break  # resto será igual o más antiguo (orden desc)

        # 2) Filtros de negocio
        if "secundario" not in (row.get(headers[idx_nivel], "").lower()):
            continue
        if normalize(row.get(headers[idx_depto], "")) not in ALLOWED_DEPTOS:
            continue
        if not publi_dt or (publi_dt != today and publi_dt != yesterday):
            continue

        # 3) Exclusión por materia/cargo
        materia_norm = normalize(row.get(headers[idx_materia], "")) if idx_materia is not None else ""
        cargo_norm   = normalize(row.get(headers[idx_cargo], "")) if idx_cargo   is not None else ""
        if _is_blocked(materia_norm, cargo_norm):
            continue

        out.append(row)
    return out, stop_old

def _paginate_info(page):
    """Intenta obtener info de DataTables; si no hay, devuelve None."""
    return page.evaluate("""
        () => {
          const $ = window.jQuery || window.$;
          if (!$) return null;
          const t = $('table').DataTable ? $('table').DataTable() : null;
          if (!t) return null;
          const i = t.page.info();
          return {page:i.page, pages:i.pages, length:i.length, records:(i.recordsDisplay ?? i.recordsTotal)};
        }
    """)

def _goto_next_datatables_page(page, current_page, debug=False):
    ok = page.evaluate("""
        (curr) => {
          const $ = window.jQuery || window.$;
          if (!$) return false;
          const t = $('table').DataTable ? $('table').DataTable() : null;
          if (!t) return false;
          t.page('next').draw('page');
          return true;
        }
    """, current_page)
    if not ok:
        return False
    # Espera cambio de índice de página (hasta ~10s)
    for _ in range(50):
        page.wait_for_timeout(200)
        newp = page.evaluate("""
            () => {
              const $ = window.jQuery || window.$;
              if (!$) return -1;
              const t = $('table').DataTable ? $('table').DataTable() : null;
              if (!t) return -1;
              return t.page.info().page;
            }
        """)
        if newp != -1 and newp != current_page:
            if debug: print(f"· Página cambiada: {current_page} -> {newp}")
            return True
    if debug: print("· No cambió la página (posible última)")
    return False

def _goto_next_by_click(page, debug=False):
    nxt = page.query_selector("a.paginate_button.next, .dataTables_paginate a.next, a:has-text('Siguiente')")
    if not nxt:
        if debug: print("· No hay botón Siguiente")
        return False
    cls = (nxt.get_attribute("class") or "")
    aria_dis = (nxt.get_attribute("aria-disabled") or "")
    if "disabled" in cls or aria_dis == "true":
        if debug: print("· Siguiente deshabilitado")
        return False

    before = page.eval_on_selector("table tbody tr:first-child", "el => el ? el.innerText : ''")
    nxt.click()
    # Espera activa hasta ~10s a que cambie la primera fila
    for _ in range(50):
        page.wait_for_timeout(200)
        after = page.eval_on_selector("table tbody tr:first-child", "el => el ? el.innerText : ''")
        if after and after != before:
            if debug: print("· Cambió la primera fila (avanzó página)")
            return True
    if debug: print("· La primera fila no cambió (asumo última)")
    return False

# =============== SCRAPER ===============
def scrape_all_pages(url: str, headful=False, debug=False, use_chromium=False):
    today = date.today()
    yesterday = today - timedelta(days=1)
    stop_threshold = today - timedelta(days=2)  # ← corte anticipado (2 días o más)

    with sync_playwright() as p:
        browser = (p.chromium if use_chromium else p.firefox).launch(headless=not headful)
        page = browser.new_page()
        page.goto(url, timeout=TABLE_WAIT_MS)

        # Esperar la tabla
        page.wait_for_selector("table", timeout=TABLE_WAIT_MS)
        headers = _get_headers(page)

        # Índices
        idx_nivel   = _find_col(headers, "nivel")
        idx_depto   = _find_col(headers, "depart")   # "Departamento"
        idx_publi   = _find_col(headers, "public")   # "Publicado"
        idx_materia = _find_col(headers, "mater")    # "Materia"
        idx_cargo   = _find_col(headers, "cargo")    # "Cargo"
        if None in (idx_nivel, idx_depto, idx_publi):
            browser.close()
            raise RuntimeError(f"No se ubicaron columnas necesarias. Headers: {headers}")

        # Filtrar Nivel = Secundario
        filtro_col = f"table thead tr:nth-of-type(2) th:nth-of-type({idx_nivel+1}) input"
        if page.query_selector("table thead tr:nth-of-type(2)") and page.query_selector(filtro_col):
            page.fill(filtro_col, "Secundario")
            if debug: print("· Filtro por columna (Nivel=Secundario)")
            page.wait_for_timeout(AFTER_FILTER_WAIT_MS)
        else:
            _try_global_search(page, "Secundario")
            if debug: print("· Filtro por buscador global (Secundario)")

        _select_page_length(page, debug=debug)

        all_matches = []
        stop_everything = False

        # Si hay DataTables, usar su API
        info = _paginate_info(page)
        if debug and info:
            print(f"· DT info: page={info['page']} pages={info['pages']} length={info['length']} records={info['records']}")
        if info and info["pages"] > 0:
            curr = info["page"]
            pages = min(info["pages"], MAX_PAGES)
            for _ in range(pages):
                page_rows, stop_old = _collect_rows_current_page(
                    page, headers, idx_nivel, idx_depto, idx_publi, idx_materia, idx_cargo,
                    today, yesterday, stop_threshold
                )
                all_matches += page_rows
                if stop_old:
                    if debug: print("· Se detectó fila de hace ≥2 días. Corte anticipado.")
                    stop_everything = True
                    break
                if not _goto_next_datatables_page(page, curr, debug=debug):
                    break
                curr = page.evaluate("""
                    () => {
                      const $ = window.jQuery || window.$;
                      if (!$) return -1;
                      const t = $('table').DataTable ? $('table').DataTable() : null;
                      if (!t) return -1;
                      return t.page.info().page;
                    }
                """)
        else:
            # Fallback: click en "Siguiente"
            page_count = 0
            while page_count < MAX_PAGES and not stop_everything:
                page_rows, stop_old = _collect_rows_current_page(
                    page, headers, idx_nivel, idx_depto, idx_publi, idx_materia, idx_cargo,
                    today, yesterday, stop_threshold
                )
                all_matches += page_rows
                if stop_old:
                    if debug: print("· Se detectó fila de hace ≥2 días. Corte anticipado.")
                    break
                if not _goto_next_by_click(page, debug=debug):
                    break
                page_count += 1

        browser.close()
        return headers, all_matches, today, yesterday

# ---------- Salidas ----------
def rows_to_text(headers, rows):
    name_map = {h.lower(): h for h in headers}
    chosen = [name_map[c.lower()] for c in PREFERRED_COLS if c.lower() in name_map] or headers
    lines = []
    for i, r in enumerate(rows, start=1):
        parts = [f"{h}: {r.get(h,'')}" for h in chosen]
        lines.append(f"{i:02d}) " + " | ".join(parts))
    return "\n".join(lines)

def rows_to_html(headers, rows, url, today, yesterday):
    name_map = {h.lower(): h for h in headers}
    chosen = [name_map[c.lower()] for c in PREFERRED_COLS if c.lower() in name_map] or headers

    css = """
    <style>
      body { font-family: system-ui,-apple-system,"Segoe UI",Roboto,"Helvetica Neue",Arial; color:#111; }
      .meta { margin: 0 0 12px 0; color:#444; }
      table { border-collapse: collapse; width: 100%; font-size: 14px; }
      th, td { border: 1px solid #e5e5e5; padding: 6px 8px; text-align: left; vertical-align: top; }
      thead th { background: #f6f8fa; position: sticky; top: 0; }
      tbody tr:nth-child(odd) { background: #fafafa; }
      .muted { color:#666; font-size:12px; }
    </style>
    """

    head = "".join(f"<th>{escape(h)}</th>" for h in chosen)
    body_rows = []
    for r in rows:
        tds = "".join(f"<td>{escape(r.get(h,'') or '')}</td>" for h in chosen)
        body_rows.append(f"<tr>{tds}</tr>")
    body = "\n".join(body_rows)

    return f"""
    <html>
      <head>{css}</head>
      <body>
        <p class="meta"><b>{len(rows)}</b> coincidencia(s). Filtros: Nivel=Secundario; Departamentos={", ".join(sorted(ALLOWED_DEPTOS))}; Publicado en hoy ({today:%d/%m/%Y}) o ayer ({yesterday:%d/%m/%Y}).</p>
        <table>
          <thead><tr>{head}</tr></thead>
          <tbody>{body}</tbody>
        </table>
        <p class="muted">Fuente: {escape(url)}</p>
      </body>
    </html>
    """

# =============== CLI ===============
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=DEFAULT_URL, help="URL de la tabla de avisos (NO la home)")
    ap.add_argument("--no-email", action="store_true", help="solo mostrar por consola; no enviar email")
    ap.add_argument("--debug", action="store_true", help="logs de depuración")
    ap.add_argument("--headful", action="store_true", help="abrir navegador visible para debug")
    ap.add_argument("--chromium", action="store_true", help="usar Chromium en vez de Firefox")
    args = ap.parse_args()

    try:
        headers, matches, today, yesterday = scrape_all_pages(
            args.url, headful=args.headful, debug=args.debug, use_chromium=args.chromium
        )
    except PWTimeout:
        print("⏳ Timeout al cargar/leer la tabla. Verificá que la URL sea la del LISTADO (no la home).")
        return
    except Exception as e:
        print(f"❌ Error: {e}")
        return

    if not matches:
        print("ℹ️ No hubo coincidencias con los filtros (Nivel=Secundario + dptos elegidos + Publicado hoy/ayer, excluyendo Lengua/Química/Preceptor).")
        return

    # Consola (texto) y correo (HTML)
    print(f"\n✅ {len(matches)} coincidencia(s) totales:\n")
    print(rows_to_text(headers, matches))

    if not args.no_email:
        subject = f"[Avisos] {len(matches)} coincidencias • Secundario • Hoy/Ayer • Deptos seleccionados"
        html = rows_to_html(headers, matches, args.url, today, yesterday)
        try:
            send_email_html(subject, html)
            print(f"\n📧 Email enviado a {MAIL_TO}.")
        except Exception as e:
            print(f"\n📭 No se pudo enviar el correo: {e}")

if __name__ == "__main__":
    main()
