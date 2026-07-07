# app.py
# Flask App – PostgreSQL/Supabase Version (Aufbau wie APP 9), Logik unverändert übernommen aus der SQLite-Version.
#
# Start:
#   export DATABASE_URL="postgresql://user:pass@host:5432/dbname?sslmode=require"
#   export SECRET_KEY="."
#   python app.py
#
from flask import Flask, render_template, render_template_string, request, redirect, url_for, session, jsonify, g
import os, uuid, re, io, json, glob, base64
from datetime import datetime
from zoneinfo import ZoneInfo
import calendar
from decimal import Decimal, ROUND_HALF_UP


def normalize_role(role: str) -> str:
    r = (role or "").strip().lower()
    # akzeptiere Anzeigenamen mit Leerzeichen
    if r in ["planner bbs", "planner_bbs"]:
        return "planner_bbs"
    if r in ["vorgesetzter cp", "vorgesetzter_cp"]:
        return "vorgesetzter_cp"
    return r




# --- Mail (Gmail App Password / SMTP) ---
import smtplib
from email.message import EmailMessage

# ---------------- SMTP Config ----------------
SMTP_HOST = os.environ.get("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.environ.get("SMTP_PORT", "587"))
SMTP_USER = os.environ.get("SMTP_USER", "")
SMTP_PASS = os.environ.get("SMTP_PASS", "")
MAIL_FROM = os.environ.get("MAIL_FROM", f"CV - Planung <{SMTP_USER}>")

def send_mail(to_addr: str, subject: str, body: str) -> None:
    """Send a plain text email via SMTP. No-op if config is missing."""
    to_addr = (to_addr or "").strip()
    if not to_addr:
        return
    if not (SMTP_HOST and SMTP_PORT and SMTP_USER and SMTP_PASS):
        return

    msg = EmailMessage()
    msg["From"] = MAIL_FROM
    msg["To"] = to_addr
    msg["Subject"] = subject
    msg.set_content(body)

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=20) as s:
        s.ehlo()
        s.starttls()
        s.login(SMTP_USER, SMTP_PASS)
        s.send_message(msg)

def _format_event_date(event_start_dt: str) -> str:
    date_de = "TT.MM.JJJJ"
    try:
        if isinstance(event_start_dt, str) and event_start_dt.strip():
            d = datetime.fromisoformat(event_start_dt.replace("Z", "").strip())
            date_de = d.strftime("%d.%m.%Y")
    except Exception:
        pass
    return date_de


def _format_event_time(event_start_dt: str, override_time: str = "") -> str:
    time_de = ""
    try:
        if isinstance(event_start_dt, str) and event_start_dt.strip():
            d = datetime.fromisoformat(event_start_dt.replace("Z", "").strip())
            time_de = d.strftime("%H:%M")
    except Exception:
        pass
    custom = (override_time or "").strip()
    return custom or time_de


def _event_info_lines(event_title: str, ort: str = "", dienstkleidung: str = "", start_time: str = "") -> list[str]:
    lines = [f"Auftrag: {(event_title or '').strip() or '-'}"]
    if start_time:
        lines.append(f"Startzeit: {start_time}")
    if ort:
        lines.append(f"Ort: {ort}")
    if dienstkleidung:
        lines.append(f"Dienstkleidung: {dienstkleidung}")
    return lines


def build_welcome_mail(employee_name: str, username: str, password: str) -> str:
    lines = [
        f"Hallo {employee_name},",
        "",
        "herzlich willkommen beim",
        "Casutt Veranstaltungsservice!",
        "",
        "Deine Zugangsdaten:",
        f"Benutzername: {username}",
        f"Passwort: {password}",
        "",
        "Hier geht es zur CV-Planung:",
        "https://cv-planung.onrender.com",
        "",
        "Wir freuen uns auf die Zusammenarbeit!",
        "",
        "Viele Grüße",
        "CV - Planung"
    ]
    return "\n".join(lines)


def build_change_mail(employee_name: str,
                      event_title: str,
                      event_start_dt: str,
                      ort: str,
                      dienstkleidung: str,
                      new_start_time: str,
                      new_remark: str = "") -> str:
    date_de = _format_event_date(event_start_dt)
    start_time = (new_start_time or "").strip()
    remark_line = (new_remark or "").strip()

    lines = [
        f"Hallo {employee_name},",
        "",
        f"es liegt eine Aktualisierung zu deinem Auftrag am {date_de} vor.",
        ""
    ]

    if start_time:
        lines.append(f"Neue Startzeit: {start_time} ✅")
    if remark_line:
        lines.append(f"Neue Bemerkung: {remark_line} ✅")

    lines.extend([
        "",
        *_event_info_lines(event_title, ort, dienstkleidung),
        "",
        "Bitte logge dich bei Bedarf ins Portal 'CV-Planung' ein, um die Details einzusehen.",
        "https://cv-planung.onrender.com/",
        "",
        "Viele Grüße",
        "CV - Planung"
    ])
    return "\n".join(lines)


def build_confirmation_mail(employee_name: str,
                            event_title: str,
                            event_start_dt: str,
                            ort: str,
                            dienstkleidung: str,
                            start_time: str = "") -> str:
    date_de = _format_event_date(event_start_dt)
    time_de = _format_event_time(event_start_dt, start_time)

    lines = [
        f"Hallo {employee_name},",
        "",
        f"du wurdest bei: {(event_title or '').strip() or '-'} am {date_de} bestätigt ✅",
        "",
        *_event_info_lines(event_title, ort, dienstkleidung, time_de),
        "",
        "Bitte logge dich bei Bedarf ins Portal 'CV-Planung' ein, um die Details einzusehen.",
        "https://cv-planung.onrender.com/",
        "",
        "Viele Grüße",
        "CV - Planung"
    ]
    return "\n".join(lines)


def build_assignment_mail(employee_name: str,
                          event_title: str,
                          event_start_dt: str,
                          ort: str,
                          dienstkleidung: str,
                          start_time: str = "") -> str:
    date_de = _format_event_date(event_start_dt)
    time_de = _format_event_time(event_start_dt, start_time)

    lines = [
        f"Hallo {employee_name},",
        "",
        f"du wurdest bei: {(event_title or '').strip() or '-'} am {date_de} zugewiesen ✅",
        "",
        *_event_info_lines(event_title, ort, dienstkleidung, time_de),
        "",
        "Bitte logge dich bei Bedarf ins Portal 'CV-Planung' ein, um die Details einzusehen.",
        "https://cv-planung.onrender.com/",
        "",
        "Viele Grüße",
        "CV - Planung"
    ]
    return "\n".join(lines)


def build_rejection_mail(employee_name: str,
                         event_title: str,
                         event_start_dt: str,
                         ort: str = "",
                         dienstkleidung: str = "") -> str:
    date_de = _format_event_date(event_start_dt)
    lines = [
        f"Hallo {employee_name},",
        "",
        f"du wurdest bei: {(event_title or '').strip() or '-'} am {date_de} abgewiesen ❌",
        "",
        "Bitte logge dich bei Bedarf ins Portal 'CV-Planung' ein, um die Details einzusehen.",
        "https://cv-planung.onrender.com/",
        "",
        "Viele Grüße",
        "CV - Planung"
    ]
    return "\n".join(lines)


def build_board_post_mail(employee_name: str, content: str, author: str = "") -> str:
    employee_name = (employee_name or "Mitarbeiter/in").strip() or "Mitarbeiter/in"
    author = (author or "Vorgesetzter").strip() or "Vorgesetzter"
    content = (content or "").strip()

    lines = [
        f"Hallo {employee_name},",
        "",
        "es gibt einen neuen Beitrag auf der Startseite der CV-Planung.",
        "",
        "Inhalt:",
        content or "-",
        "",
        f"Veröffentlicht von: {author}",
        "",
        "Bitte logge dich bei Bedarf ins Portal 'CV-Planung' ein, um die Details einzusehen.",
        "",
        "Viele Grüße",
        "CV - Planung",
    ]
    return "\n".join(lines)

import psycopg2
import psycopg2.extras
from psycopg2 import IntegrityError
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.utils import ImageReader
from reportlab.pdfbase.pdfmetrics import stringWidth
from reportlab.platypus import Table, TableStyle, Paragraph
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "geheimes_passwort")

# Supabase/PostgreSQL connection string
DATABASE_URL = os.environ.get("DATABASE_URL")


# ---------------- DB helpers (PostgreSQL / Supabase) ----------------
class DBWrapper:
    def __init__(self, conn):
        self.conn = conn

    def execute(self, sql, params=None):
        cur = self.conn.cursor()
        cur.execute(sql, params or ())
        return cur

    def commit(self):
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()

    def close(self):
        try:
            self.conn.close()
        except Exception:
            pass


def get_db():
    db = getattr(g, "_db", None)
    if db is None:
        if not DATABASE_URL:
            raise RuntimeError("DATABASE_URL ist nicht gesetzt (Supabase/PostgreSQL Verbindung fehlt).")

        connect_kwargs = {
            "dsn": DATABASE_URL,
            "cursor_factory": psycopg2.extras.RealDictCursor,
        }
        # Supabase verlangt i.d.R. SSL. Wenn sslmode nicht im URL steht, erzwingen wir require.
        if "sslmode=" not in (DATABASE_URL or ""):
            connect_kwargs["sslmode"] = "require"

        conn = psycopg2.connect(**connect_kwargs)
        db = g._db = DBWrapper(conn)
    return db


@app.teardown_appcontext
def close_db(exc):
    db = getattr(g, "_db", None)
    if db is not None:
        db.close()


@app.before_request
def update_current_user_activity():
    if "username" not in session:
        return
    if request.endpoint == "static":
        return
    # Schreibzugriffe begrenzen: maximal einmal pro Minute je Session aktualisieren.
    now = datetime.now(ZoneInfo("Europe/Berlin"))
    last = session.get("last_activity_write")
    try:
        if last and (now - datetime.fromisoformat(last)).total_seconds() < 60:
            return
    except Exception:
        pass
    try:
        db = get_db()
        db.execute("UPDATE users SET last_activity_at=%s WHERE username=%s", (now.strftime("%Y-%m-%d %H:%M:%S"), session.get("username")))
        db.commit()
        session["last_activity_write"] = now.isoformat()
    except Exception:
        try:
            get_db().rollback()
        except Exception:
            pass


def col_exists(db, table, col):
    cur = db.execute(
        '''
        SELECT 1
        FROM information_schema.columns
        WHERE table_name=%s AND column_name=%s
        ''',
        (table, col),
    )
    return cur.fetchone() is not None


def row_to_dict(row):
    return dict(row)


def to_int(v, default=0):
    try:
        return int(v)
    except Exception:
        try:
            return int(float(v))
        except Exception:
            return default


def yesno(v, default="nein"):
    s = str(v or "").strip().lower()
    return "ja" if s in ("1", "true", "ja", "yes", "on") else default


def freeze_effective_rate_snapshot(db, event_id: str, username: str):
    """Freeze the effective hourly rate for a response row.

    Priority:
    1) event.stundensatz when the event is configured to use its own rate
    2) users.stundensatz otherwise

    This keeps historical reports stable even when profile or event rates
    are changed later.
    """
    ev = db.execute(
        "SELECT use_event_rate, stundensatz FROM event WHERE id=%s",
        (event_id,),
    ).fetchone()

    use_event_rate = to_int((ev or {}).get("use_event_rate", 1), 1) == 1
    event_rate = (ev or {}).get("stundensatz")
    if use_event_rate and event_rate not in (None, ""):
        try:
            return float(event_rate)
        except Exception:
            pass

    user_row = db.execute("SELECT stundensatz FROM users WHERE username=%s", (username,)).fetchone()
    if not user_row:
        return None

    user_rate = user_row.get("stundensatz")
    if user_rate in (None, ""):
        return None

    try:
        return float(user_rate)
    except Exception:
        return None


def freeze_confirmed_user_snapshots(db, username: str) -> int:
    """Freeze already confirmed assignments up to today before the profile rate changes.

    Past and today's confirmed assignments keep their old Personal-tab rate.
    Future confirmed assignments without an existing snapshot remain dynamic and can
    show the newly changed Personal-tab rate.
    """
    today = datetime.now().date()
    rows = db.execute(
        """SELECT r.event_id, e.start
           FROM response r
           JOIN event e ON e.id = r.event_id
           WHERE r.username=%s
             AND r.status=%s
             AND r.profile_rate_snapshot IS NULL""",
        (username, "bestätigt"),
    ).fetchall() or []

    changed = 0
    for row in rows:
        event_id = row.get("event_id")
        if not event_id:
            continue

        start_dt = parse_iso_dt(row.get("start"))
        # Nur Vergangenheit + heute einfrieren. Zukunft soll den neuen Personal-Satz übernehmen.
        if start_dt and start_dt.date() > today:
            continue

        snapshot = freeze_effective_rate_snapshot(db, event_id, username)
        db.execute(
            """UPDATE response
               SET profile_rate_snapshot=%s
               WHERE event_id=%s
                 AND username=%s
                 AND status=%s
                 AND profile_rate_snapshot IS NULL""",
            (snapshot, event_id, username, "bestätigt"),
        )
        changed += 1
    return changed


def release_future_profile_rate_snapshots(db, username: str) -> int:
    """Let future confirmed assignments follow the current Personal-tab hourly rate.

    Past and today's assignments remain frozen by freeze_confirmed_user_snapshots().
    For future assignments that do NOT use a fixed event rate, the snapshot is
    cleared so the employee modal/report preview can read the updated profile rate.
    Event-specific rates and manual rate overrides stay untouched.
    """
    today = datetime.now().date()
    rows = db.execute(
        """SELECT r.event_id, e.start, e.use_event_rate, e.stundensatz
           FROM response r
           JOIN event e ON e.id = r.event_id
           WHERE r.username=%s
             AND r.status=%s
             AND r.rate_override IS NULL
             AND r.profile_rate_snapshot IS NOT NULL""",
        (username, "bestätigt"),
    ).fetchall() or []

    changed = 0
    for row in rows:
        start_dt = parse_iso_dt(row.get("start"))
        if not start_dt or start_dt.date() <= today:
            continue

        use_event_rate = to_int(row.get("use_event_rate", 1), 1)
        has_event_rate = row.get("stundensatz") not in (None, "")

        # Nur Profil-Stundensatz dynamisch halten. Feste Einsatz-SVS bleiben eingefroren.
        if use_event_rate == 1 and has_event_rate:
            continue

        db.execute(
            """UPDATE response
               SET profile_rate_snapshot=NULL
               WHERE event_id=%s
                 AND username=%s
                 AND status=%s
                 AND rate_override IS NULL""",
            (row.get("event_id"), username, "bestätigt"),
        )
        changed += 1
    return changed


def freeze_confirmed_event_snapshots(db, event_id: str) -> int:
    """Freeze all already confirmed assignments for one event before the event rate changes.

    This prevents already confirmed assignments from adopting a later event-modal
    hourly-rate change. New confirmations after the edit still use the new rate.
    """
    rows = db.execute(
        """SELECT username
           FROM response
           WHERE event_id=%s
             AND status=%s
             AND profile_rate_snapshot IS NULL""",
        (event_id, "bestätigt"),
    ).fetchall() or []

    changed = 0
    for row in rows:
        username = row.get("username")
        if not username:
            continue
        snapshot = freeze_effective_rate_snapshot(db, event_id, username)
        db.execute(
            """UPDATE response
               SET profile_rate_snapshot=%s
               WHERE event_id=%s
                 AND username=%s
                 AND status=%s
                 AND profile_rate_snapshot IS NULL""",
            (snapshot, event_id, username, "bestätigt"),
        )
        changed += 1
    return changed


def parse_language_skills(value):
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    try:
        data = json.loads(value)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def dump_language_skills(value):
    if isinstance(value, str):
        try:
            json.loads(value)
            return value
        except Exception:
            return json.dumps({}, ensure_ascii=False)
    return json.dumps(value or {}, ensure_ascii=False)




def clean_image_data(value):
    value = (value or "").strip()
    if not value:
        return ""
    if value.startswith("data:image/") and ";base64," in value:
        return value
    return ""

def normalize_user_payload(d):
    language_skills = d.get("language_skills") or {}
    if isinstance(language_skills, str):
        language_skills = parse_language_skills(language_skills)

    cleaned_languages = {}
    for lang, level in (language_skills or {}).items():
        lang_name = str(lang or "").strip()
        level_name = str(level or "").strip()
        if lang_name and level_name:
            cleaned_languages[lang_name] = level_name

    return {
        "language_skills": dump_language_skills(cleaned_languages),
        "brandschutzhelfer": yesno(d.get("brandschutzhelfer")),
        "deeskalation": yesno(d.get("deeskalation")),
        "gssk": yesno(d.get("gssk")),
        "fachkraft_ss": yesno(d.get("fachkraft_ss")),
        "personenschutz": yesno(d.get("personenschutz")),
        "waffensachkunde": yesno(d.get("waffensachkunde")),
        "behoerdlich_studium": yesno(d.get("behoerdlich_studium")),
        "fuehrerschein": yesno(d.get("fuehrerschein")),
        "fuehrerschein_klassen": (d.get("fuehrerschein_klassen") or "").strip(),
        "image_data": clean_image_data(d.get("image_data")),
    }


def normalize_s34a_art(value):
    if not value:
        return value

    value = value.strip().lower()

    if value == "unterrichtung":
        return "Unterrichtung"
    if value == "sachkunde":
        return "Sachkunde"

    return value


def status_to_css_token(value: str) -> str:
    """Normalize status strings for safe CSS class tokens (e.g. 'bestätigt' -> 'bestaetigt')."""
    s = (value or "").strip().lower()
    if not s:
        return ""
    # German umlauts
    s = (s.replace("ä", "ae")
           .replace("ö", "oe")
           .replace("ü", "ue")
           .replace("ß", "ss"))
    # allow only [a-z0-9_-], replace other runs with '-'
    s = re.sub(r"[^a-z0-9_-]+", "-", s)
    s = re.sub(r"-{2,}", "-", s).strip("-")
    return s




def get_user_consent(db, username: str) -> dict:
    """Return consent info for a user: {given: bool, name: str, date: str, full_name: str}."""
    u = db.execute(
        "SELECT vorname, nachname, consent_given, consent_name, consent_date FROM users WHERE username=%s",
        (username,),
    ).fetchone()
    if not u:
        return {"given": False, "name": "", "date": "", "full_name": ""}

    full_name = f"{(u.get('vorname') or '').strip()} {(u.get('nachname') or '').strip()}".strip()
    given = bool(u.get("consent_given") or False)
    name = (u.get("consent_name") or "").strip()
    date = (u.get("consent_date") or "").strip()

    # Namensänderung Amine Saleh -> Amine Salah:
    # Alte Zustimmungs-/Bestandsdaten dürfen keine alte Anzeige erzwingen.
    username_key = str(username or "").strip().lower().replace(".", "")
    full_key = re.sub(r"\s+", "", (full_name or "").strip().lower())
    consent_key = re.sub(r"\s+", "", (name or "").strip().lower())
    if username_key in ("aminesaleh", "aminesalah") or full_key in ("aminesaleh", "aminesalah") or consent_key == "aminesaleh":
        full_name = "Amine Salah"
        if not name or consent_key == "aminesaleh":
            name = "Amine Salah"

    return {"given": given, "name": name, "date": date, "full_name": full_name}



def get_session_user_full_name() -> str:
    if "username" not in session:
        return ""
    try:
        u = get_db().execute(
            "SELECT vorname, nachname FROM users WHERE username=%s",
            (session.get("username"),),
        ).fetchone()
        if not u:
            return ""
        return f"{(u.get('vorname') or '').strip()} {(u.get('nachname') or '').strip()}".strip()
    except Exception:
        return ""

def employee_requires_consent() -> bool:
    """True if current session is a 'mitarbeiter' and consent is missing."""
    if session.get("role") != "mitarbeiter":
        return False
    try:
        info = get_user_consent(get_db(), session.get("username"))
        return not bool(info.get("given"))
    except Exception:
        # Im Zweifel sperren wir
        return True

def is_amine_salah_user() -> bool:
    full_name = re.sub(r"\s+", " ", (get_session_user_full_name() or "").strip()).lower()
    username = str(session.get("username") or "").strip().lower()
    # Namensänderung: Amine Saleh -> Amine Salah. Alte Usernamen/Berechtigungen bleiben gültig.
    return full_name in ("amine saleh", "amine salah") or username in ("amine.saleh", "aminesaleh", "amine.salah", "aminesalah")


def is_amine_salah_row(user_row) -> bool:
    """Robuste Prüfung für Personal-/API-Regeln zu Amine Saleh/Salah."""
    if not user_row:
        return False
    try:
        full_name = re.sub(r"\s+", " ", f"{(user_row.get('vorname') or '').strip()} {(user_row.get('nachname') or '').strip()}".strip()).lower()
        username = str(user_row.get("username") or "").strip().lower()
    except Exception:
        return False
    return full_name in ("amine saleh", "amine salah") or username in ("amine.saleh", "aminesaleh", "amine.salah", "aminesalah")


def current_user_can_see_bs() -> bool:
    """BS-Einsätze sind ausschließlich für den Mitarbeiter Amine Saleh sichtbar/änderbar."""
    return normalize_role(session.get("role") or "") == "mitarbeiter" and is_amine_salah_user()

def current_user_can_manage_private_jobs() -> bool:
    """Private Auftraggeber/Einsätze sind ausschließlich für Amine Saleh in der Mitarbeiteransicht."""
    return normalize_role(session.get("role") or "") == "mitarbeiter" and is_amine_salah_user()


def normalize_private_category(value: str, fallback: str = "PRIVAT") -> str:
    """Kategorie/Auftraggeber für Amines eigene Einsätze robust normalisieren.

    BS wird bewusst nicht mehr genutzt. Eigene Auftraggeber bleiben möglich,
    werden aber auf einfache, sichere Tokens begrenzt.
    """
    raw = str(value or "").strip() or fallback
    raw_up = raw.upper()
    if raw_up == "BS":
        raw_up = fallback
    if raw_up in ("CP", "CV"):
        return raw_up
    token = re.sub(r"[^A-ZÄÖÜ0-9_-]+", "_", raw_up)[:32].strip("_")
    return token or fallback


def is_private_amine_category(value: str) -> bool:
    cat = str(value or "").strip().upper()
    return bool(cat and cat not in ("CP", "CV") and cat != "BS")


def estimate_meal_allowance(hours) -> Decimal:
    """14 € Verpflegungspauschale automatisch ab 8 Stunden Einsatzdauer."""
    try:
        return Decimal("14.00") if Decimal(str(hours or 0)) >= Decimal("8") else Decimal("0.00")
    except Exception:
        return Decimal("0.00")



def event_is_bs(db, event_id: str) -> bool:
    row = db.execute("SELECT category FROM event WHERE id=%s", (event_id,)).fetchone()
    return bool(row and is_private_amine_category(row.get("category")))


def deny_bs_for_non_amine(db, event_id: str):
    if event_is_bs(db, event_id) and not current_user_can_manage_private_jobs():
        return jsonify({"error": "Private Einsätze sind nur für Amine sichtbar."}), 403
    return None


def render_locked_account_page():
    return render_template_string("""
<!DOCTYPE html>
<html lang="de">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Account gesperrt</title>
  <style>
    body{margin:0;font-family:Arial,Helvetica,sans-serif;background:#f3f4f6;color:#111827;}
    .wrap{min-height:100vh;display:flex;align-items:center;justify-content:center;padding:24px;}
    .card{width:min(560px,100%);background:#fff;border:1px solid #e5e7eb;border-radius:18px;box-shadow:0 16px 40px rgba(0,0,0,.08);padding:28px 24px;}
    .bar{height:5px;background:#dc2626;border-radius:999px;margin-bottom:18px;}
    h1{margin:0 0 12px;font-size:28px;line-height:1.15;}
    p{margin:0 0 10px;font-size:16px;line-height:1.55;}
    .hint{color:#6b7280;font-size:14px;margin-top:12px;}
    .btn{display:inline-block;margin-top:18px;padding:10px 14px;border-radius:10px;background:#111827;color:#fff;text-decoration:none;font-weight:700;}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="card">
      <div class="bar"></div>
      <h1>Account gesperrt</h1>
      <p>Ihr Account wurde gesperrt.</p>
      <p>Wenden Sie sich an Ihren Auftraggeber.</p>
      <p class="hint">Ein Login ist aktuell nicht möglich.</p>
      <a class="btn" href="/">Zurück zur Anmeldung</a>
    </div>
  </div>
</body>
</html>
    """), 423



def month_label_de(year: int, month: int) -> str:
    names = ["Januar", "Februar", "März", "April", "Mai", "Juni", "Juli", "August", "September", "Oktober", "November", "Dezember"]
    return f"{names[month-1]} {year}"


def decimal_money(value) -> Decimal:
    try:
        return Decimal(str(value or 0)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    except Exception:
        return Decimal("0.00")


def format_eur(value) -> str:
    amount = decimal_money(value)
    s = f"{amount:,.2f}"
    s = s.replace(",", "X").replace(".", ",").replace("X", ".")
    return f"{s} €"


def format_rate_eur(value) -> str:
    amount = decimal_money(value)
    s = f"{amount:,.2f}"
    s = s.replace(",", "X").replace(".", ",").replace("X", ".")
    return f"{s}€"


def parse_iso_dt(value: str):
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", ""))
    except Exception:
        try:
            return datetime.fromisoformat(raw.split("T")[0])
        except Exception:
            return None

def now_berlin_str() -> str:
    return datetime.now(ZoneInfo("Europe/Berlin")).strftime("%Y-%m-%d %H:%M:%S")


def parse_einsatzleitung_usernames(value, fallback=None) -> list[str]:
    """Return a clean, unique list of max. 3 Einsatzleiter usernames."""
    raw_values = []
    if isinstance(value, list):
        raw_values = value
    elif isinstance(value, str) and value.strip():
        txt = value.strip()
        try:
            parsed = json.loads(txt)
            if isinstance(parsed, list):
                raw_values = parsed
            else:
                raw_values = re.split(r"[,;]", txt)
        except Exception:
            raw_values = re.split(r"[,;]", txt)

    if fallback:
        raw_values.append(fallback)

    cleaned = []
    seen = set()
    for item in raw_values:
        username = str(item or "").strip()
        if not username or username in seen:
            continue
        cleaned.append(username)
        seen.add(username)
        if len(cleaned) >= 3:
            break
    return cleaned


def dump_einsatzleitung_usernames(value) -> str:
    return json.dumps(parse_einsatzleitung_usernames(value), ensure_ascii=False)


def format_last_activity(value) -> str:
    raw = str(value or "").strip()
    if not raw:
        return "Noch keine Aktivität"
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "").replace(" ", "T"))
        return "Zuletzt online: " + dt.strftime("%d.%m.%Y, %H:%M Uhr")
    except Exception:
        return "Zuletzt online: " + raw


def parse_hhmm(value: str):
    raw = str(value or "").strip()
    if not raw or ":" not in raw:
        return None
    try:
        hh, mm = raw.split(":", 1)
        return int(hh), int(mm)
    except Exception:
        return None


def build_invoice_entries_for_user(db, username: str, year: int, month: int, category: str):
    ecur = db.execute("SELECT * FROM event WHERE UPPER(COALESCE(category,'CP'))=%s", (category,))
    events = [row_to_dict(e) for e in ecur.fetchall()]
    entries = []

    for ev in events:
        resp = db.execute(
            """SELECT status, start_time, end_time, rate_override, profile_rate_snapshot
               FROM response WHERE event_id=%s AND username=%s""",
            (ev.get("id"), username)
        ).fetchone()

        if not resp:
            continue
        if (resp.get("status") or "").strip() != "bestätigt":
            continue
        if not (resp.get("end_time") or "").strip():
            continue

        start_dt = parse_iso_dt(ev.get("start"))
        if not start_dt:
            continue

        custom_start = parse_hhmm(resp.get("start_time"))
        if custom_start:
            start_dt = start_dt.replace(hour=custom_start[0], minute=custom_start[1], second=0, microsecond=0)

        custom_end = parse_hhmm(resp.get("end_time"))
        if not custom_end:
            continue
        end_dt = start_dt.replace(hour=custom_end[0], minute=custom_end[1], second=0, microsecond=0)
        if end_dt < start_dt:
            from datetime import timedelta
            end_dt = end_dt + timedelta(days=1)

        if start_dt.year != year or start_dt.month != month:
            continue

        if resp.get("rate_override") not in (None, ""):
            rate = decimal_money(resp.get("rate_override"))
        elif resp.get("profile_rate_snapshot") not in (None, ""):
            rate = decimal_money(resp.get("profile_rate_snapshot"))
        else:
            # Kein Snapshot vorhanden: aktueller effektiver Satz (für zukünftige/dynamische Einsätze).
            rate = decimal_money(freeze_effective_rate_snapshot(db, ev.get("id"), username))

        hours = decimal_money((end_dt - start_dt).total_seconds() / 3600)
        total = decimal_money(hours * rate)
        extra_costs = get_response_extra_costs(db, ev.get("id"), username)
        extra_total = sum((decimal_money(c.get("amount")) for c in extra_costs), Decimal("0.00"))
        entries.append({
            "date": start_dt,
            "title": (ev.get("title") or "Dienstleistung").strip() or "Dienstleistung",
            "event_id": ev.get("id"),
            "hours": hours,
            "rate": rate,
            "total": total,
            "extra_costs": extra_costs,
            "extra_total": extra_total,
            "grand_total": decimal_money(total + extra_total),
        })

    entries.sort(key=lambda x: (x["date"], x["title"]))
    return entries



def parse_extra_costs_payload(value):
    """Zusatzkosten aus JSON/Form-Daten robust normalisieren."""
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except Exception:
            value = []
    if not isinstance(value, list):
        return []
    cleaned = []
    for item in value:
        if not isinstance(item, dict):
            continue
        cost_id = str(item.get("id") or uuid.uuid4()).strip()
        label = str(item.get("label") or item.get("type") or item.get("kategorie") or "").strip()
        if not label:
            continue
        description = str(item.get("description") or item.get("beschreibung") or "").strip()
        amount_value = item.get("amount") if item.get("amount") is not None else item.get("betrag")
        amount_raw = str(amount_value if amount_value is not None else "0").strip().replace("€", "").replace(" ", "").replace(",", ".")
        amount = decimal_money(amount_raw)
        cleaned.append({"id": cost_id, "label": label, "description": description, "amount": float(amount), "amount_text": format_eur(amount)})
    return cleaned

def get_response_extra_costs(db, event_id: str, username: str) -> list[dict]:
    rows = db.execute(
        """SELECT id, label, description, amount
           FROM response_extra_costs
           WHERE event_id=%s AND username=%s
           ORDER BY created_at, id""",
        (event_id, username),
    ).fetchall() or []
    result = []
    for row in rows:
        amount = decimal_money(row.get("amount"))
        result.append({"id": row.get("id"), "label": row.get("label") or "", "description": row.get("description") or "", "amount": float(amount), "amount_text": format_eur(amount)})
    return result

def replace_response_extra_costs(db, event_id: str, username: str, costs: list[dict]):
    db.execute("DELETE FROM response_extra_costs WHERE event_id=%s AND username=%s", (event_id, username))
    now = datetime.now(ZoneInfo("Europe/Berlin")).strftime("%Y-%m-%d %H:%M:%S")
    for item in parse_extra_costs_payload(costs):
        db.execute(
            """INSERT INTO response_extra_costs
               (id, event_id, username, label, description, amount, created_at, updated_at)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
            (item["id"], event_id, username, item["label"], item["description"], item["amount"], now, now),
        )



def current_user_can_see_accounting() -> bool:
    """Buchführung ist ausschließlich für Amine Salah in der Mitarbeiteransicht aktiv."""
    return normalize_role(session.get("role") or "") == "mitarbeiter" and is_amine_salah_user()


def require_accounting_access():
    if "username" not in session:
        return jsonify({"error": "Nicht eingeloggt"}), 403
    if not current_user_can_see_accounting():
        return jsonify({"error": "Buchführung ist nur für Amine Salah verfügbar"}), 403
    if employee_requires_consent():
        return jsonify({"error": "Bitte zuerst im Report in die Datenverarbeitung einwilligen."}), 403
    return None


def parse_period_args():
    view = (request.args.get("view") or request.form.get("view") or "month").strip().lower()
    if view not in ("month", "year"):
        view = "month"
    today = datetime.now()
    try:
        year = int(request.args.get("year") or request.form.get("year") or today.year)
    except Exception:
        year = today.year
    try:
        month = int(request.args.get("month") or request.form.get("month") or today.month)
    except Exception:
        month = today.month
    if month < 1 or month > 12:
        month = today.month
    return view, year, month


def dt_in_period(dt, view, year, month):
    if not dt or dt.year != int(year):
        return False
    return not (view == "month" and dt.month != int(month))


def build_accounting_revenue_entries(db, username: str, view: str, year: int, month: int):
    events = [row_to_dict(e) for e in db.execute("SELECT * FROM event").fetchall()]
    entries = []
    for ev in events:
        # Buchführung: CP, CV und Amines eigene Auftraggeber berücksichtigen. BS bleibt bewusst außen vor.
        ev_category = (ev.get("category") or "CP").strip().upper()
        if ev_category == "BS":
            continue
        resp = db.execute("""SELECT status, start_time, end_time, rate_override, profile_rate_snapshot
                             FROM response WHERE event_id=%s AND username=%s""", (ev.get("id"), username)).fetchone()
        if not resp or (resp.get("status") or "").strip() != "bestätigt" or not (resp.get("end_time") or "").strip():
            continue
        start_dt = parse_iso_dt(ev.get("start"))
        if not start_dt:
            continue
        custom_start = parse_hhmm(resp.get("start_time"))
        if custom_start:
            start_dt = start_dt.replace(hour=custom_start[0], minute=custom_start[1], second=0, microsecond=0)
        if not dt_in_period(start_dt, view, year, month):
            continue
        custom_end = parse_hhmm(resp.get("end_time"))
        if not custom_end:
            continue
        end_dt = start_dt.replace(hour=custom_end[0], minute=custom_end[1], second=0, microsecond=0)
        if end_dt < start_dt:
            from datetime import timedelta
            end_dt = end_dt + timedelta(days=1)
        if resp.get("rate_override") not in (None, ""):
            rate = decimal_money(resp.get("rate_override"))
        elif resp.get("profile_rate_snapshot") not in (None, ""):
            rate = decimal_money(resp.get("profile_rate_snapshot"))
        else:
            rate = decimal_money(freeze_effective_rate_snapshot(db, ev.get("id"), username))
        hours = decimal_money((end_dt - start_dt).total_seconds() / 3600)
        total = decimal_money(hours * rate)
        meal = estimate_meal_allowance(hours)
        entries.append({
            "event_id": ev.get("id"), "date": start_dt.strftime("%Y-%m-%d"),
            "title": ev.get("title") or "(ohne Titel)", "category": (ev.get("category") or "CP").upper(),
            "ort": ev.get("ort") or "", "hours": float(hours), "rate": float(rate), "amount": float(total),
            "meal_allowance": float(meal)
        })
    entries.sort(key=lambda x: (x["date"], x["title"]))
    return entries


def build_accounting_summary(db, username: str, view: str, year: int, month: int):
    revenues = build_accounting_revenue_entries(db, username, view, year, month)

    manual_revenue_rows = db.execute("""SELECT id, datum, beschreibung, betrag, created_at
                                      FROM accounting_manual_revenues
                                      WHERE username=%s
                                      ORDER BY datum ASC, created_at ASC""", (username,)).fetchall() or []
    manual_revenues = []
    for r in manual_revenue_rows:
        dt = parse_iso_dt(r.get("datum"))
        if not dt_in_period(dt, view, year, month):
            continue
        amount = decimal_money(r.get("betrag"))
        manual_revenues.append({"id": r.get("id"), "date": (r.get("datum") or "")[:10],
                                "description": r.get("beschreibung") or "", "amount": float(amount)})

    automatic_revenue_total = decimal_money(sum(decimal_money(e["amount"]) for e in revenues))
    manual_revenue_total = decimal_money(sum(decimal_money(e["amount"]) for e in manual_revenues))
    revenue_total = decimal_money(automatic_revenue_total + manual_revenue_total)

    expense_rows = db.execute("""SELECT id, datum, kategorie, beschreibung, betrag, beleg_path, beleg_name, created_at
                                 FROM accounting_expenses WHERE username=%s ORDER BY datum ASC, created_at ASC""", (username,)).fetchall() or []
    expenses = []
    for r in expense_rows:
        dt = parse_iso_dt(r.get("datum"))
        if not dt_in_period(dt, view, year, month):
            continue
        amount = decimal_money(r.get("betrag"))
        expenses.append({"id": r.get("id"), "date": (r.get("datum") or "")[:10], "category": r.get("kategorie") or "Sonstiges",
                         "description": r.get("beschreibung") or "", "amount": float(amount),
                         "receipt_name": r.get("beleg_name") or "", "has_receipt": bool(r.get("beleg_path"))})
    expenses_total = decimal_money(sum(decimal_money(e["amount"]) for e in expenses))

    travel_rows = db.execute("""SELECT t.id, t.event_id, t.km_total, t.note, e.title, e.ort, e.start, COALESCE(e.category,'CP') AS category
                                FROM accounting_travel t JOIN event e ON e.id=t.event_id
                                WHERE t.username=%s AND UPPER(COALESCE(e.category,'CP')) <> 'BS'
                                ORDER BY e.start ASC""", (username,)).fetchall() or []
    travel = []
    for r in travel_rows:
        dt = parse_iso_dt(r.get("start"))
        if not dt_in_period(dt, view, year, month):
            continue
        km = decimal_money(r.get("km_total"))
        cost = decimal_money(km * Decimal("0.30"))
        travel.append({"id": r.get("id"), "event_id": r.get("event_id"), "date": dt.strftime("%Y-%m-%d") if dt else "",
                       "title": r.get("title") or "(ohne Titel)", "category": (r.get("category") or "CP").upper(),
                       "ort": r.get("ort") or "", "km_total": float(km), "amount": float(cost), "note": r.get("note") or ""})
    travel_total = decimal_money(sum(decimal_money(t["amount"]) for t in travel))

    meal_total = decimal_money(sum(decimal_money(e.get("meal_allowance") or 0) for e in revenues))

    settings = db.execute("SELECT office_address, homeoffice_days_month, internet_monthly, phone_monthly FROM accounting_settings WHERE username=%s", (username,)).fetchone() or {}
    internet_total = decimal_money(settings.get("internet_monthly") or 0) * (Decimal("1") if view == "month" else Decimal("12"))
    phone_total = decimal_money(settings.get("phone_monthly") or 0) * (Decimal("1") if view == "month" else Decimal("12"))
    fixed_total = decimal_money(internet_total + phone_total)
    total_expenses = decimal_money(expenses_total + travel_total + meal_total + fixed_total)
    profit = decimal_money(revenue_total - total_expenses)
    return {"view": view, "year": year, "month": month,
            "settings": {"office_address": settings.get("office_address") or "", "homeoffice_days_month": 0,
                         "internet_monthly": float(decimal_money(settings.get("internet_monthly") or 0)),
                         "phone_monthly": float(decimal_money(settings.get("phone_monthly") or 0))},
            "revenues": revenues, "manual_revenues": manual_revenues, "expenses": expenses, "travel": travel,
            "totals": {"revenues": float(revenue_total), "automatic_revenues": float(automatic_revenue_total),
                       "manual_revenues": float(manual_revenue_total), "manual_expenses": float(expenses_total), "travel": float(travel_total),
                       "meal_allowance": float(meal_total), "homeoffice": 0.0, "internet": float(decimal_money(internet_total)),
                       "phone": float(decimal_money(phone_total)), "fixed": float(fixed_total),
                       "expenses": float(total_expenses), "profit": float(profit)}}

def init_db():
    db = get_db()

    # NOTE: In Postgres ist "user" ein reserviertes Wort -> wir nutzen "users".
    db.execute(
        '''
        CREATE TABLE IF NOT EXISTS users (
            username TEXT PRIMARY KEY,
            password TEXT NOT NULL,
            role TEXT DEFAULT 'mitarbeiter',
            vorname TEXT,
            nachname TEXT,
            email TEXT,
            s34a TEXT,
            s34a_art TEXT,
            pschein TEXT,
            bewach_id TEXT,
            steuernummer TEXT,
            bsw TEXT,
            sanitaeter TEXT,
            bemerkung TEXT,
            is_locked BOOLEAN DEFAULT FALSE,
            stundensatz DOUBLE PRECISION,
            consent_given BOOLEAN DEFAULT FALSE,
            consent_name TEXT,
            consent_date TEXT,
            language_skills TEXT,
            brandschutzhelfer TEXT DEFAULT 'nein',
            deeskalation TEXT DEFAULT 'nein',
            gssk TEXT DEFAULT 'nein',
            fachkraft_ss TEXT DEFAULT 'nein',
            personenschutz TEXT DEFAULT 'nein',
            waffensachkunde TEXT DEFAULT 'nein',
            behoerdlich_studium TEXT DEFAULT 'nein',
            fuehrerschein TEXT DEFAULT 'nein',
            fuehrerschein_klassen TEXT,
            image_data TEXT,
            ausweis_art TEXT,
            ausweis_nr TEXT,
            ausweis_behoerde TEXT,
            ausweis_gueltig_bis TEXT,
            geburtsort TEXT,
            geburtstag TEXT,
            last_activity_at TEXT
        );
        '''
    )

    db.execute(
        '''
        CREATE TABLE IF NOT EXISTS event (
            id TEXT PRIMARY KEY,
            title TEXT,
            ort TEXT,
            dienstkleidung TEXT,
            auftraggeber TEXT,
            start TEXT,
            planned_end_time TEXT,      -- 'HH:MM'
            frist TEXT,                 -- 'YYYY-MM-DDTHH:MM' (Annahmefrist)
            status TEXT,                -- 'geplant' | 'offen'
            category TEXT DEFAULT 'CP', -- 'CP' | 'CV' | 'BS' | 'HB'
            required_staff INTEGER DEFAULT 0,
            use_event_rate INTEGER DEFAULT 1, -- 1=Einsatz-Stundensatz, 0=User-Profil
            stundensatz DOUBLE PRECISION,
            einsatzleitung_username TEXT,
            einsatzleitung_usernames TEXT
        );
        '''
    )

    db.execute(
        '''
        CREATE TABLE IF NOT EXISTS board_posts (
            id SERIAL PRIMARY KEY,
            content TEXT NOT NULL,
            created_at TEXT NOT NULL,
            created_by TEXT NOT NULL
        );
        '''
    )

    db.execute(
        '''
        CREATE TABLE IF NOT EXISTS response (
            id SERIAL PRIMARY KEY,
            event_id TEXT NOT NULL REFERENCES event(id) ON DELETE CASCADE,
            username TEXT NOT NULL REFERENCES users(username) ON DELETE CASCADE,
            status TEXT,
            remark TEXT,
            start_time TEXT,
            end_time TEXT,
            rate_override DOUBLE PRECISION,
            profile_rate_snapshot DOUBLE PRECISION,
            UNIQUE(event_id, username)
        );
        '''
    )

    # Indizes
    db.execute("CREATE INDEX IF NOT EXISTS idx_response_event ON response(event_id);")
    db.execute("CREATE INDEX IF NOT EXISTS idx_response_user  ON response(username);")


    db.execute(
        """
        CREATE TABLE IF NOT EXISTS response_extra_costs (
            id TEXT PRIMARY KEY,
            event_id TEXT NOT NULL REFERENCES event(id) ON DELETE CASCADE,
            username TEXT NOT NULL REFERENCES users(username) ON DELETE CASCADE,
            label TEXT NOT NULL,
            description TEXT,
            amount DOUBLE PRECISION NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        """
    )
    db.execute("CREATE INDEX IF NOT EXISTS idx_response_extra_costs_event_user ON response_extra_costs(event_id, username);")

    # ---- Migrationen (falls Tabellen schon existieren, aber Spalten fehlen) ----
    # users
    for c, ddl in [
        ("email", "ALTER TABLE users ADD COLUMN email TEXT"),
        ("bewach_id", "ALTER TABLE users ADD COLUMN bewach_id TEXT"),
        ("steuernummer", "ALTER TABLE users ADD COLUMN steuernummer TEXT"),
        ("bsw", "ALTER TABLE users ADD COLUMN bsw TEXT"),
        ("sanitaeter", "ALTER TABLE users ADD COLUMN sanitaeter TEXT"),
        ("bemerkung", "ALTER TABLE users ADD COLUMN bemerkung TEXT"),
        ("is_locked", "ALTER TABLE users ADD COLUMN is_locked BOOLEAN DEFAULT FALSE"),
        ("stundensatz", "ALTER TABLE users ADD COLUMN stundensatz DOUBLE PRECISION"),
        ("consent_given", "ALTER TABLE users ADD COLUMN consent_given BOOLEAN DEFAULT FALSE"),
        ("consent_name", "ALTER TABLE users ADD COLUMN consent_name TEXT"),
        ("consent_date", "ALTER TABLE users ADD COLUMN consent_date TEXT"),
        ("s34a", "ALTER TABLE users ADD COLUMN s34a TEXT"),
        ("s34a_art", "ALTER TABLE users ADD COLUMN s34a_art TEXT"),
        ("pschein", "ALTER TABLE users ADD COLUMN pschein TEXT"),
        ("vorname", "ALTER TABLE users ADD COLUMN vorname TEXT"),
        ("nachname", "ALTER TABLE users ADD COLUMN nachname TEXT"),
        ("role", "ALTER TABLE users ADD COLUMN role TEXT DEFAULT 'mitarbeiter'"),
        ("password", "ALTER TABLE users ADD COLUMN password TEXT"),
        ("language_skills", "ALTER TABLE users ADD COLUMN language_skills TEXT"),
        ("brandschutzhelfer", "ALTER TABLE users ADD COLUMN brandschutzhelfer TEXT DEFAULT 'nein'"),
        ("deeskalation", "ALTER TABLE users ADD COLUMN deeskalation TEXT DEFAULT 'nein'"),
        ("gssk", "ALTER TABLE users ADD COLUMN gssk TEXT DEFAULT 'nein'"),
        ("fachkraft_ss", "ALTER TABLE users ADD COLUMN fachkraft_ss TEXT DEFAULT 'nein'"),
        ("personenschutz", "ALTER TABLE users ADD COLUMN personenschutz TEXT DEFAULT 'nein'"),
        ("waffensachkunde", "ALTER TABLE users ADD COLUMN waffensachkunde TEXT DEFAULT 'nein'"),
        ("behoerdlich_studium", "ALTER TABLE users ADD COLUMN behoerdlich_studium TEXT DEFAULT 'nein'"),
        ("fuehrerschein", "ALTER TABLE users ADD COLUMN fuehrerschein TEXT DEFAULT 'nein'"),
        ("fuehrerschein_klassen", "ALTER TABLE users ADD COLUMN fuehrerschein_klassen TEXT"),
        ("image_data", "ALTER TABLE users ADD COLUMN image_data TEXT"),
        ("ausweis_art", "ALTER TABLE users ADD COLUMN ausweis_art TEXT"),
        ("ausweis_nr", "ALTER TABLE users ADD COLUMN ausweis_nr TEXT"),
        ("ausweis_behoerde", "ALTER TABLE users ADD COLUMN ausweis_behoerde TEXT"),
        ("ausweis_gueltig_bis", "ALTER TABLE users ADD COLUMN ausweis_gueltig_bis TEXT"),
        ("geburtsort", "ALTER TABLE users ADD COLUMN geburtsort TEXT"),
        ("geburtstag", "ALTER TABLE users ADD COLUMN geburtstag TEXT"),
        ("last_activity_at", "ALTER TABLE users ADD COLUMN last_activity_at TEXT"),
    ]:
        if not col_exists(db, "users", c):
            db.execute(ddl)


    # Stammdatenänderung: Amine Saleh heißt jetzt Amine Salah; Username/Berechtigungen bleiben unverändert.
    db.execute(
        """UPDATE users
           SET vorname='Amine', nachname='Salah', consent_name=CASE
                 WHEN LOWER(COALESCE(consent_name,''))='amine saleh' THEN 'Amine Salah'
                 ELSE consent_name
               END
           WHERE (LOWER(COALESCE(vorname,''))='amine' AND LOWER(COALESCE(nachname,'')) IN ('saleh','salah'))
              OR LOWER(REPLACE(COALESCE(username,''),'.','')) IN ('aminesaleh','aminesalah')"""
    )

    # event
    for c, ddl in [
        ("planned_end_time", "ALTER TABLE event ADD COLUMN planned_end_time TEXT"),
        ("frist", "ALTER TABLE event ADD COLUMN frist TEXT"),
        ("status", "ALTER TABLE event ADD COLUMN status TEXT"),
        ("category", "ALTER TABLE event ADD COLUMN category TEXT DEFAULT 'CP'"),
        ("required_staff", "ALTER TABLE event ADD COLUMN required_staff INTEGER DEFAULT 0"),
        ("use_event_rate", "ALTER TABLE event ADD COLUMN use_event_rate INTEGER DEFAULT 1"),
        ("stundensatz", "ALTER TABLE event ADD COLUMN stundensatz DOUBLE PRECISION"),
        ("einsatzleitung_username", "ALTER TABLE event ADD COLUMN einsatzleitung_username TEXT"),
        ("einsatzleitung_usernames", "ALTER TABLE event ADD COLUMN einsatzleitung_usernames TEXT"),
    ]:
        if not col_exists(db, "event", c):
            db.execute(ddl)

    # response
    for c, ddl in [
        ("profile_rate_snapshot", "ALTER TABLE response ADD COLUMN profile_rate_snapshot DOUBLE PRECISION"),
    ]:
        if not col_exists(db, "response", c):
            db.execute(ddl)

    # response
    for c, ddl in [
        ("status", "ALTER TABLE response ADD COLUMN status TEXT"),
        ("remark", "ALTER TABLE response ADD COLUMN remark TEXT"),
        ("start_time", "ALTER TABLE response ADD COLUMN start_time TEXT"),
        ("end_time", "ALTER TABLE response ADD COLUMN end_time TEXT"),
        ("rate_override", "ALTER TABLE response ADD COLUMN rate_override DOUBLE PRECISION"),
    ]:
        if not col_exists(db, "response", c):
            db.execute(ddl)


    # accounting (nur für Amine Saleh sichtbar, technisch pro username gespeichert)
    db.execute(
        '''
        CREATE TABLE IF NOT EXISTS accounting_expenses (
            id TEXT PRIMARY KEY,
            username TEXT NOT NULL REFERENCES users(username) ON DELETE CASCADE,
            datum TEXT NOT NULL,
            kategorie TEXT NOT NULL,
            beschreibung TEXT,
            betrag DOUBLE PRECISION NOT NULL DEFAULT 0,
            beleg_path TEXT,
            beleg_name TEXT,
            created_at TEXT NOT NULL
        );
        '''
    )
    db.execute("CREATE INDEX IF NOT EXISTS idx_accounting_expenses_user ON accounting_expenses(username);")
    db.execute("CREATE INDEX IF NOT EXISTS idx_accounting_expenses_date ON accounting_expenses(datum);")

    db.execute(
        '''
        CREATE TABLE IF NOT EXISTS accounting_manual_revenues (
            id TEXT PRIMARY KEY,
            username TEXT NOT NULL REFERENCES users(username) ON DELETE CASCADE,
            datum TEXT NOT NULL,
            beschreibung TEXT NOT NULL,
            betrag DOUBLE PRECISION NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL
        );
        '''
    )
    db.execute("CREATE INDEX IF NOT EXISTS idx_accounting_manual_revenues_user ON accounting_manual_revenues(username);")
    db.execute("CREATE INDEX IF NOT EXISTS idx_accounting_manual_revenues_date ON accounting_manual_revenues(datum);")

    db.execute(
        '''
        CREATE TABLE IF NOT EXISTS accounting_travel (
            id TEXT PRIMARY KEY,
            username TEXT NOT NULL REFERENCES users(username) ON DELETE CASCADE,
            event_id TEXT NOT NULL REFERENCES event(id) ON DELETE CASCADE,
            km_total DOUBLE PRECISION NOT NULL DEFAULT 0,
            note TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(username, event_id)
        );
        '''
    )
    db.execute("CREATE INDEX IF NOT EXISTS idx_accounting_travel_user ON accounting_travel(username);")

    db.execute(
        '''
        CREATE TABLE IF NOT EXISTS accounting_settings (
            username TEXT PRIMARY KEY REFERENCES users(username) ON DELETE CASCADE,
            office_address TEXT,
            homeoffice_days_month INTEGER DEFAULT 0,
            internet_monthly DOUBLE PRECISION DEFAULT 0,
            phone_monthly DOUBLE PRECISION DEFAULT 0,
            updated_at TEXT NOT NULL
        );
        '''
    )


    # driver_rides (Fahrer-Reiter nur für Amine Saleh; Fotos als JSON/Data-URLs gespeichert)
    db.execute(
        '''
        CREATE TABLE IF NOT EXISTS driver_rides (
            id TEXT PRIMARY KEY,
            username TEXT NOT NULL REFERENCES users(username) ON DELETE CASCADE,
            driver_name TEXT,
            duty_date TEXT,
            service_start TEXT,
            service_end TEXT,
            license_plate TEXT,
            passenger TEXT,
            departure_time TEXT,
            duration_minutes INTEGER DEFAULT 0,
            arrival_time TEXT,
            destination TEXT,
            remark TEXT,
            vehicle_photos TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        '''
    )
    db.execute("ALTER TABLE driver_rides ADD COLUMN IF NOT EXISTS service_end TEXT;")
    db.execute("ALTER TABLE driver_rides ADD COLUMN IF NOT EXISTS license_plate TEXT;")
    db.execute("CREATE INDEX IF NOT EXISTS idx_driver_rides_user ON driver_rides(username);")
    db.execute("CREATE INDEX IF NOT EXISTS idx_driver_rides_date ON driver_rides(duty_date);")

    db.commit()

    # ---- AdminTest ----
    exists = db.execute("SELECT 1 FROM users WHERE username=%s", ("AdminTest",)).fetchone()
    if not exists:
        db.execute(
            '''
            INSERT INTO users
               (username,password,role,vorname,nachname,email,s34a,s34a_art,pschein,bewach_id,steuernummer,bsw,sanitaeter,bemerkung,is_locked,stundensatz)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ''',
            (
                "AdminTest", "Test1234", "vorgesetzter",
                "Admin", "Test",
                "",          # email
                "ja",        # s34a
                "Sachkunde", # s34a_art
                "ja",        # pschein
                "A-000",     # bewach_id
                "ST-000",    # steuernummer
                "nein",      # bsw
                "nein",      # sanitaeter
                "",          # bemerkung
                False,       # is_locked
                0.0,
            ),
        )
        db.commit()


def safe_init_db():
    try:
        with app.app_context():
            init_db()
        print("DB-Initialisierung erfolgreich.")
    except Exception as e:
        # Wichtig: nicht crashen, nur Fehler loggen
        print("FEHLER bei init_db():", repr(e))


# Wird beim Import einmal ausgeführt
safe_init_db()


# ---------------- Routes ----------------
@app.route("/health")
def health():
    return "ok", 200


@app.route("/", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form["username"].strip()
        password = request.form["password"]

        db = get_db()
        u = db.execute("SELECT * FROM users WHERE username=%s", (username,)).fetchone()

        if u and u.get("password") == password:
            if bool(u.get("is_locked") or False):
                return render_locked_account_page()
            session["username"] = username
            session["role"] = u.get("role") or "mitarbeiter"
            try:
                now_s = now_berlin_str()
                db.execute("UPDATE users SET last_activity_at=%s WHERE username=%s", (now_s, username))
                db.commit()
                session["last_activity_write"] = datetime.now(ZoneInfo("Europe/Berlin")).isoformat()
            except Exception:
                try:
                    db.rollback()
                except Exception:
                    pass
            return redirect(url_for("dashboard"))

        return render_template("login.html", error="Login fehlgeschlagen")
    return render_template("login.html")


@app.route("/dashboard")
def dashboard():
    if "username" not in session:
        return redirect(url_for("login"))

    role = normalize_role(session.get("role") or "mitarbeiter")
    full_name = get_session_user_full_name()

    # Chef-Dashboard auch für Planer (UI beschränkt Planer auf den Planung-Reiter)
    if role in ["chef", "vorgesetzter", "planer", "planner_bbs", "vorgesetzter_cp"]:
        return render_template("dashboard_chef.html", user=session["username"], role=role, full_name=full_name)

    return render_template("dashboard_mitarbeiter.html", user=session["username"], role=role, full_name=full_name, amine_enabled=is_amine_salah_user())


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


# ---------------- Consent (DSGVO) ----------------
@app.route("/consent_status", methods=["GET"])
def consent_status():
    if "username" not in session:
        return jsonify({"error": "Nicht eingeloggt"}), 403
    db = get_db()
    info = get_user_consent(db, session.get("username"))
    return jsonify(info)


@app.route("/consent", methods=["POST"])
def consent_set():
    if "username" not in session:
        return jsonify({"error": "Nicht eingeloggt"}), 403

    # Nur Mitarbeiter müssen hier zustimmen
    if session.get("role") != "mitarbeiter":
        return jsonify({"error": "Nicht erlaubt"}), 403

    d = request.json or {}
    yes = bool(d.get("yes") is True or str(d.get("yes")).lower() in ("1", "true", "ja", "yes"))
    name = (d.get("name") or "").strip()
    date = (d.get("date") or "").strip()

    if not yes:
        return jsonify({"error": "Bitte bestätige die Einwilligung."}), 400
    if not name:
        return jsonify({"error": "Name ist erforderlich."}), 400
    if not date:
        # Fallback: heute
        date = datetime.now().strftime("%Y-%m-%d")

    db = get_db()
    db.execute(
        "UPDATE users SET consent_given=TRUE, consent_name=%s, consent_date=%s WHERE username=%s",
        (name, date, session.get("username")),
    )
    db.commit()
    return jsonify({"status": "ok"})


# ---------------- Board / Startseite ----------------
@app.route("/board", methods=["GET"])
def get_board_posts():
    if "username" not in session:
        return jsonify({"error": "Nicht eingeloggt"}), 403

    db = get_db()
    cur = db.execute(
        "SELECT id, content, created_at, created_by FROM board_posts ORDER BY id DESC LIMIT 50"
    )
    return jsonify([row_to_dict(r) for r in cur.fetchall()])


@app.route("/board", methods=["POST"])
def add_board_post():
    if "username" not in session:
        return jsonify({"error": "Nicht eingeloggt"}), 403

    role = normalize_role(session.get("role") or "")
    if role not in ["chef", "vorgesetzter", "vorgesetzter_cp"]:
        return jsonify({"error": "Nicht erlaubt"}), 403

    d = request.json or {}
    content = (d.get("content") or "").strip()
    send_mail_flag = bool(d.get("send_mail") is True or str(d.get("send_mail")).lower() in ("1", "true", "ja", "yes", "on"))
    if not content:
        return jsonify({"error": "Bitte einen Text eingeben."}), 400

    if len(content) > 5000:
        return jsonify({"error": "Der Beitrag ist zu lang."}), 400

    db = get_db()
    author = session.get("username")
    db.execute(
        "INSERT INTO board_posts (content, created_at, created_by) VALUES (%s, %s, %s)",
        (content, datetime.now().isoformat(timespec="seconds"), author),
    )
    db.commit()

    sent = 0
    if send_mail_flag:
        cur = db.execute(
            "SELECT vorname, nachname, email FROM users WHERE role=%s AND COALESCE(is_locked, FALSE)=FALSE",
            ("mitarbeiter",),
        )
        rows = cur.fetchall() or []
        subject = "Neuer Beitrag auf der Startseite"
        for u in rows:
            to_addr = (u.get("email") or "").strip()
            if not to_addr:
                continue
            recipient_name = f"{(u.get('vorname') or '').strip()} {(u.get('nachname') or '').strip()}".strip() or "Mitarbeiter/in"
            body = build_board_post_mail(recipient_name, content, author)
            try:
                send_mail(to_addr, subject, body)
                sent += 1
            except Exception:
                pass

    return jsonify({"status": "ok", "sent": sent})




@app.route("/board/<int:post_id>", methods=["DELETE"])
def delete_board_post(post_id):
    if "username" not in session:
        return jsonify({"error": "Nicht eingeloggt"}), 403

    role = normalize_role(session.get("role") or "")
    if role not in ["chef", "vorgesetzter", "vorgesetzter_cp"]:
        return jsonify({"error": "Nicht erlaubt"}), 403

    db = get_db()
    cur = db.execute("DELETE FROM board_posts WHERE id=%s", (post_id,))
    db.commit()
    if cur.rowcount == 0:
        return jsonify({"error": "Beitrag nicht gefunden"}), 404
    return jsonify({"status": "ok"})


# ---------------- Users API ----------------
@app.route("/users", methods=["GET"])
def get_users():
    # ✅ Sensible Personaldaten: Chef, Vorgesetzter und Vorgesetzter CP
    if normalize_role(session.get("role")) not in ["chef", "vorgesetzter", "vorgesetzter_cp"]:
        return jsonify({"error": "Nicht erlaubt"}), 403

    cur = get_db().execute(
        """SELECT * FROM users\n           WHERE username NOT IN (%s,%s)\n           ORDER BY\n             CASE WHEN LOWER(COALESCE(vorname, '')) = %s AND LOWER(COALESCE(nachname, '')) = %s THEN 0 ELSE 1 END,\n             LOWER(COALESCE(vorname, '')),\n             LOWER(COALESCE(nachname, '')),\n             LOWER(COALESCE(username, ''))""",
        ("AdminTest","TestAdmin", "kevin", "casutt")
    )
    users = [row_to_dict(r) for r in cur.fetchall()]
    viewer_role = normalize_role(session.get("role"))
    for u in users:
        if u.get("stundensatz") is None:
            u["stundensatz"] = ""
        u["language_skills"] = parse_language_skills(u.get("language_skills"))
        # Vorgesetzter/Vorgesetzter CP dürfen Amine Salahs Passwort weder sehen noch im UI ändern.
        if viewer_role in ["vorgesetzter", "vorgesetzter_cp"] and is_amine_salah_row(u):
            u["password"] = ""
            u["password_protected"] = True
    return jsonify(users)


@app.route("/users_public", methods=["GET"])
def users_public():
    """
    Minimaler User-Export (nur Name) für Planung.
    Erlaubt für eingeloggte Rollen inkl. Planer – ohne sensible Felder/Passwörter.
    """
    if "username" not in session:
        return jsonify({"error": "Nicht eingeloggt"}), 403

    if normalize_role(session.get("role")) not in ["chef", "vorgesetzter", "planer", "planner_bbs", "vorgesetzter_cp"]:
        return jsonify({"error": "Nicht erlaubt"}), 403

    cur = get_db().execute(
        """SELECT username, vorname, nachname FROM users\n           WHERE username NOT IN (%s,%s) AND COALESCE(is_locked, FALSE)=FALSE\n           ORDER BY\n             CASE WHEN LOWER(COALESCE(vorname, '')) = %s AND LOWER(COALESCE(nachname, '')) = %s THEN 0 ELSE 1 END,\n             LOWER(COALESCE(vorname, '')),\n             LOWER(COALESCE(nachname, '')),\n             LOWER(COALESCE(username, ''))""",
        ("AdminTest", "TestAdmin", "kevin", "casutt")
    )
    users = [row_to_dict(r) for r in cur.fetchall()]
    return jsonify(users)




@app.route("/users_planner_bbs", methods=["GET"])
def users_planner_bbs():
    if "username" not in session:
        return jsonify({"error": "Nicht eingeloggt"}), 403

    if normalize_role(session.get("role")) not in ["chef", "vorgesetzter", "vorgesetzter_cp", "planer", "planner_bbs"]:
        return jsonify({"error": "Nicht erlaubt"}), 403

    cur = get_db().execute(
        """SELECT username, vorname, nachname, role FROM users
           WHERE username NOT IN (%s,%s)
             AND COALESCE(is_locked, FALSE)=FALSE
             AND (
               LOWER(COALESCE(role, '')) = %s
               OR (LOWER(COALESCE(vorname, '')) = %s AND LOWER(COALESCE(nachname, '')) = %s)
               OR (LOWER(COALESCE(vorname, '')) = %s AND LOWER(COALESCE(nachname, '')) = %s)
             )
           ORDER BY LOWER(COALESCE(vorname, '')), LOWER(COALESCE(nachname, '')), LOWER(COALESCE(username, ''))""",
        ("AdminTest", "TestAdmin", "planner_bbs", "lucas", "pfennig", "kevin", "cassut")
    )
    return jsonify([row_to_dict(r) for r in cur.fetchall()])


@app.route("/users_extract", methods=["GET"])
def users_extract():
    """Moderner Mitarbeiter-Auszug direkt im Portal für Einsatzleiter.

    Enthält nur die für Einsatzplanung relevanten Vorschau-Felder und keine
    Passwörter, Stundensätze oder administrativen Bearbeitungsdaten.
    """
    if "username" not in session:
        return jsonify({"error": "Nicht eingeloggt"}), 403

    if normalize_role(session.get("role")) not in ["chef", "vorgesetzter", "vorgesetzter_cp", "planner_bbs"]:
        return jsonify({"error": "Nicht erlaubt"}), 403

    cur = get_db().execute(
        """SELECT username, vorname, nachname, geburtstag, geburtsort, bemerkung,
                  s34a, s34a_art, bewach_id, bsw, sanitaeter, pschein
           FROM users
           WHERE username NOT IN (%s,%s)
             AND COALESCE(is_locked, FALSE)=FALSE
             AND LOWER(COALESCE(role, '')) NOT IN (%s,%s,%s,%s,%s)
           ORDER BY
             LOWER(COALESCE(vorname, '')),
             LOWER(COALESCE(nachname, '')),
             LOWER(COALESCE(username, ''))""",
        ("AdminTest", "TestAdmin", "chef", "vorgesetzter", "vorgesetzter_cp", "planer", "planner_bbs")
    )
    users = [row_to_dict(r) for r in cur.fetchall()]
    for u in users:
        for key in ["bemerkung", "s34a", "s34a_art", "bewach_id", "bsw", "sanitaeter", "pschein", "geburtstag", "geburtsort"]:
            u[key] = u.get(key) or ""
    return jsonify(users)


@app.route("/users", methods=["POST"])
def add_user():
    if normalize_role(session.get("role")) not in ["chef", "vorgesetzter", "vorgesetzter_cp"]:
        return jsonify({"error": "Nicht erlaubt"}), 403

    d = request.json or {}
    username = (d.get("username") or "").strip()
    if not username:
        return jsonify({"error": "username ist erforderlich"}), 400

    db = get_db()
    stundensatz = d.get("stundensatz")
    stundensatz = None if stundensatz in (None, "") else float(stundensatz)

    password = d.get("password") or ""
    email = (d.get("email") or "").strip()
    employee_name = f"{(d.get('vorname') or '').strip()} {(d.get('nachname') or '').strip()}".strip() or username
    extra = normalize_user_payload(d)

    try:
        db.execute(
            """INSERT INTO users
               (username,password,role,vorname,nachname,email,geburtsort,geburtstag,s34a,s34a_art,pschein,bewach_id,steuernummer,bsw,sanitaeter,bemerkung,is_locked,stundensatz,
                language_skills,brandschutzhelfer,deeskalation,gssk,fachkraft_ss,personenschutz,waffensachkunde,behoerdlich_studium,fuehrerschein,fuehrerschein_klassen,image_data,ausweis_art,ausweis_nr,ausweis_behoerde,ausweis_gueltig_bis)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (
                username,
                password,
                d.get("role") or "mitarbeiter",
                d.get("vorname") or "",
                d.get("nachname") or "",
                email,
                (d.get("geburtsort") or "").strip(),
                (d.get("geburtstag") or "").strip(),
                d.get("s34a") or "nein",
                normalize_s34a_art(d.get("s34a_art") or ""),
                d.get("pschein") or "nein",
                d.get("bewach_id") or "",
                d.get("steuernummer") or "",
                d.get("bsw") or "nein",
                d.get("sanitaeter") or "nein",
                d.get("bemerkung") or "",
                False,
                stundensatz,
                extra["language_skills"],
                extra["brandschutzhelfer"],
                extra["deeskalation"],
                extra["gssk"],
                extra["fachkraft_ss"],
                extra["personenschutz"],
                extra["waffensachkunde"],
                extra["behoerdlich_studium"],
                extra["fuehrerschein"],
                extra["fuehrerschein_klassen"],
                extra["image_data"],
                d.get("ausweis_art") or "",
                d.get("ausweis_nr") or "",
                d.get("ausweis_behoerde") or "",
                d.get("ausweis_gueltig_bis") or "",
            ),
        )
        db.commit()
    except Exception as e:
        db.rollback()
        return jsonify({"error": str(e)}), 500

    mail_sent = False
    mail_error = ""
    if email:
        subject = "Deine Zugangsdaten zum Portal"
        body = build_welcome_mail(employee_name, username, password)
        try:
            send_mail(email, subject, body)
            mail_sent = True
        except Exception as e:
            mail_error = str(e)
    else:
        mail_error = "Keine E-Mail-Adresse hinterlegt."

    created_user = db.execute("SELECT * FROM users WHERE username=%s", (username,)).fetchone()
    created_user = row_to_dict(created_user) if created_user else {"username": username}
    if created_user.get("stundensatz") is None:
        created_user["stundensatz"] = ""
    created_user["language_skills"] = parse_language_skills(created_user.get("language_skills"))

    return jsonify({"status": "ok", "mail_sent": mail_sent, "mail_error": mail_error, "user": created_user})

@app.route("/users/rename", methods=["POST"])
def rename_user():
    # ✅ Sensible Personaldaten: Chef, Vorgesetzter und Vorgesetzter CP
    if normalize_role(session.get("role")) not in ["chef", "vorgesetzter", "vorgesetzter_cp"]:
        return jsonify({"error": "Nicht erlaubt"}), 403

    d = request.json or {}
    old_username = (d.get("old_username") or "").strip()
    new_username = (d.get("new_username") or "").strip()

    if not old_username or not new_username:
        return jsonify({"error": "old_username und new_username erforderlich"}), 400

    db = get_db()

    try:
        old = db.execute("SELECT * FROM users WHERE username=%s", (old_username,)).fetchone()
        if not old:
            return jsonify({"error": "Alter Benutzer nicht gefunden"}), 404

        if db.execute("SELECT 1 FROM users WHERE username=%s", (new_username,)).fetchone():
            return jsonify({"error": "Neuer Benutzername existiert schon"}), 400

        # Wichtig: In SQLite kann ein UPDATE des PK (username) scheitern,
        # wenn es Foreign-Key-Referenzen gibt (response.username -> user.username),
        # da im Schema kein ON UPDATE CASCADE definiert ist.
        # Lösung: neuen User anlegen, Referenzen umhängen, alten User löschen.
        db.execute(
            """INSERT INTO users
               (username,password,role,vorname,nachname,email,geburtsort,geburtstag,s34a,s34a_art,pschein,bewach_id,steuernummer,bsw,sanitaeter,bemerkung,is_locked,stundensatz,
                language_skills,brandschutzhelfer,deeskalation,gssk,fachkraft_ss,personenschutz,waffensachkunde,behoerdlich_studium,fuehrerschein,fuehrerschein_klassen,image_data,
                consent_given,consent_name,consent_date)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (
                new_username,
                old["password"],
                old["role"] or "mitarbeiter",
                old["vorname"] or "",
                old["nachname"] or "",
                (old.get("email") or "").strip(),
                (old.get("geburtsort") or "").strip(),
                (old.get("geburtstag") or "").strip(),
                old["s34a"] or "nein",
                normalize_s34a_art(old["s34a_art"] or ""),
                old["pschein"] or "nein",
                old["bewach_id"] or "",
                old["steuernummer"] or "",
                old["bsw"] or "nein",
                old["sanitaeter"] or "nein",
                old.get("bemerkung") or "",
                bool(old.get("is_locked") or False),
                old.get("stundensatz"),
                old.get("language_skills") or dump_language_skills({}),
                old.get("brandschutzhelfer") or "nein",
                old.get("deeskalation") or "nein",
                old.get("gssk") or "nein",
                old.get("fachkraft_ss") or "nein",
                old.get("personenschutz") or "nein",
                old.get("waffensachkunde") or "nein",
                old.get("behoerdlich_studium") or "nein",
                old.get("fuehrerschein") or "nein",
                old.get("fuehrerschein_klassen") or "",
                old.get("image_data") or "",
                bool(old.get("consent_given") or False),
                old.get("consent_name") or "",
                old.get("consent_date") or "",
            )
        )

        db.execute("UPDATE response SET username=%s WHERE username=%s", (new_username, old_username))
        db.execute("DELETE FROM users WHERE username=%s", (old_username,))

        db.commit()
        return jsonify({"status": "ok"})
    except IntegrityError as e:
        db.rollback()
        return jsonify({"error": f"Datenbankfehler: {str(e)}"}), 400
    except Exception as e:
        db.rollback()
        return jsonify({"error": f"Serverfehler: {str(e)}"}), 500



@app.route("/users/<username>", methods=["PUT"])
def edit_user(username):
    # ✅ Sensible Personaldaten: Chef, Vorgesetzter und Vorgesetzter CP
    if normalize_role(session.get("role")) not in ["chef", "vorgesetzter", "vorgesetzter_cp"]:
        return jsonify({"error": "Nicht erlaubt"}), 403

    d = request.json or {}
    db = get_db()

    u = db.execute("SELECT * FROM users WHERE username=%s", (username,)).fetchone()
    if not u:
        return jsonify({"error": "Benutzer nicht gefunden"}), 404

    updates = dict(u)
    for k in ["vorname", "nachname", "email", "geburtsort", "geburtstag", "role", "s34a", "s34a_art", "pschein",
              "bewach_id", "steuernummer", "bsw", "sanitaeter", "bemerkung", "ausweis_art", "ausweis_nr", "ausweis_behoerde", "ausweis_gueltig_bis",
              "brandschutzhelfer", "deeskalation", "gssk", "fachkraft_ss", "personenschutz",
              "waffensachkunde", "behoerdlich_studium", "fuehrerschein", "fuehrerschein_klassen", "image_data"]:
        if k in d:
            # ✅ Bugfix: Sachkunde darf beim Speichern der E-Mail nicht verschwinden.
            # Wenn Frontend ein leeres Feld sendet, behalten wir den bisherigen Wert.
            if k == "s34a_art":
                newv = normalize_s34a_art(d.get(k))
                if str(newv or "").strip() == "":
                    continue
                updates[k] = newv
            else:
                updates[k] = d[k]

    password_locked_for_viewer = (
        normalize_role(session.get("role")) in ["vorgesetzter", "vorgesetzter_cp"]
        and is_amine_salah_row(u)
    )
    if "password" in d and d["password"] is not None and not password_locked_for_viewer:
        updates["password"] = d["password"]

    if "stundensatz" in d:
        old_rate = u.get("stundensatz")
        new_rate = None if d["stundensatz"] in ("", None) else float(d["stundensatz"])
        if str(old_rate or "") != str(new_rate or ""):
            # Vergangenheit/heute sichern, bevor der Personal-Stundensatz geändert wird.
            freeze_confirmed_user_snapshots(db, username)
            # Zukunft darf den neuen Personal-Stundensatz übernehmen.
            release_future_profile_rate_snapshots(db, username)
        updates["stundensatz"] = new_rate

    if "language_skills" in d:
        updates["language_skills"] = normalize_user_payload(d)["language_skills"]

    if "image_data" in d:
        updates["image_data"] = clean_image_data(d.get("image_data"))

    extra_updates = normalize_user_payload(d)
    for k in ["brandschutzhelfer", "deeskalation", "gssk", "fachkraft_ss", "personenschutz",
              "waffensachkunde", "behoerdlich_studium", "fuehrerschein", "fuehrerschein_klassen", "image_data"]:
        if k in d:
            updates[k] = extra_updates[k]

    db.execute(
        """UPDATE users SET
           password=%s, role=%s, vorname=%s, nachname=%s, email=%s, geburtsort=%s, geburtstag=%s, s34a=%s, s34a_art=%s, pschein=%s,
           bewach_id=%s, steuernummer=%s, bsw=%s, sanitaeter=%s, bemerkung=%s, ausweis_art=%s, ausweis_nr=%s, ausweis_behoerde=%s, ausweis_gueltig_bis=%s, stundensatz=%s,
           language_skills=%s, brandschutzhelfer=%s, deeskalation=%s, gssk=%s, fachkraft_ss=%s,
           personenschutz=%s, waffensachkunde=%s, behoerdlich_studium=%s, fuehrerschein=%s, fuehrerschein_klassen=%s, image_data=%s
           WHERE username=%s""",
        (
            updates["password"], updates["role"], updates["vorname"], updates["nachname"], updates.get("email") or "", updates.get("geburtsort") or "", updates.get("geburtstag") or "",
            updates["s34a"], updates["s34a_art"], updates["pschein"],
            updates["bewach_id"], updates["steuernummer"], updates["bsw"], updates["sanitaeter"], updates.get("bemerkung") or "",
            updates.get("ausweis_art") or "", updates.get("ausweis_nr") or "", updates.get("ausweis_behoerde") or "", updates.get("ausweis_gueltig_bis") or "",
            updates["stundensatz"], updates.get("language_skills") or dump_language_skills({}),
            updates.get("brandschutzhelfer") or "nein", updates.get("deeskalation") or "nein", updates.get("gssk") or "nein", updates.get("fachkraft_ss") or "nein",
            updates.get("personenschutz") or "nein", updates.get("waffensachkunde") or "nein", updates.get("behoerdlich_studium") or "nein",
            updates.get("fuehrerschein") or "nein", updates.get("fuehrerschein_klassen") or "", clean_image_data(updates.get("image_data")), username
        )
    )
    db.commit()
    return jsonify({"status": "ok"})


@app.route("/users/<username>/lock", methods=["POST"])
def toggle_user_lock(username):
    if normalize_role(session.get("role")) not in ["chef", "vorgesetzter", "vorgesetzter_cp"]:
        return jsonify({"error": "Nicht erlaubt"}), 403

    db = get_db()
    u = db.execute("SELECT username, COALESCE(is_locked, FALSE) AS is_locked FROM users WHERE username=%s", (username,)).fetchone()
    if not u:
        return jsonify({"error": "Benutzer nicht gefunden"}), 404

    new_state = not bool(u.get("is_locked") or False)
    db.execute("UPDATE users SET is_locked=%s WHERE username=%s", (new_state, username))
    db.commit()
    return jsonify({"status": "ok", "is_locked": new_state})




@app.route("/einsatzleitung/user_extract/<event_id>/<username>", methods=["GET"])
def einsatzleitung_user_extract(event_id, username):
    """JSON-Vorschau für Einsatzleitung: Mitarbeiter-Auszug direkt im Portal anzeigen."""
    role_lc = normalize_role(session.get("role"))
    if role_lc not in ["chef", "vorgesetzter", "vorgesetzter_cp", "planner_bbs"]:
        return jsonify({"error": "Nicht erlaubt"}), 403

    db = get_db()
    ev = db.execute(
        "SELECT * FROM event WHERE id=%s",
        (event_id,),
    ).fetchone()
    if not ev:
        return jsonify({"error": "Einsatz nicht gefunden"}), 404

    if role_lc == "planner_bbs":
        assigned_leads = parse_einsatzleitung_usernames(ev.get("einsatzleitung_usernames"), ev.get("einsatzleitung_username"))
        if (session.get("username") or "").strip() not in assigned_leads:
            return jsonify({"error": "Nicht erlaubt"}), 403

    resp = db.execute(
        "SELECT * FROM response WHERE event_id=%s AND username=%s",
        (event_id, username),
    ).fetchone()
    status_lc = str((resp or {}).get("status") or "").strip().lower()
    blocked_status = {"abgelehnt", "abgelehnt_chef", "entfernt_chef"}
    if not resp or status_lc in blocked_status:
        return jsonify({"error": "Mitarbeiter ist für diesen Einsatz nicht verfügbar"}), 403

    u = db.execute("SELECT * FROM users WHERE username=%s", (username,)).fetchone()
    if not u:
        return jsonify({"error": "Benutzer nicht gefunden"}), 404

    def clean(value):
        return "" if value is None else str(value).strip()

    def yn_label(value):
        return "Ja" if str(value or "").strip().lower() == "ja" else "Nein"

    qualifications = []
    qualification_fields = [
        ("brandschutzhelfer", "Brandschutzhelfer"),
        ("deeskalation", "Deeskalation"),
        ("gssk", "GSSK"),
        ("fachkraft_ss", "Fachkraft Schutz und Sicherheit"),
        ("personenschutz", "Personenschutz"),
        ("waffensachkunde", "Waffensachkunde"),
        ("behoerdlich_studium", "Behördliches Studium"),
        ("fuehrerschein", "Führerschein"),
    ]
    for col, label in qualification_fields:
        if str(u.get(col) or "").strip().lower() == "ja":
            qualifications.append(label)
    if clean(u.get("fuehrerschein_klassen")):
        qualifications.append("Führerschein: " + clean(u.get("fuehrerschein_klassen")))

    full_name = f"{clean(u.get('vorname'))} {clean(u.get('nachname'))}".strip() or clean(username)

    return jsonify({
        "event": {
            "id": clean(ev.get("id")),
            "title": clean(ev.get("title")),
            "start": clean(ev.get("start")),
            "end": clean(ev.get("end")),
            "ort": clean(ev.get("ort")),
            "category": clean(ev.get("category")) or "CV",
            "dienstkleidung": clean(ev.get("dienstkleidung")),
            "auftrag": clean(ev.get("auftrag")),
            "planned_end_time": clean(ev.get("planned_end_time")),
        },
        "response": {
            "status": clean(resp.get("status")),
            "start_time": clean(resp.get("start_time")),
            "end_time": clean(resp.get("end_time")),
            "remark": clean(resp.get("remark")),
        },
        "user": {
            "username": clean(u.get("username")),
            "full_name": full_name,
            "vorname": clean(u.get("vorname")),
            "nachname": clean(u.get("nachname")),
            "geburtstag": clean(u.get("geburtstag")),
            "geburtsort": clean(u.get("geburtsort")),
            "ausweis_art": clean(u.get("ausweis_art")),
            "ausweis_nr": clean(u.get("ausweis_nr")),
            "s34a": yn_label(u.get("s34a")),
            "s34a_art": clean(u.get("s34a_art")),
            "bewach_id": clean(u.get("bewach_id")),
            "steuernummer": clean(u.get("steuernummer")),
            "language_skills": parse_language_skills(u.get("language_skills")),
            "qualifications": qualifications,
            "image_data": clean_image_data(u.get("image_data")),
        }
    })


@app.route("/users/<username>/pdf", methods=["GET"])
def user_pdf(username):
    role_lc = normalize_role(session.get("role"))
    if role_lc not in ["chef", "vorgesetzter", "vorgesetzter_cp", "planner_bbs"]:
        return jsonify({"error": "Nicht erlaubt"}), 403

    pdf_type = (request.args.get("pdf_type") or "CV").strip().upper()
    if pdf_type not in ("CV", "CP"):
        pdf_type = "CV"

    db = get_db()

    if role_lc == "planner_bbs":
        # Einsatzleitung darf PDF-Auszüge nur für Mitarbeiter sehen,
        # die in einem ihr zugewiesenen CV-Einsatz eingetragen und nicht abgelehnt/entfernt sind.
        event_id = (request.args.get("event_id") or "").strip()
        if not event_id:
            return jsonify({"error": "Einsatz fehlt"}), 403

        ev = db.execute(
            "SELECT id, category, einsatzleitung_username, einsatzleitung_usernames FROM event WHERE id=%s",
            (event_id,),
        ).fetchone()
        if not ev:
            return jsonify({"error": "Einsatz nicht gefunden"}), 404

        assigned_leads = parse_einsatzleitung_usernames(ev.get("einsatzleitung_usernames"), ev.get("einsatzleitung_username"))
        if (session.get("username") or "").strip() not in assigned_leads:
            return jsonify({"error": "Nicht erlaubt"}), 403
        if (ev.get("category") or "CP").strip().upper() != "CV":
            return jsonify({"error": "Nicht erlaubt"}), 403

        resp = db.execute(
            "SELECT status FROM response WHERE event_id=%s AND username=%s",
            (event_id, username),
        ).fetchone()
        status_lc = str((resp or {}).get("status") or "").strip().lower()
        blocked_status = {"abgelehnt", "abgelehnt_chef", "entfernt_chef"}
        if not resp or status_lc in blocked_status:
            return jsonify({"error": "Mitarbeiter ist für diesen Einsatz nicht verfügbar"}), 403

    u = db.execute("SELECT * FROM users WHERE username=%s", (username,)).fetchone()
    if not u:
        return jsonify({"error": "Benutzer nicht gefunden"}), 404

    from flask import send_file
    import base64

    def yn(value):
        return "Ja" if str(value or "").strip().lower() == "ja" else "Nein"

    def clean_text(value, fallback="-"):
        value = str(value or "").strip()
        return value if value else fallback

    def fmt_date_de(value):
        value = (value or "").strip()
        if not value:
            return "-"
        try:
            return datetime.fromisoformat(value.replace("Z", "")).strftime("%d.%m.%Y")
        except Exception:
            return value

    def draw_wrapped(c, text, x, y, max_width, line_height=12, font_name="Helvetica", font_size=10, color=colors.black):
        c.setFont(font_name, font_size)
        c.setFillColor(color)
        words = str(text or "-").split()
        if not words:
            c.drawString(x, y, "-")
            return y - line_height
        line = ""
        for word in words:
            test = word if not line else f"{line} {word}"
            if stringWidth(test, font_name, font_size) <= max_width:
                line = test
            else:
                if line:
                    c.drawString(x, y, line)
                    y -= line_height
                line = word
        if line:
            c.drawString(x, y, line)
            y -= line_height
        return y

    def wrap_lines(text, max_width, font_name="Helvetica", font_size=10):
        words = str(text or "-").split()
        if not words:
            return ["-"]
        lines, line = [], ""
        for word in words:
            test = word if not line else f"{line} {word}"
            if stringWidth(test, font_name, font_size) <= max_width:
                line = test
            else:
                if line:
                    lines.append(line)
                line = word
        if line:
            lines.append(line)
        return lines or ["-"]

    def rounded_card(c, x, y_top, w, h, title, accent="#111827"):
        """Clean modern card: dark header, neutral border/shadow, no light-blue glow."""
        y = y_top - h

        # neutral shadow (no light blue)
        c.setFillColor(colors.HexColor("#e5e7eb"))
        c.roundRect(x + 1.6, y - 2.0, w, h, 12, stroke=0, fill=1)

        c.setFillColor(colors.HexColor("#ffffff"))
        c.setStrokeColor(colors.HexColor("#111827"))
        c.setLineWidth(0.7)
        c.roundRect(x, y, w, h, 12, stroke=1, fill=1)

        header_h = 34
        c.setFillColor(colors.HexColor(accent))
        c.roundRect(x, y_top - header_h, w, header_h, 12, stroke=0, fill=1)
        c.rect(x, y_top - header_h, w, 12, stroke=0, fill=1)

        # subtle white accent instead of light blue
        c.setFillColor(colors.HexColor("#ffffff"))
        c.roundRect(x + 13, y_top - header_h + 9, 3.0, header_h - 18, 2, stroke=0, fill=1)

        c.setFont("Helvetica-Bold", 11.0)
        c.setFillColor(colors.white)
        c.drawString(x + 24, y_top - 21.5, title.upper())

        return y_top - header_h - 18

    def measure_label_value_rows_height(rows, w, label_w=150, value_size=10.2):
        """Benötigte Höhe für Basisdaten berechnen, damit nichts aus der Karte läuft."""
        inner_r = w - 18
        value_x = 18 + label_w
        max_w = max(40, inner_r - value_x)
        heights = []
        for _label, value in rows:
            lines = wrap_lines(value, max_w, "Helvetica", value_size)[:2]
            heights.append(max(21.0, 11.0 + (len(lines) * 10.2)))
        return sum(heights)

    def draw_label_value_rows(c, rows, x, y, w, label_w=150, label_size=9.2, value_size=10.2):
        inner_l = x + 18
        inner_r = x + w - 18
        value_x = inner_l + label_w
        max_w = max(40, inner_r - value_x)
        current_y = y

        for idx, (label, value) in enumerate(rows):
            lines = wrap_lines(value, max_w, "Helvetica", value_size)[:2]
            row_h = max(21.0, 11.0 + (len(lines) * 10.2))


            c.setFont("Helvetica-Bold", label_size)
            c.setFillColor(colors.HexColor("#111827"))
            c.drawString(inner_l, current_y, f"{label}")

            c.setFont("Helvetica", value_size)
            c.setFillColor(colors.HexColor("#111827"))
            for i, line in enumerate(lines):
                c.drawString(value_x, current_y - (i * 10.2), line)

            current_y -= row_h

        return current_y

    def draw_pill(c, text, x, y, font_name="Helvetica", font_size=8.1, pad_x=8, pad_y=4, fill="#f8fafc", stroke="#d0d7e2", text_color="#1f2937", dot_color=None, max_w=None):
        """Draw one badge/pill that grows with the text width and returns its size."""
        text = str(text or "-").strip() or "-"
        available_text_w = None
        if max_w is not None:
            available_text_w = max(18, max_w - (pad_x * 2) - (8 if dot_color else 0))
            lines = wrap_lines(text, available_text_w, font_name, font_size)
            text = lines[0]
            if len(lines) > 1 and len(text) > 1:
                while stringWidth(text + "...", font_name, font_size) > available_text_w and len(text) > 1:
                    text = text[:-1]
                text = text.rstrip() + "..."

        text_w = stringWidth(text, font_name, font_size)
        dot_w = 8 if dot_color else 0
        pill_w = text_w + (pad_x * 2) + dot_w
        if max_w is not None:
            pill_w = min(pill_w, max_w)
        pill_h = font_size + (pad_y * 2)

        c.setFillColor(colors.HexColor(fill))
        c.setStrokeColor(colors.HexColor(stroke))
        c.setLineWidth(0.7)
        c.roundRect(x, y - pill_h + 3, pill_w, pill_h, 8, stroke=1, fill=1)

        text_x = x + pad_x
        if dot_color:
            c.setFillColor(colors.HexColor(dot_color))
            c.circle(x + pad_x - 1, y - (pill_h / 2) + 3, 2.0, stroke=0, fill=1)
            text_x += dot_w

        c.setFont(font_name, font_size)
        c.setFillColor(colors.HexColor(text_color))
        c.drawString(text_x, y - font_size + 1.5, text)
        return pill_w, pill_h

    def draw_chip_list(c, x, y_top, w, title, items, min_height=100, accent="#111827"):
        """Modern neutral qualification list. Long items wrap instead of being cut off."""
        items = [str(v or "-").strip() or "-" for v in items]
        inner_x = x + 18
        inner_w = w - 36
        font_name = "Helvetica"
        font_size = 10.0
        line_h = 12.0
        gap_y = 8

        prepared = [wrap_lines(item, inner_w - 42, font_name, font_size) for item in items]
        row_heights = [max(28, 14 + len(lines) * line_h) for lines in prepared]
        box_h = max(min_height, 58 + sum(row_heights) + gap_y * max(0, len(items) - 1) + 18)
        rounded_card(c, x, y_top, w, box_h, title, accent)

        current_y = y_top - 58
        for idx, lines in enumerate(prepared):
            row_h = row_heights[idx]
            c.setFillColor(colors.HexColor("#ffffff"))
            c.setStrokeColor(colors.HexColor("#9ca3af"))
            c.setLineWidth(0.9)
            c.roundRect(inner_x, current_y - row_h + 5, inner_w, row_h, 9, stroke=1, fill=1)

            c.setFillColor(colors.HexColor("#111827"))
            c.circle(inner_x + 16, current_y - 11, 3.2, stroke=0, fill=1)

            c.setFont(font_name, font_size)
            c.setFillColor(colors.HexColor("#111827"))
            text_y = current_y - 15.0
            for line in lines:
                c.drawString(inner_x + 34, text_y, line)
                text_y -= line_h

            current_y -= row_h + gap_y

        return y_top - box_h

    def draw_language_box(c, x, y_top, w, title, rows, min_height=120, accent="#111827"):
        """Show every selected language from Personal. No headers, no truncation, neutral styling."""
        prepared = []
        for lang, level in rows:
            lang = clean_text(lang)
            level = clean_text(level)
            if not level or level == "-":
                level = "Verhandlungssicher in Wort und Schrift"
            prepared.append((lang, level))

        if not prepared:
            prepared = [("Keine Fremdsprache ausgewählt", "Verhandlungssicher in Wort und Schrift")]

        inner_x = x + 14
        inner_w = w - 28
        lang_font = "Helvetica-Bold"
        level_font = "Helvetica"
        lang_size = 9.6
        level_size = 8.6
        line_h = 11.0
        gap_y = 9

        measured = []
        for lang, level in prepared:
            # Etwas mehr nutzbare Breite und Höhe, damit lange Sprachlevel sauber in der Umrandung bleiben.
            lang_lines = wrap_lines(lang, inner_w - 34, lang_font, lang_size)
            level_lines = wrap_lines(level, inner_w - 34, level_font, level_size)
            row_h = max(44, 22 + (len(lang_lines) * line_h) + (len(level_lines) * line_h))
            measured.append((lang_lines, level_lines, row_h))

        box_h = max(min_height, 62 + sum(m[2] for m in measured) + gap_y * max(0, len(measured) - 1) + 28)
        rounded_card(c, x, y_top, w, box_h, title, accent)

        current_y = y_top - 58
        for idx, (lang_lines, level_lines, row_h) in enumerate(measured):
            c.setFillColor(colors.HexColor("#ffffff"))
            c.setStrokeColor(colors.HexColor("#9ca3af"))
            c.setLineWidth(0.9)
            c.roundRect(inner_x, current_y - row_h + 5, inner_w, row_h, 9, stroke=1, fill=1)

            c.setFillColor(colors.HexColor("#111827"))
            c.circle(inner_x + 16, current_y - 13, 3.2, stroke=0, fill=1)

            text_y = current_y - 15.5
            c.setFont(lang_font, lang_size)
            c.setFillColor(colors.HexColor("#111827"))
            for line in lang_lines:
                c.drawString(inner_x + 34, text_y, line)
                text_y -= line_h

            c.setFont(level_font, level_size)
            c.setFillColor(colors.HexColor("#374151"))
            for line in level_lines:
                c.drawString(inner_x + 34, text_y, line)
                text_y -= line_h

            current_y -= row_h + gap_y

        return y_top - box_h

    language_skills = parse_language_skills(u.get("language_skills"))
    language_rows = [(str(lang).strip(), str(level).strip()) for lang, level in language_skills.items() if str(lang).strip()]
    if not language_rows:
        language_rows = [("Keine Fremdsprache ausgewählt", "")]

    qual_values = []
    for label, key in [
        ("Ersthelfer/-in", "brandschutzhelfer"),
        ("Rettungssanitäter", "sanitaeter"),
        ("Deeskalationslehrgang", "deeskalation"),
        ("Geprüfte Schutz- und Sicherheitskraft (GSSK)", "gssk"),
        ("Fachkraft für Schutz und Sicherheit", "fachkraft_ss"),
        ("Personenschutz", "personenschutz"),
        ("Waffensachkunde / Berufswaffenträger/-in", "waffensachkunde"),
        ("Behördliches Studium", "behoerdlich_studium"),
        ("BSW", "bsw"),
        ("P-Schein", "pschein"),
    ]:
        if yn(u.get(key)) == "Ja":
            qual_values.append(label)

    fuehrerschein_text = yn(u.get("fuehrerschein"))
    if fuehrerschein_text == "Ja":
        klassen = clean_text(u.get('fuehrerschein_klassen'), '')
        qual_values.append(f"Führerschein{f' – Klasse {klassen}' if klassen else ''}")

    if not qual_values:
        qual_values = ["-"]

    full_name = f"{(u.get('vorname') or '').strip()} {(u.get('nachname') or '').strip()}".strip() or username
    s34a_flag = yn(u.get("s34a"))
    s34a_art = clean_text(u.get("s34a_art"), "")
    if s34a_flag == "Ja":
        art_lc = s34a_art.strip().lower()
        if art_lc == "sachkunde":
            s34a_text = "Sachkunde"
        elif art_lc == "unterrichtung":
            s34a_text = "Unterrichtung"
        else:
            s34a_text = "Ja"
    else:
        s34a_text = "Nein"

    buffer = io.BytesIO()
    pdf = canvas.Canvas(buffer, pagesize=A4)
    width, height = A4
    margin = 34
    content_w = width - 2 * margin
    static_dir = os.path.join(app.root_path, "static")
    logo_label = "CV logo" if pdf_type == "CV" else "CP logo"
    if pdf_type == "CV":
        logo_path = os.path.join(static_dir, "casutt_logo.jpeg")
    else:
        logo_path = os.path.join(static_dir, "CP-Logo.png")
    if not os.path.exists(logo_path):
        logo_path = ""

    pdf.setTitle(f"Mitarbeiter_{username}")
    pdf.setAuthor("CV Planung")
    pdf.setSubject("Mitarbeiterprofil")

    header_y = height - 28
    pdf.setFont("Helvetica-Bold", 15)
    pdf.setFillColor(colors.HexColor("#1f2937"))
    pdf.drawString(margin, header_y, "Mitarbeiterprofil")
    pdf.setFont("Helvetica", 8)
    pdf.setFillColor(colors.HexColor("#6b7280"))
    berlin_now = datetime.now(ZoneInfo("Europe/Berlin"))
    pdf.drawString(margin, header_y - 12, f"Export am {berlin_now.strftime('%d.%m.%Y, %H:%M Uhr')}")
    header_logo_w = 200
    header_logo_h = 80
    header_logo_x = width - margin - header_logo_w
    header_logo_y = header_y - 18
    if logo_path:
        try:
            pdf.drawImage(logo_path, header_logo_x, header_logo_y, header_logo_w, header_logo_h, preserveAspectRatio=True, mask='auto', anchor='c')
        except Exception:
            pdf.setStrokeColor(colors.HexColor("#d2d7df"))
            pdf.setFillColor(colors.white)
            pdf.roundRect(header_logo_x, header_logo_y, header_logo_w, header_logo_h, 6, stroke=1, fill=1)
            pdf.setFont("Helvetica-Bold", 10)
            pdf.setFillColor(colors.HexColor("#111827"))
            pdf.drawCentredString(header_logo_x + header_logo_w / 2, header_logo_y + 11, logo_label)
    else:
        pdf.setStrokeColor(colors.HexColor("#d2d7df"))
        pdf.setFillColor(colors.white)
        pdf.roundRect(header_logo_x, header_logo_y, header_logo_w, header_logo_h, 6, stroke=1, fill=1)
        pdf.setFont("Helvetica-Bold", 10)
        pdf.setFillColor(colors.HexColor("#111827"))
        pdf.drawCentredString(header_logo_x + header_logo_w / 2, header_logo_y + 11, logo_label)

    top_y = height - 70
    left_w = content_w * 0.56
    gap = 14
    right_w = content_w - left_w - gap
    right_x = margin + left_w + gap

    # Basisdaten links oben - professioneller Karten-Look
    basis_rows = [
        ("Vorname", clean_text(u.get("vorname"))),
        ("Nachname", clean_text(u.get("nachname"))),
        ("Geburtstag", fmt_date_de(u.get("geburtstag"))),
        ("Geburtsort", clean_text(u.get("geburtsort"))),
        ("Amtl. Dokument", clean_text(u.get("ausweis_art"))),
        ("Dokumentennr.", clean_text(u.get("ausweis_nr"))),
        ("§ 34a GewO", s34a_text),
        ("Bewacher ID", clean_text(u.get("bewach_id"))),
    ]
    basis_rows_h = measure_label_value_rows_height(basis_rows, left_w, label_w=128, value_size=9.5)
    basis_h = max(224, 34 + 18 + basis_rows_h + 20)
    basis_content_y = rounded_card(pdf, margin, top_y, left_w, basis_h, "Basisdaten", accent="#111827")
    draw_label_value_rows(pdf, basis_rows, margin, basis_content_y, left_w, label_w=128, label_size=9.0, value_size=9.5)

    # Bild rechts oben – gleiche Höhe wie Basisdaten, damit die darunterliegenden Karten sauber starten
    img_h = basis_h
    img_y = top_y - img_h
    pdf.setStrokeColor(colors.HexColor("#111827"))
    pdf.setLineWidth(0.9)
    pdf.setFillColor(colors.white)
    pdf.roundRect(right_x, img_y, right_w, img_h, 10, stroke=1, fill=1)

    img_value = (u.get("image_data") or "").strip()
    drawn_image = False
    if img_value.startswith("data:image/") and ";base64," in img_value:
        try:
            raw = base64.b64decode(img_value.split(",", 1)[1])
            reader = ImageReader(io.BytesIO(raw))
            iw, ih = reader.getSize()
            pad = 10
            max_w = right_w - 2 * pad
            max_h = img_h - 2 * pad
            scale = min(max_w / iw, max_h / ih)
            draw_w = iw * scale
            draw_h = ih * scale
            draw_x = right_x + (right_w - draw_w) / 2
            draw_y = img_y + (img_h - draw_h) / 2
            pdf.drawImage(reader, draw_x, draw_y, draw_w, draw_h, preserveAspectRatio=True, mask='auto')
            drawn_image = True
        except Exception:
            drawn_image = False
    if not drawn_image:
        pdf.setFillColor(colors.HexColor("#f8fafc"))
        pdf.roundRect(right_x + 10, img_y + 10, right_w - 20, img_h - 20, 8, stroke=0, fill=1)
        pdf.setFillColor(colors.HexColor("#6b7280"))
        pdf.setFont("Helvetica-Bold", 12)
        pdf.drawCentredString(right_x + right_w / 2, img_y + img_h / 2 + 4, "Kein Bild")
        pdf.setFont("Helvetica", 9)
        pdf.drawCentredString(right_x + right_w / 2, img_y + img_h / 2 - 10, "Kein Foto hinterlegt")

    lower_top = img_y - 22
    left_bottom = draw_chip_list(pdf, margin, lower_top, left_w, "Qualifikationen", qual_values, min_height=190, accent="#111827")

    right_items = [(lang, level or "Verhandlungssicher in Wort und Schrift") for lang, level in language_rows]
    right_bottom = draw_language_box(pdf, right_x, lower_top, right_w, "Fremdsprachen", right_items, min_height=220, accent="#111827")

    pdf.save()
    buffer.seek(0)
    preview = str(request.args.get("preview") or "").strip().lower() in ("1", "true", "ja", "yes")
    return send_file(buffer, mimetype="application/pdf", as_attachment=not preview, download_name=f"mitarbeiter_{username}.pdf")


@app.route("/users/<username>", methods=["DELETE"])
def delete_user(username):
    # ✅ Sensible Personaldaten: Chef, Vorgesetzter und Vorgesetzter CP
    if normalize_role(session.get("role")) not in ["chef", "vorgesetzter", "vorgesetzter_cp"]:
        return jsonify({"error": "Nicht erlaubt"}), 403
    db = get_db()
    db.execute("DELETE FROM users WHERE username=%s", (username,))
    db.commit()
    return jsonify({"status": "ok"})


@app.route("/invoice/current_user", methods=["GET"])
def invoice_current_user():
    if "username" not in session:
        return jsonify({"error": "Nicht eingeloggt"}), 403
    if session.get("role") != "mitarbeiter":
        return jsonify({"error": "Nicht erlaubt"}), 403
    if not is_amine_salah_user():
        return jsonify({"error": "Rechnung ist nur für diesen Mitarbeiter verfügbar"}), 403
    if employee_requires_consent():
        return jsonify({"error":"Bitte zuerst im Report in die Datenverarbeitung einwilligen."}), 403

    month_raw = (request.args.get("month") or "").strip()
    category = (request.args.get("category") or "CV").strip().upper()
    invoice_number = (request.args.get("invoice_number") or "").strip()
    if category == "BS":
        category = "CV"
    if not invoice_number:
        return jsonify({"error": "Bitte eine Rechnungsnummer angeben."}), 400

    try:
        year, month = [int(x) for x in month_raw.split("-", 1)]
        if month < 1 or month > 12:
            raise ValueError
    except Exception:
        return jsonify({"error": "Monat ungültig"}), 400

    db = get_db()
    entries = build_invoice_entries_for_user(db, session.get("username"), year, month, category)
    if not entries:
        return jsonify({"error": "Für den gewählten Monat und die gewählte Kategorie wurden keine abrechenbaren Einsätze gefunden."}), 404

    company_map = {
        "CV": {
            "label": "",
            "recipient_name": "Kevin Casutt",
            "recipient_company": "Casutt Veranstaltungsservice",
            "recipient_address_1": "Dörpfeldstr. 75",
            "recipient_address_2": "12489 Berlin",
            "mail": "kontakt@casutt-veranstaltungsservice.de",
        },
        "CP": {
            "label": "",
            "recipient_name": "Lucas Pfennig",
            "recipient_company": "CP-Security-Solutions",
            "recipient_address_1": "Lehnitzstr. 103",
            "recipient_address_2": "12623 Berlin",
            "mail": "contact@cp-security-solutions.de",
        },
        "HB": {
            
            "recipient_name": "Hibex Sicherheit & Service",
            "recipient_company": "Vagif Shamailov",
            "recipient_address_1": "Mahlower Straße 24",
            "recipient_address_2": "12049 Berlin",
            "mail": "",
        }
    }
    recipient = company_map.get(category, {
        "label": "",
        "recipient_name": category,
        "recipient_company": category,
        "recipient_address_1": "",
        "recipient_address_2": "",
        "mail": "",
    })

    sender = {
        "name": "Amine Salah",
        "name_top": "AMINE, SALAH",
        "signature_name": "Amine Salah",
        "street": "Hugo-Wolf-Steig 7",
        "zip_city": "12557 Berlin",
        "tax_no": "16/503/01534",
        "tax_office": "Berlin Bezirk Neukölln",
        "bank": "N26",
        "iban": "DE85 1001 1001 2823 1738 75",
        "bic": "NTSBDEB1XXX",
    }

    invoice_date = datetime(year, month, calendar.monthrange(year, month)[1])
    total_amount = sum((e.get("grand_total", e["total"]) for e in entries), Decimal("0.00"))

    from flask import send_file
    buffer = io.BytesIO()
    pdf = canvas.Canvas(buffer, pagesize=A4)
    width, height = A4

    margin_left = 42
    margin_right = 42
    usable_width = width - margin_left - margin_right

    def draw_text(txt, x, yv, size=11, font="Helvetica", color=None):
        if color is not None:
            pdf.setFillColor(color)
        pdf.setFont(font, size)
        pdf.drawString(x, yv, str(txt))
        if color is not None:
            pdf.setFillColor(colors.black)

    def draw_right(txt, x, yv, size=11, font="Helvetica", color=None):
        if color is not None:
            pdf.setFillColor(color)
        pdf.setFont(font, size)
        pdf.drawRightString(x, yv, str(txt))
        if color is not None:
            pdf.setFillColor(colors.black)

    blue = colors.HexColor("#2F75B5")
    dark = colors.HexColor("#3A3A3A")

    # top line
    pdf.setStrokeColor(blue)
    pdf.setLineWidth(2.2)
    pdf.line(margin_left, height - 28, width - margin_right, height - 28)
    pdf.setStrokeColor(colors.black)
    pdf.setLineWidth(1)

    # header block like template
    draw_text(sender["name_top"], margin_left, height - 46, 16, "Helvetica-Bold", blue)
    draw_text(invoice_date.strftime("%d.%m.%Y"), margin_left, height - 72, 14, "Helvetica-Bold", blue)
    draw_text("RECHNUNG", margin_left, height - 104, 18, "Helvetica")
    draw_text(invoice_number, margin_left, height - 132, 18, "Helvetica")

    # sender left / recipient right
    left_y = height - 182
    draw_text(sender["signature_name"], margin_left, left_y, 10.5, "Helvetica", blue)
    draw_text(sender["street"], margin_left, left_y - 18, 10.5, "Helvetica", colors.HexColor("#666666"))
    draw_text(sender["zip_city"], margin_left, left_y - 36, 10.5, "Helvetica", colors.HexColor("#666666"))
    draw_text("Steuernummer:", margin_left, left_y - 78, 10.5, "Helvetica", colors.HexColor("#666666"))
    draw_text(sender["tax_no"], margin_left, left_y - 96, 10.5, "Helvetica", colors.HexColor("#666666"))
    draw_text("Finanzamt:", margin_left, left_y - 138, 10.5, "Helvetica", colors.HexColor("#666666"))
    draw_text(sender["tax_office"], margin_left, left_y - 156, 10.5, "Helvetica", colors.HexColor("#666666"))

    right_x = 155
    right_y = height - 134

    # Empfängerblock: bei HB darf kein "HB" im PDF stehen.
    recipient_lines = []
    if category in ("CV", "CP") and recipient.get("label"):
        recipient_lines.append((recipient.get("label"), 12.5, "Helvetica-Bold"))
    for key in ("recipient_name", "recipient_company", "recipient_address_1", "recipient_address_2"):
        value = (recipient.get(key) or "").strip()
        if value:
            recipient_lines.append((value, 10.5, "Helvetica"))

    for idx, (line, size, font) in enumerate(recipient_lines):
        draw_text(line, right_x, right_y - (idx * 18), size, font)

    headline_y = height - 338
    draw_text(f"Für meinen Service im {month_label_de(year, month)} stelle ich Ihnen folgende Summe in", right_x, headline_y, 10.8, "Helvetica")
    draw_text("Rechnung:", right_x, headline_y - 19, 10.8, "Helvetica")

    # table (sauber mit automatischem Zeilenumbruch)
    table_x = right_x
    table_y = headline_y - 64
    table_width = width - table_x - margin_right
    col_widths = [table_width * 0.52, table_width * 0.17, table_width * 0.15, table_width * 0.16]

    styles = getSampleStyleSheet()
    desc_style = ParagraphStyle(
        "InvoiceDesc",
        parent=styles["Normal"],
        fontName="Helvetica",
        fontSize=9.6,
        leading=12,
        alignment=TA_LEFT,
        wordWrap="CJK",
        splitLongWords=True,
        spaceAfter=0,
        spaceBefore=0,
    )
    center_style = ParagraphStyle(
        "InvoiceCenter",
        parent=desc_style,
        alignment=TA_CENTER,
    )
    right_style = ParagraphStyle(
        "InvoiceRight",
        parent=desc_style,
        alignment=TA_RIGHT,
    )
    header_style = ParagraphStyle(
        "InvoiceHeader",
        parent=desc_style,
        fontName="Helvetica-Bold",
        fontSize=10.2,
        leading=12,
        textColor=colors.white,
    )
    total_style = ParagraphStyle(
        "InvoiceTotal",
        parent=desc_style,
        fontName="Helvetica-Bold",
        fontSize=10.2,
        leading=12,
        alignment=TA_RIGHT,
    )

    def esc_pdf_text(value):
        value = str(value or "")
        return (value.replace("&", "&amp;")
                     .replace("<", "&lt;")
                     .replace(">", "&gt;"))

    table_data = [[
        Paragraph("Beschreibung, Datum", header_style),
        Paragraph("Stunden", header_style),
        Paragraph("€", header_style),
        Paragraph("Summe", header_style),
    ]]

    for entry in entries:
        table_data.append([
            Paragraph(esc_pdf_text(f"Eventbetreuung – {entry['title']} – {entry['date'].strftime('%d.%m.%Y')}"), desc_style),
            Paragraph(esc_pdf_text(str(entry["hours"]).replace(".", ",")), center_style),
            Paragraph(esc_pdf_text(format_rate_eur(entry["rate"])), center_style),
            Paragraph(esc_pdf_text(format_eur(entry["total"])), right_style),
        ])
        for cost in entry.get("extra_costs", []):
            label = (cost.get("label") or "Zusatzkosten").strip()
            desc2 = f"Zusatzkosten – {label}"
            if (cost.get("description") or "").strip():
                desc2 += f" ({(cost.get('description') or '').strip()})"
            table_data.append([
                Paragraph(esc_pdf_text(desc2), desc_style),
                Paragraph("", center_style),
                Paragraph("", center_style),
                Paragraph(esc_pdf_text(format_eur(cost.get("amount"))), right_style),
            ])

    table_data.append([
        Paragraph("", desc_style),
        Paragraph("", center_style),
        Paragraph("Gesamt:", total_style),
        Paragraph(esc_pdf_text(format_eur(total_amount)), right_style),
    ])

    invoice_table = Table(table_data, colWidths=col_widths, repeatRows=1, hAlign="LEFT")
    invoice_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), blue),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("GRID", (0, 0), (-1, -1), 0.8, colors.black),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("LEFTPADDING", (0, 0), (-1, -1), 7),
        ("RIGHTPADDING", (0, 0), (-1, -1), 7),
        ("TOPPADDING", (0, 0), (-1, -1), 6),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
        ("FONTNAME", (2, -1), (2, -1), "Helvetica-Bold"),
        ("ALIGN", (1, 1), (1, -1), "CENTER"),
        ("ALIGN", (2, 1), (2, -2), "CENTER"),
        ("ALIGN", (3, 1), (3, -1), "RIGHT"),
    ]))

    # Tabelle mit automatischem Seitenumbruch zeichnen. Dadurch landen Linien nie auf Text.
    current_y = table_y
    available_width = table_width
    bottom_limit = 165
    footer_reserved = 135
    parts = [invoice_table]
    while parts:
        part = parts.pop(0)
        available_height = current_y - bottom_limit - (footer_reserved if not parts else 0)
        if available_height < 120:
            pdf.showPage()
            current_y = height - 56
            available_height = current_y - bottom_limit

        split_parts = part.split(available_width, available_height)
        if not split_parts:
            pdf.showPage()
            current_y = height - 56
            parts.insert(0, part)
            continue

        draw_part = split_parts[0]
        remaining_parts = split_parts[1:]
        w_tbl, h_tbl = draw_part.wrap(available_width, available_height)
        draw_part.drawOn(pdf, table_x, current_y - h_tbl)
        current_y = current_y - h_tbl

        if remaining_parts:
            parts = remaining_parts + parts
            pdf.showPage()
            current_y = height - 56

    footer_y = current_y - 36
    footer_lines = [
        "Es wird gemäß §19 Abs. 1 Umsatzsteuergesetz keine Umsatzsteuer erhoben.",
        "Der Gesamtbetrag ist ab Erhalt dieser Rechnung zahlbar innerhalb von 14 Tagen ohne",
        "Abzug. Wenn nicht anders angegeben entspricht das Leistungsdatum dem",
        "Rechnungsdatum.",
        "Ich bedanke mich für die Zusammenarbeit.",
        "",
        "Mit freundlichen Grüßen",
        "",
        sender["signature_name"],
    ]
    pdf.setFont("Helvetica", 9.8)
    for line in footer_lines:
        if line == "":
            footer_y -= 14
            continue
        pdf.drawString(right_x, footer_y, line)
        footer_y -= 15

    # bank details at the very bottom as requested
    bank_y = 88
    label_color = colors.HexColor("#666666")
    draw_text("Bankverbindung:", margin_left, bank_y, 10.5, "Helvetica", label_color)
    draw_right(sender["bank"], width - margin_right, bank_y, 10.5, "Helvetica", label_color)
    draw_text("IBAN:", margin_left, bank_y - 18, 10.5, "Helvetica", label_color)
    draw_right(sender["iban"], width - margin_right, bank_y - 18, 10.5, "Helvetica", label_color)
    draw_text("BIC:", margin_left, bank_y - 36, 10.5, "Helvetica", label_color)
    draw_right(sender["bic"], width - margin_right, bank_y - 36, 10.5, "Helvetica", label_color)

    pdf.save()
    buffer.seek(0)
    filename = f"rechnung_{sender['signature_name'].lower().replace(' ', '_')}_{year}_{month:02d}_{category}.pdf"

    # PDF zusätzlich lokal speichern.
    # Standard-Ziel: C:\Users\Admin\OneDrive\Desktop\Abrechnung\CV|CP|HB
    # Kann bei Bedarf per Umgebungsvariable INVOICE_OUTPUT_BASE überschrieben werden.
    saved_path = ""
    try:
        output_base = os.environ.get("INVOICE_OUTPUT_BASE", r"C:\Users\Admin\OneDrive\Desktop\Abrechnung")
        output_dir = os.path.join(output_base, category)
        os.makedirs(output_dir, exist_ok=True)
        saved_path = os.path.join(output_dir, filename)
        with open(saved_path, "wb") as f:
            f.write(buffer.getvalue())
        buffer.seek(0)
    except Exception as exc:
        app.logger.warning("PDF konnte nicht lokal gespeichert werden: %s", exc)
        buffer.seek(0)

    response = send_file(buffer, mimetype="application/pdf", as_attachment=True, download_name=filename)
    if saved_path:
        response.headers["X-Saved-Path"] = saved_path
    return response



# ---------------- Accounting API (nur Amine Saleh) ----------------


def current_user_can_see_driver() -> bool:
    """Fahrer-Reiter ist ausschließlich für Amine Salah in der Mitarbeiteransicht aktiv."""
    return normalize_role(session.get("role") or "") == "mitarbeiter" and is_amine_salah_user()


def require_driver_access():
    if "username" not in session:
        return jsonify({"error": "Nicht eingeloggt"}), 403
    if not current_user_can_see_driver():
        return jsonify({"error": "Fahrer ist nur für Amine Salah verfügbar"}), 403
    if employee_requires_consent():
        return jsonify({"error": "Bitte zuerst im Report in die Datenverarbeitung einwilligen."}), 403
    return None


def _driver_photos_from_payload(value):
    """Return vehicle photos as [{data, saved_at}].

    Backward compatible: old saved rows may contain a plain list of data-URLs.
    New rows store a timestamp for every image so it can be printed in the PDF.
    """
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except Exception:
            value = []
    if not isinstance(value, list):
        value = []
    cleaned = []
    now = datetime.now().isoformat(timespec="seconds")
    for item in value[:8]:
        saved_at = ""
        raw_img = ""
        if isinstance(item, dict):
            raw_img = str(item.get("data") or item.get("image") or "")
            saved_at = str(item.get("saved_at") or item.get("created_at") or "").strip()
        else:
            raw_img = str(item or "")
        img = clean_image_data(raw_img)
        if img:
            cleaned.append({"data": img, "saved_at": saved_at or now})
    return cleaned


@app.route("/driver/profile", methods=["GET"])
def driver_profile():
    denied = require_driver_access()
    if denied:
        return denied
    db = get_db()
    u = db.execute("SELECT username, vorname, nachname, image_data FROM users WHERE username=%s", (session.get("username"),)).fetchone()
    full_name = get_session_user_full_name() or session.get("username")
    return jsonify({"username": session.get("username"), "full_name": full_name, "image_data": (u or {}).get("image_data") or ""})


@app.route("/driver/rides", methods=["GET"])
def driver_rides_get():
    denied = require_driver_access()
    if denied:
        return denied
    db = get_db()
    rows = db.execute(
        """SELECT * FROM driver_rides
           WHERE username=%s
           ORDER BY duty_date DESC NULLS LAST, departure_time DESC NULLS LAST, created_at DESC""",
        (session.get("username"),),
    ).fetchall() or []
    out = []
    for r in rows:
        d = row_to_dict(r)
        d["vehicle_photos"] = _driver_photos_from_payload(d.get("vehicle_photos"))
        out.append(d)
    return jsonify(out)


def _driver_duration_minutes(departure_time: str, arrival_time: str, fallback=0) -> int:
    start = parse_hhmm(departure_time)
    end = parse_hhmm(arrival_time)
    if not start or not end:
        return to_int(fallback, 0)
    start_minutes = start[0] * 60 + start[1]
    end_minutes = end[0] * 60 + end[1]
    if end_minutes < start_minutes:
        end_minutes += 24 * 60
    return max(0, end_minutes - start_minutes)


@app.route("/driver/rides", methods=["POST"])
def driver_rides_post():
    denied = require_driver_access()
    if denied:
        return denied
    data = request.get_json(silent=True) or request.form.to_dict() or {}
    db = get_db()
    now = datetime.now().isoformat(timespec="seconds")
    ride_id = (data.get("id") or str(uuid.uuid4())).strip()
    photos = _driver_photos_from_payload(data.get("vehicle_photos") or [])
    payload = {
        "id": ride_id,
        "username": session.get("username"),
        "driver_name": (data.get("driver_name") or get_session_user_full_name() or session.get("username") or "").strip(),
        "duty_date": (data.get("duty_date") or "").strip(),
        "service_start": (data.get("service_start") or "").strip(),
        "service_end": (data.get("service_end") or "").strip(),
        "license_plate": (data.get("license_plate") or "").strip(),
        "passenger": (data.get("passenger") or "").strip(),
        "departure_time": (data.get("departure_time") or "").strip(),
        "arrival_time": (data.get("arrival_time") or "").strip(),
        "duration_minutes": _driver_duration_minutes(data.get("departure_time"), data.get("arrival_time"), data.get("duration_minutes")),
        "destination": (data.get("destination") or "").strip(),
        "remark": (data.get("remark") or "").strip(),
        "vehicle_photos": json.dumps(photos, ensure_ascii=False),
        "created_at": now,
        "updated_at": now,
    }
    if not payload["duty_date"]:
        return jsonify({"error": "Bitte Einsatztag eintragen."}), 400
    if not payload["passenger"]:
        return jsonify({"error": "Bitte eintragen, wer gefahren wurde."}), 400

    exists = db.execute("SELECT created_at FROM driver_rides WHERE id=%s AND username=%s", (ride_id, session.get("username"))).fetchone()
    if exists:
        payload["created_at"] = exists.get("created_at") or now
        db.execute(
            """UPDATE driver_rides SET driver_name=%s, duty_date=%s, service_start=%s, service_end=%s, license_plate=%s, passenger=%s,
               departure_time=%s, duration_minutes=%s, arrival_time=%s, destination=%s, remark=%s,
               vehicle_photos=%s, updated_at=%s WHERE id=%s AND username=%s""",
            (payload["driver_name"], payload["duty_date"], payload["service_start"], payload["service_end"], payload["license_plate"], payload["passenger"],
             payload["departure_time"], payload["duration_minutes"], payload["arrival_time"], payload["destination"],
             payload["remark"], payload["vehicle_photos"], now, ride_id, session.get("username")),
        )
    else:
        db.execute(
            """INSERT INTO driver_rides
               (id, username, driver_name, duty_date, service_start, service_end, license_plate, passenger, departure_time, duration_minutes,
                arrival_time, destination, remark, vehicle_photos, created_at, updated_at)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (payload["id"], payload["username"], payload["driver_name"], payload["duty_date"], payload["service_start"], payload["service_end"], payload["license_plate"],
             payload["passenger"], payload["departure_time"], payload["duration_minutes"], payload["arrival_time"],
             payload["destination"], payload["remark"], payload["vehicle_photos"], payload["created_at"], payload["updated_at"]),
        )
    db.commit()
    payload["vehicle_photos"] = photos
    return jsonify({"status": "ok", "ride": payload})


@app.route("/driver/rides/<ride_id>", methods=["DELETE"])
def driver_rides_delete(ride_id):
    denied = require_driver_access()
    if denied:
        return denied
    db = get_db()
    db.execute("DELETE FROM driver_rides WHERE id=%s AND username=%s", (ride_id, session.get("username")))
    db.commit()
    return jsonify({"status": "ok"})


def _pdf_draw_wrapped(c, text, x, y, max_width, line_height=13, font="Helvetica", size=9):
    c.setFont(font, size)
    words = str(text or "").replace("\n", " ").split()
    if not words:
        return y
    line = ""
    for w in words:
        candidate = (line + " " + w).strip()
        if stringWidth(candidate, font, size) <= max_width:
            line = candidate
        else:
            c.drawString(x, y, line)
            y -= line_height
            line = w
    if line:
        c.drawString(x, y, line)
        y -= line_height
    return y


def _pdf_image_from_data_url(data_url):
    try:
        header, b64 = str(data_url or "").split(",", 1)
        return ImageReader(io.BytesIO(base64.b64decode(b64)))
    except Exception:
        return None


@app.route("/driver/export_pdf", methods=["GET"])
def driver_export_pdf():
    denied = require_driver_access()
    if denied:
        return denied
    db = get_db()
    ride_id = (request.args.get("id") or "").strip()
    if ride_id:
        rows = db.execute("SELECT * FROM driver_rides WHERE username=%s AND id=%s", (session.get("username"), ride_id)).fetchall() or []
    else:
        rows = db.execute("SELECT * FROM driver_rides WHERE username=%s ORDER BY duty_date ASC, departure_time ASC", (session.get("username"),)).fetchall() or []

    u = db.execute("SELECT image_data FROM users WHERE username=%s", (session.get("username"),)).fetchone() or {}
    full_name = get_session_user_full_name() or session.get("username")
    buffer = io.BytesIO()
    c = canvas.Canvas(buffer, pagesize=A4)
    width, height = A4

    def header():
        c.setFillColor(colors.HexColor("#111827"))
        c.setFont("Helvetica-Bold", 18)
        c.drawString(36, height - 42, "Fahrer-Report")
        c.setFont("Helvetica", 10)
        c.drawString(36, height - 60, f"Mitarbeiter: {full_name}")
        c.drawString(36, height - 75, f"Erstellt am: {datetime.now().strftime('%d.%m.%Y %H:%M')}")
        img = _pdf_image_from_data_url((u or {}).get("image_data") or "")
        if img:
            try:
                c.drawImage(img, width - 120, height - 105, 76, 76, preserveAspectRatio=True, mask='auto')
            except Exception:
                pass
        c.setStrokeColor(colors.HexColor("#e5e7eb"))
        c.line(36, height - 116, width - 36, height - 116)
        return height - 140

    y = header()
    all_photos = []
    if not rows:
        c.setFont("Helvetica", 11)
        c.drawString(36, y, "Keine Fahrten vorhanden.")
    for idx, row in enumerate(rows, 1):
        r = row_to_dict(row)
        photos = _driver_photos_from_payload(r.get("vehicle_photos"))
        for photo_index, photo in enumerate(photos, 1):
            all_photos.append({
                "ride_index": idx,
                "passenger": r.get("passenger") or "-",
                "duty_date": r.get("duty_date") or "-",
                "license_plate": r.get("license_plate") or "-",
                "saved_at": photo.get("saved_at") or "-",
                "data": photo.get("data") or "",
                "photo_index": photo_index,
            })
        needed = 132
        if y < needed:
            c.showPage(); y = header()
        c.setFillColor(colors.HexColor("#f3f4f6"))
        c.rect(36, y - 4, width - 72, 20, fill=1, stroke=0)
        c.setFillColor(colors.black)
        c.setFont("Helvetica-Bold", 12)
        c.drawString(42, y + 1, f"Fahrt {idx}: {r.get('passenger') or '-'}")
        y -= 24
        fields = [
            ("Einsatztag", r.get("duty_date") or "-"),
            ("Dienstbeginn", r.get("service_start") or "-"),
            ("Dienstende", r.get("service_end") or "-"),
            ("Kennzeichen", r.get("license_plate") or "-"),
            ("Losgefahren", r.get("departure_time") or "-"),
            ("Dauer", f"{to_int(r.get('duration_minutes'),0)} Minuten"),
            ("Ankunft Ziel", r.get("arrival_time") or "-"),
            ("Ziel", r.get("destination") or "-"),
            ("Fahrzeugbilder", f"{len(photos)} Bild(er) – Bilder am Ende des Reports"),
        ]
        for label, val in fields:
            c.setFont("Helvetica-Bold", 9); c.drawString(42, y, f"{label}:")
            c.setFont("Helvetica", 9); c.drawString(130, y, str(val))
            y -= 14
        c.setFont("Helvetica-Bold", 9); c.drawString(42, y, "Bemerkung:")
        y = _pdf_draw_wrapped(c, r.get("remark") or "-", 130, y, width - 172, 13, "Helvetica", 9)
        y -= 10

    if all_photos:
        c.showPage()
        y = header()
        c.setFont("Helvetica-Bold", 14)
        c.drawString(36, y, "Fahrzeugbilder")
        y -= 24
        for item in all_photos:
            if y < 210:
                c.showPage(); y = header()
                c.setFont("Helvetica-Bold", 14)
                c.drawString(36, y, "Fahrzeugbilder")
                y -= 24
            c.setFillColor(colors.HexColor("#f3f4f6"))
            c.rect(36, y - 4, width - 72, 20, fill=1, stroke=0)
            c.setFillColor(colors.black)
            c.setFont("Helvetica-Bold", 10)
            c.drawString(42, y + 1, f"Bild {item['photo_index']} zu Fahrt {item['ride_index']}: {item['passenger']}")
            y -= 18
            meta = f"Einsatztag: {item['duty_date']}   Kennzeichen: {item['license_plate']}   Gespeichert am: {item['saved_at']}"
            c.setFont("Helvetica", 9)
            c.drawString(42, y, meta)
            y -= 10
            img = _pdf_image_from_data_url(item.get("data"))
            if img:
                try:
                    c.drawImage(img, 42, y-150, width-84, 145, preserveAspectRatio=True, mask='auto')
                except Exception:
                    pass
            y -= 170
    c.save()
    buffer.seek(0)
    from flask import send_file
    return send_file(buffer, mimetype="application/pdf", as_attachment=True, download_name="fahrer_report_amine_salah.pdf")


@app.route("/accounting/summary", methods=["GET"])
def accounting_summary():
    denied = require_accounting_access()
    if denied:
        return denied
    view, year, month = parse_period_args()
    return jsonify(build_accounting_summary(get_db(), session.get("username"), view, year, month))


@app.route("/accounting/settings", methods=["POST"])
def accounting_save_settings():
    denied = require_accounting_access()
    if denied:
        return denied
    d = request.get_json(silent=True) or {}
    username = session.get("username")
    now = datetime.now().isoformat(timespec="seconds")
    db = get_db()
    db.execute(
        """INSERT INTO accounting_settings (username, office_address, homeoffice_days_month, internet_monthly, phone_monthly, updated_at)
           VALUES (%s,%s,%s,%s,%s,%s)
           ON CONFLICT (username) DO UPDATE SET
             office_address=EXCLUDED.office_address,
             homeoffice_days_month=EXCLUDED.homeoffice_days_month,
             internet_monthly=EXCLUDED.internet_monthly,
             phone_monthly=EXCLUDED.phone_monthly,
             updated_at=EXCLUDED.updated_at""",
        (username, (d.get("office_address") or "").strip(), 0,
         float(decimal_money(d.get("internet_monthly") or 0)), float(decimal_money(d.get("phone_monthly") or 0)), now),
    )
    db.commit()
    return jsonify({"status":"ok"})


@app.route("/accounting/manual_revenues", methods=["POST"])
def accounting_add_manual_revenue():
    denied = require_accounting_access()
    if denied:
        return denied
    username = session.get("username")
    datum = (request.form.get("datum") or "").strip()
    beschreibung = (request.form.get("beschreibung") or "").strip()
    try:
        betrag = float(decimal_money(request.form.get("betrag") or 0))
    except Exception:
        return jsonify({"error":"Summe ungültig"}), 400
    if not datum:
        return jsonify({"error":"Datum fehlt"}), 400
    if not beschreibung:
        return jsonify({"error":"Beschreibung fehlt"}), 400
    if betrag < 0:
        return jsonify({"error":"Summe darf nicht negativ sein"}), 400

    db = get_db()
    db.execute(
        """INSERT INTO accounting_manual_revenues (id, username, datum, beschreibung, betrag, created_at)
           VALUES (%s,%s,%s,%s,%s,%s)""",
        (str(uuid.uuid4()), username, datum, beschreibung, betrag, datetime.now().isoformat(timespec="seconds")),
    )
    db.commit()
    return jsonify({"status":"ok"})


@app.route("/accounting/manual_revenues/<revenue_id>", methods=["DELETE"])
def accounting_delete_manual_revenue(revenue_id):
    denied = require_accounting_access()
    if denied:
        return denied
    db = get_db()
    row = db.execute("SELECT 1 FROM accounting_manual_revenues WHERE id=%s AND username=%s", (revenue_id, session.get("username"))).fetchone()
    if not row:
        return jsonify({"error":"Einnahme nicht gefunden"}), 404
    db.execute("DELETE FROM accounting_manual_revenues WHERE id=%s AND username=%s", (revenue_id, session.get("username")))
    db.commit()
    return jsonify({"status":"ok"})


@app.route("/accounting/expenses", methods=["POST"])
def accounting_add_expense():
    denied = require_accounting_access()
    if denied:
        return denied
    username = session.get("username")
    datum = (request.form.get("datum") or "").strip()
    kategorie = (request.form.get("kategorie") or "Sonstiges").strip() or "Sonstiges"
    beschreibung = (request.form.get("beschreibung") or "").strip()
    try:
        betrag = float(decimal_money(request.form.get("betrag") or 0))
    except Exception:
        return jsonify({"error":"Betrag ungültig"}), 400
    if not datum:
        return jsonify({"error":"Datum fehlt"}), 400
    if betrag < 0:
        return jsonify({"error":"Betrag darf nicht negativ sein"}), 400

    receipt_file = request.files.get("beleg")
    beleg_path = ""
    beleg_name = ""
    if receipt_file and receipt_file.filename:
        safe_name = re.sub(r"[^A-Za-z0-9._-]+", "_", receipt_file.filename)[:120]
        ext = os.path.splitext(safe_name)[1].lower()
        if ext not in (".pdf", ".png", ".jpg", ".jpeg", ".webp"):
            return jsonify({"error":"Beleg muss PDF, PNG, JPG/JPEG oder WEBP sein."}), 400
        upload_dir = os.path.join(app.root_path, "static", "accounting_receipts")
        os.makedirs(upload_dir, exist_ok=True)
        fname = f"{uuid.uuid4().hex}{ext}"
        receipt_file.save(os.path.join(upload_dir, fname))
        beleg_path = f"accounting_receipts/{fname}"
        beleg_name = safe_name

    db = get_db()
    db.execute(
        """INSERT INTO accounting_expenses (id, username, datum, kategorie, beschreibung, betrag, beleg_path, beleg_name, created_at)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
        (str(uuid.uuid4()), username, datum, kategorie, beschreibung, betrag, beleg_path, beleg_name, datetime.now().isoformat(timespec="seconds")),
    )
    db.commit()
    return jsonify({"status":"ok"})


@app.route("/accounting/expenses/<expense_id>", methods=["DELETE"])
def accounting_delete_expense(expense_id):
    denied = require_accounting_access()
    if denied:
        return denied
    db = get_db()
    row = db.execute("SELECT 1 FROM accounting_expenses WHERE id=%s AND username=%s", (expense_id, session.get("username"))).fetchone()
    if not row:
        return jsonify({"error":"Ausgabe nicht gefunden"}), 404
    db.execute("DELETE FROM accounting_expenses WHERE id=%s AND username=%s", (expense_id, session.get("username")))
    db.commit()
    return jsonify({"status":"ok"})


@app.route("/accounting/travel", methods=["POST"])
def accounting_save_travel():
    denied = require_accounting_access()
    if denied:
        return denied
    d = request.get_json(silent=True) or {}
    event_id = (d.get("event_id") or "").strip()
    km_total = float(decimal_money(d.get("km_total") or 0))
    note = (d.get("note") or "").strip()
    if not event_id:
        return jsonify({"error":"Einsatz fehlt"}), 400
    if km_total < 0:
        return jsonify({"error":"Kilometer dürfen nicht negativ sein"}), 400
    db = get_db()
    ok = db.execute("SELECT 1 FROM response WHERE event_id=%s AND username=%s AND status=%s", (event_id, session.get("username"), "bestätigt")).fetchone()
    if not ok:
        return jsonify({"error":"Einsatz nicht gefunden oder nicht bestätigt"}), 403
    now = datetime.now().isoformat(timespec="seconds")
    db.execute(
        """INSERT INTO accounting_travel (id, username, event_id, km_total, note, created_at, updated_at)
           VALUES (%s,%s,%s,%s,%s,%s,%s)
           ON CONFLICT (username, event_id) DO UPDATE SET
             km_total=EXCLUDED.km_total, note=EXCLUDED.note, updated_at=EXCLUDED.updated_at""",
        (str(uuid.uuid4()), session.get("username"), event_id, km_total, note, now, now),
    )
    db.commit()
    return jsonify({"status":"ok"})


@app.route("/accounting/export_pdf", methods=["GET"])
def accounting_export_pdf():
    denied = require_accounting_access()
    if denied:
        return denied
    view, year, month = parse_period_args()
    data = build_accounting_summary(get_db(), session.get("username"), view, year, month)
    from flask import send_file
    buffer = io.BytesIO()
    pdf = canvas.Canvas(buffer, pagesize=A4)
    width, height = A4
    x0, y = 42, height - 42

    def text(txt, x, yy, size=10, font="Helvetica"):
        pdf.setFont(font, size)
        pdf.drawString(x, yy, str(txt))

    def ensure_page(rows_needed=1):
        nonlocal y
        if y < 80 + rows_needed * 14:
            pdf.showPage()
            y = height - 42

    title = "Buchführung Monatsübersicht" if view == "month" else "Buchführung Jahresübersicht"
    period = month_label_de(year, month) if view == "month" else str(year)
    text(title, x0, y, 16, "Helvetica-Bold"); y -= 22
    text(f"Amine Salah – Zeitraum: {period}", x0, y, 11); y -= 24
    for label, key in [("Einnahmen gesamt","revenues"),("Einnahmen aus Einsätzen","automatic_revenues"),("Zusätzliche Einnahmen","manual_revenues"),("Manuelle Ausgaben","manual_expenses"),("Fahrtkosten","travel"),("Essenspauschale","meal_allowance"),("Internet","internet"),("Telefon","phone"),("Ausgaben gesamt","expenses"),("Gewinn","profit")]:
        text(label + ":", x0, y, 10, "Helvetica-Bold" if key in ("expenses","profit") else "Helvetica")
        pdf.drawRightString(width - 42, y, format_eur(data["totals"][key]))
        y -= 15

    y -= 10; ensure_page(2); text("Einnahmen", x0, y, 12, "Helvetica-Bold"); y -= 16
    for e in data["revenues"]:
        ensure_page()
        text(f"{e['date']} | {e['category']} | {e['title'][:42]} | {e['hours']:.2f} h", x0, y, 9)
        pdf.drawRightString(width - 42, y, format_eur(e["amount"])); y -= 13

    y -= 10; ensure_page(2); text("Zusätzliche Einnahmen", x0, y, 12, "Helvetica-Bold"); y -= 16
    for e in data.get("manual_revenues", []):
        ensure_page()
        text(f"{e['date']} | {e['description'][:55]}", x0, y, 9)
        pdf.drawRightString(width - 42, y, format_eur(e["amount"])); y -= 13

    y -= 10; ensure_page(2); text("Fahrtkosten", x0, y, 12, "Helvetica-Bold"); y -= 16
    for t in data["travel"]:
        ensure_page()
        text(f"{t['date']} | {t['title'][:38]} | {t['km_total']:.1f} km x 0,30 EUR", x0, y, 9)
        pdf.drawRightString(width - 42, y, format_eur(t["amount"])); y -= 13

    y -= 10; ensure_page(2); text("Ausgaben / Belege", x0, y, 12, "Helvetica-Bold"); y -= 16
    for e in data["expenses"]:
        ensure_page()
        receipt = " | Beleg vorhanden" if e.get("has_receipt") else ""
        text(f"{e['date']} | {e['category']} | {e['description'][:45]}{receipt}", x0, y, 9)
        pdf.drawRightString(width - 42, y, format_eur(e["amount"])); y -= 13

    y -= 18; ensure_page(3)
    text("Hinweis", x0, y, 10, "Helvetica-Bold"); y -= 14
    text("Diese Übersicht dient als Vorbereitung für die EÜR/Steuererklärung. Bitte Belege zusätzlich aufbewahren.", x0, y, 8)
    pdf.save()
    buffer.seek(0)
    filename = f"buchfuehrung_amine_salah_{year}_{month:02d}.pdf" if view == "month" else f"buchfuehrung_amine_salah_{year}.pdf"
    return send_file(buffer, mimetype="application/pdf", as_attachment=True, download_name=filename)


# ---------------- Events API ----------------
@app.route("/events", methods=["GET"])
def events_list():
    # ✅ Login erforderlich (damit Planer/Mitarbeiter nicht anonym zugreifen)
    if "username" not in session:
        return jsonify({"error": "Nicht eingeloggt"}), 403

    # ✅ DSGVO: Mitarbeiter ohne Einwilligung dürfen keine Einsätze laden
    if employee_requires_consent():
        return jsonify({"error":"Bitte zuerst im Report in die Datenverarbeitung einwilligen."}), 403

    db = get_db()
    role = normalize_role(session.get("role") or "mitarbeiter")

    # Performance: Events optional nach sichtbarem Zeitraum oder einzelner ID laden.
    # FullCalendar sendet start/end; dadurch wird nicht mehr die komplette Historie geladen.
    event_id_filter = (request.args.get("event_id") or "").strip()
    start_filter = (request.args.get("start") or "").strip()
    end_filter = (request.args.get("end") or "").strip()
    lite_mode = (request.args.get("lite") or "").strip().lower() in ("1", "true", "yes")

    where = []
    params = []
    if event_id_filter:
        where.append("id=%s")
        params.append(event_id_filter)
    else:
        if start_filter:
            where.append("start >= %s")
            params.append(start_filter)
        if end_filter:
            where.append("start < %s")
            params.append(end_filter)

    sql = "SELECT * FROM event"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY start ASC"
    ecur = db.execute(sql, tuple(params))
    events = [row_to_dict(e) for e in ecur.fetchall()]

    # ✅ Rollen-Restriktionen (serverseitig)
    role_lc = normalize_role(role)
    if role_lc == "planner_bbs":
        today = datetime.now().date()

        def _planner_bbs_visible_from_today(ev):
            # Planer BBS darf nur seine explizit zugewiesenen CV-Einsätze ab dem heutigen Tag sehen.
            # Alles andere bleibt für diese Rolle unsichtbar.
            if (ev.get("category") or "CP").strip().upper() != "CV":
                return False

            assigned_leads = parse_einsatzleitung_usernames(ev.get("einsatzleitung_usernames"), ev.get("einsatzleitung_username"))
            if (session.get("username") or "").strip() not in assigned_leads:
                return False

            raw_start = str(ev.get("start") or "").strip()
            if not raw_start:
                return False

            try:
                start_date = datetime.fromisoformat(raw_start.replace("Z", "")).date()
            except Exception:
                try:
                    start_date = datetime.fromisoformat(raw_start.split("T")[0]).date()
                except Exception:
                    return False

            return start_date >= today

        events = [e for e in events if _planner_bbs_visible_from_today(e)]

    # Amines eigene Auftraggeber/Einsätze sind privat; BS wird zusätzlich ausgeblendet.
    if not current_user_can_manage_private_jobs():
        events = [e for e in events if not is_private_amine_category(e.get("category")) and (e.get("category") or "").strip().upper() != "BS"]
    else:
        events = [e for e in events if (e.get("category") or "").strip().upper() != "BS"]

    # Mitarbeiter: Profil-Stundensatz holen (für my_rate)
    my_profile_rate = 0.0
    if role not in ["chef", "vorgesetzter", "planer", "planner_bbs", "vorgesetzter_cp"]:
        me = db.execute("SELECT * FROM users WHERE username=%s", (session.get("username"),)).fetchone()
        if me:
            my_profile_rate = float(me.get("stundensatz") or 0.0)

    result = []
    for e in events:
        rcur = db.execute(
            "SELECT username,status,remark,start_time,end_time,rate_override,profile_rate_snapshot FROM response WHERE event_id=%s",
            (e["id"],)
        )
        rmap = {}
        for r in rcur.fetchall():
            # Einheitlicher effektiver Satz für Frontend/Report:
            # Override > Snapshot > aktueller effektiver Satz (Einsatz-SVS oder Personal-SVS).
            effective_rate = None
            if r.get("rate_override") not in (None, ""):
                effective_rate = r.get("rate_override")
            elif r.get("profile_rate_snapshot") not in (None, ""):
                effective_rate = r.get("profile_rate_snapshot")
            else:
                effective_rate = freeze_effective_rate_snapshot(db, e["id"], r["username"])

            rmap[r["username"]] = {
                "status": r["status"] or "",
                "remark": r["remark"] or "",
                "start_time": r["start_time"] or "",
                "end_time": r.get("end_time") or "",
                "rate_override": r["rate_override"],
                "profile_rate_snapshot": r.get("profile_rate_snapshot"),
                "effective_rate": effective_rate,
                # Zusatzkosten sind nur in Detail-/Report-Ladevorgängen nötig.
                # Im Kalender-Lite-Modus werden sie weggelassen, damit die Startansicht schneller lädt.
                "extra_costs": [] if lite_mode else get_response_extra_costs(db, e["id"], r["username"])
            }
        e["responses"] = rmap

        assigned_leads = parse_einsatzleitung_usernames(e.get("einsatzleitung_usernames"), e.get("einsatzleitung_username"))
        e["einsatzleitung_usernames"] = assigned_leads
        e["einsatzleitung_username"] = assigned_leads[0] if assigned_leads else ""

        # ---- UI helpers: CSS Klassen für FullCalendar (Dot/Block Färbung) ----
        # Diese Erweiterung entfernt/ändert keine bestehende Logik; sie ergänzt nur Metadaten fürs Frontend.
        cls = []
        # Kategorie (CP/CV/eigene Auftraggeber)
        cat = normalize_private_category(e.get("category") or "CP", "CP")
        cls.append("cat-" + cat.lower())

        # Event-Status (geplant/offen/...)
        ev_status_token = status_to_css_token(e.get("status", ""))
        if ev_status_token:
            cls.append(f"status-event-{ev_status_token}")

        # Zusatz-Status für Chef-Ansicht (nur bei status 'offen'):
        # - 'voll'  => benötigte Mitarbeiter erreicht (grün)
        # - 'bewerbung' => es gibt Bewerbungen/Zusagen, aber noch nicht voll (rot)
        # Diese Logik ergänzt nur CSS-Klassen und ändert keine Daten in der DB.
        try:
            req = int(e.get("required_staff") or 0)
        except Exception:
            req = 0

        # Bewerbungen/Zusagen zählen (alles, was nicht leer ist und nicht explizit entfernt wurde)
        has_applications = any(
            (rv.get("status") or "").strip() in ("zugesagt", "bestätigt")
            for rv in (rmap or {}).values()
        )

        confirmed_count = sum(
            1 for rv in (rmap or {}).values()
            if (rv.get("status") or "").strip() == "bestätigt"
        )

        if (e.get("status") or "").strip().lower() == "offen":
            if req > 0 and confirmed_count >= req:
                cls.append("status-event-voll")
            elif has_applications:
                cls.append("status-event-bewerbung")

        # Für Mitarbeiter: eigener Response-Status als Klasse (zugesagt/bestätigt/abgelehnt/...)
        if role not in ["chef", "vorgesetzter", "planer", "planner_bbs", "vorgesetzter_cp"]:
            my = rmap.get(session.get("username"), {}) or {}
            my_status_token = status_to_css_token(my.get("status", ""))
            if my_status_token:
                cls.append(f"status-{my_status_token}")

        # An FullCalendar übergeben (wird als classNames akzeptiert)
        e["classNames"] = cls

        # ✅ BUGFIX: 0 darf NICHT zu 1 werden
        raw_u = e.get("use_event_rate")
        use_event_rate = 1 if raw_u is None else int(raw_u)

        # Chef/Vorgesetzter/Planer: keine eigenen Raten berechnen
        if role in ["chef", "vorgesetzter", "planer", "planner_bbs", "vorgesetzter_cp"]:
            e["my_rate"] = 0
        else:
            my_response = rmap.get(session.get("username"), {}) or {}

            # Historischer Satz für den aktuell eingeloggten Mitarbeiter:
            # Priorität: rate_override -> gespeicherter Snapshot.
            # Der Snapshot enthält bereits den effektiv gültigen Satz
            # (Event-Stundensatz oder Profil-Stundensatz zum damaligen Zeitpunkt).
            if my_response.get("rate_override") not in (None, ""):
                try:
                    e["my_rate"] = float(my_response.get("rate_override") or 0.0)
                except Exception:
                    e["my_rate"] = 0.0
            else:
                snap = my_response.get("profile_rate_snapshot")
                try:
                    if snap not in (None, ""):
                        e["my_rate"] = float(snap)
                    elif use_event_rate == 1 and e.get("stundensatz") not in (None, ""):
                        e["my_rate"] = float(e.get("stundensatz") or 0.0)
                    else:
                        # Kein Snapshot = zukünftiger/dynamischer Profil-Satz
                        e["my_rate"] = float(my_profile_rate or 0.0)
                except Exception:
                    e["my_rate"] = 0.0

        result.append(e)

    return jsonify(result)



# ---------------------------------------------------------------------------
# Stabile Mitarbeiter-APIs für "Meine Termine" und "Report"
# Diese Endpunkte laden nur die benötigten Monatsdaten für den eingeloggten
# Mitarbeiter. Dadurch bleiben die Tabs schnell und brechen nicht mehr ab,
# wenn /events sehr viele Daten oder eine unerwartete Antwort liefert.
# ---------------------------------------------------------------------------
def _parse_year_month_from_request(default_today=True):
    today = datetime.now(ZoneInfo("Europe/Berlin"))
    try:
        year = int(request.args.get("year") or today.year)
    except Exception:
        year = today.year
    try:
        month = int(request.args.get("month") or today.month)
    except Exception:
        month = today.month
    if month < 1 or month > 12:
        month = today.month
    return year, month


def _month_bounds_iso(year: int, month: int):
    from datetime import timedelta
    start_dt = datetime(year, month, 1)
    if month == 12:
        end_dt = datetime(year + 1, 1, 1)
    else:
        end_dt = datetime(year, month + 1, 1)
    return start_dt.strftime("%Y-%m-%d"), end_dt.strftime("%Y-%m-%d")


def _apply_response_start_time(event_start, response_start_time):
    start_dt = parse_iso_dt(event_start)
    if not start_dt:
        return None
    custom_start = parse_hhmm(response_start_time)
    if custom_start:
        start_dt = start_dt.replace(hour=custom_start[0], minute=custom_start[1], second=0, microsecond=0)
    return start_dt


def _effective_rate_for_response(db, ev, resp, username):
    if resp.get("rate_override") not in (None, ""):
        return decimal_money(resp.get("rate_override"))
    if resp.get("profile_rate_snapshot") not in (None, ""):
        return decimal_money(resp.get("profile_rate_snapshot"))

    use_event_rate = to_int(ev.get("use_event_rate"), 1)
    if use_event_rate == 1 and ev.get("stundensatz") not in (None, ""):
        return decimal_money(ev.get("stundensatz"))

    u = db.execute("SELECT stundensatz FROM users WHERE username=%s", (username,)).fetchone()
    return decimal_money((u or {}).get("stundensatz"))


def _rate_label(rate_value):
    return format_rate_eur(rate_value)


@app.route("/api/mitarbeiter/termine", methods=["GET"])
def api_mitarbeiter_termine():
    if "username" not in session:
        return jsonify({"error": "Nicht eingeloggt"}), 403
    if employee_requires_consent():
        return jsonify({"error": "Bitte zuerst im Report in die Datenverarbeitung einwilligen."}), 403

    username = session.get("username")
    year, month = _parse_year_month_from_request()
    start_bound, end_bound = _month_bounds_iso(year, month)
    db = get_db()

    rows = db.execute(
        """
        SELECT e.*, r.status AS response_status, r.remark AS response_remark,
               r.start_time AS response_start_time, r.end_time AS response_end_time,
               r.rate_override, r.profile_rate_snapshot
        FROM event e
        JOIN response r ON r.event_id = e.id
        WHERE r.username=%s
          AND r.status=%s
          AND e.start >= %s
          AND e.start < %s
        ORDER BY e.start ASC
        """,
        (username, "bestätigt", start_bound, end_bound),
    ).fetchall() or []

    result = []
    for row in rows:
        ev = row_to_dict(row)
        cat = str(ev.get("category") or "CP").strip().upper()
        if cat == "BS":
            continue
        if is_private_amine_category(cat) and not current_user_can_manage_private_jobs():
            continue

        start_dt = _apply_response_start_time(ev.get("start"), ev.get("response_start_time"))
        if not start_dt:
            continue
        if start_dt.year != year or start_dt.month != month:
            continue

        rate = _effective_rate_for_response(db, ev, ev, username)
        result.append({
            "id": ev.get("id"),
            "date": start_dt.strftime("%d.%m.%Y"),
            "timestamp": start_dt.timestamp(),
            "title": ev.get("title") or "(ohne Titel)",
            "ort": ev.get("ort") or "-",
            "startStr": start_dt.strftime("%H:%M"),
            "plannedEndStr": ev.get("planned_end_time") or "-",
            "rateText": _rate_label(rate),
            "remark": (ev.get("response_remark") or "").strip() or "-",
            "category": cat,
        })

    result.sort(key=lambda x: (x.get("timestamp") or 0, x.get("title") or ""))
    return jsonify(result)


@app.route("/api/mitarbeiter/report", methods=["GET"])
def api_mitarbeiter_report():
    if "username" not in session:
        return jsonify({"error": "Nicht eingeloggt"}), 403
    if employee_requires_consent():
        return jsonify({"error": "Bitte zuerst im Report in die Datenverarbeitung einwilligen."}), 403

    username = session.get("username")
    year, month = _parse_year_month_from_request()
    category = str(request.args.get("category") or "CV").strip().upper() or "CV"
    start_bound, end_bound = _month_bounds_iso(year, month)
    db = get_db()

    rows = db.execute(
        """
        SELECT e.*, r.status AS response_status, r.remark AS response_remark,
               r.start_time AS response_start_time, r.end_time AS response_end_time,
               r.rate_override, r.profile_rate_snapshot
        FROM event e
        JOIN response r ON r.event_id = e.id
        WHERE r.username=%s
          AND r.status=%s
          AND COALESCE(r.end_time,'') <> ''
          AND e.start >= %s
          AND e.start < %s
        ORDER BY e.start ASC
        """,
        (username, "bestätigt", start_bound, end_bound),
    ).fetchall() or []

    entries = []
    total_hours = Decimal("0.00")
    total_earnings = Decimal("0.00")

    for row in rows:
        ev = row_to_dict(row)
        cat = str(ev.get("category") or "CP").strip().upper()
        if cat == "BS":
            continue
        if is_private_amine_category(cat) and not current_user_can_manage_private_jobs():
            continue
        if cat != category:
            continue

        start_dt = _apply_response_start_time(ev.get("start"), ev.get("response_start_time"))
        if not start_dt:
            continue
        if start_dt.year != year or start_dt.month != month:
            continue

        end_parts = parse_hhmm(ev.get("response_end_time"))
        if not end_parts:
            continue
        from datetime import timedelta
        end_dt = start_dt.replace(hour=end_parts[0], minute=end_parts[1], second=0, microsecond=0)
        if end_dt < start_dt:
            end_dt = end_dt + timedelta(days=1)

        hours = decimal_money(Decimal(str((end_dt - start_dt).total_seconds())) / Decimal("3600"))
        rate = _effective_rate_for_response(db, ev, ev, username)
        base_total = decimal_money(hours * rate)
        extra_costs = get_response_extra_costs(db, ev.get("id"), username)
        extra_total = sum((decimal_money(c.get("amount")) for c in extra_costs), Decimal("0.00"))
        earnings = decimal_money(base_total + extra_total)

        total_hours += hours
        total_earnings += earnings

        entries.append({
            "id": ev.get("id"),
            "timestamp": start_dt.timestamp(),
            "date": start_dt.strftime("%d.%m.%Y"),
            "title": ev.get("title") or "(ohne Titel)",
            "startStr": start_dt.strftime("%H:%M"),
            "plannedEnd": ev.get("planned_end_time") or "-",
            "endStr": end_dt.strftime("%H:%M"),
            "hours": float(hours),
            "rate": float(rate),
            "rateText": _rate_label(rate),
            "rateOverride": ev.get("rate_override") or "",
            "extra_costs": extra_costs,
            "extrasTotal": float(decimal_money(extra_total)),
            "earnings": float(earnings),
            "cat": cat,
        })

    entries.sort(key=lambda x: (x.get("timestamp") or 0, x.get("title") or ""))
    return jsonify({
        "entries": entries,
        "totalHours": float(decimal_money(total_hours)),
        "totalEarnings": float(decimal_money(total_earnings)),
    })


@app.route("/events", methods=["POST"])
def add_event():
    role_now = normalize_role(session.get("role") or "")
    amine_self_create = (role_now == "mitarbeiter" and is_amine_salah_user())
    if role_now not in ["chef", "vorgesetzter", "vorgesetzter_cp"] and not amine_self_create:
        return jsonify({"error": "Nicht erlaubt"}), 403

    d = request.json or {}
    ev_id = str(uuid.uuid4())

    start = d.get("start") or ""
    planned_end_time = (d.get("planned_end_time") or "").strip()
    frist = (d.get("frist") or "").strip()

    status = d.get("status", "geplant")
    category = (d.get("category") or "CP").strip().upper()
    if amine_self_create:
        # Amine kann eigene Auftraggeber/Kategorien selbst anlegen. BS wird nicht mehr verwendet.
        category = normalize_private_category(d.get("category") or d.get("auftraggeber") or "PRIVAT")
        status = "offen"
    elif category not in ("CP", "CV"):
        return jsonify({"error": "Nur CP/CV-Einsätze dürfen hier angelegt werden."}), 403
    required_staff = to_int(d.get("required_staff", 1 if amine_self_create else 0), 0)

    use_event_rate = to_int(d.get("use_event_rate", 1), 1)
    einsatzleitung_usernames = parse_einsatzleitung_usernames(d.get("einsatzleitung_usernames"), d.get("einsatzleitung_username"))
    if len(einsatzleitung_usernames) > 3:
        return jsonify({"error": "Maximal 3 Einsatzleiter erlaubt."}), 400
    einsatzleitung_username = einsatzleitung_usernames[0] if einsatzleitung_usernames else None
    einsatzleitung_usernames_json = dump_einsatzleitung_usernames(einsatzleitung_usernames)
    stundensatz = d.get("stundensatz")
    if amine_self_create:
        # Eigene Einsätze von Amine nutzen immer den direkt im Einsatz eingetragenen Stundensatz.
        # Dadurch wird NICHT der Stundensatz aus der Mitarbeiterverwaltung verwendet.
        use_event_rate = 1
        if stundensatz in ("", None):
            return jsonify({"error": "Bitte Stundensatz eintragen."}), 400
    stundensatz = None if stundensatz in ("", None) else float(stundensatz)
    if use_event_rate == 0:
        stundensatz = None

    db = get_db()
    db.execute(
        """INSERT INTO event
           (id,title,ort,dienstkleidung,auftraggeber,start,planned_end_time,frist,status,category,required_staff,use_event_rate,stundensatz,einsatzleitung_username,einsatzleitung_usernames)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
        (
            ev_id,
            d.get("title") or "",
            d.get("ort") or "",
            d.get("dienstkleidung") or "",
            (d.get("auftraggeber") or category) if amine_self_create else (d.get("auftraggeber") or ""),
            start,
            planned_end_time,
            frist,
            status,
            category,
            required_staff,
            use_event_rate,
            stundensatz,
            einsatzleitung_username,
            einsatzleitung_usernames_json
        )
    )
    if amine_self_create:
        profile_rate_snapshot = freeze_effective_rate_snapshot(db, ev_id, session.get("username"))
        db.execute(
            "INSERT INTO response (event_id, username, status, remark, start_time, end_time, profile_rate_snapshot) VALUES (%s,%s,%s,%s,%s,%s,%s)",
            (ev_id, session.get("username"), "bestätigt", "", "", "", profile_rate_snapshot)
        )
    db.commit()
    return jsonify({"status": "ok"})


@app.route("/events/assign_user", methods=["POST"])
def assign_user():
    """Chef: Mitarbeiter als bestätigt zuweisen."""
    if session.get("role") not in ["chef", "vorgesetzter", "vorgesetzter_cp"]:
        return jsonify({"error": "Nicht erlaubt"}), 403

    d = request.json or {}
    event_id = d.get("event_id")
    username = d.get("username")

    if not event_id or not username:
        return jsonify({"error": "event_id und username erforderlich"}), 400

    db = get_db()
    event_row = db.execute("SELECT id, title, start, ort, dienstkleidung FROM event WHERE id=%s", (event_id,)).fetchone()
    if not event_row:
        return jsonify({"error": "Event nicht gefunden"}), 404
    blocked = deny_bs_for_non_amine(db, event_id)
    if blocked:
        return blocked

    user_row = db.execute("SELECT username, vorname, nachname, email, role FROM users WHERE username=%s", (username,)).fetchone()
    if not user_row:
        return jsonify({"error": "User nicht gefunden"}), 404

    if normalize_role(user_row.get("role") or "") == "planner_bbs":
        return jsonify({"error": "Planer BBS kann nicht direkt zugewiesen werden."}), 400

    profile_rate_snapshot = freeze_effective_rate_snapshot(db, event_id, username)

    if db.execute("SELECT 1 FROM response WHERE event_id=%s AND username=%s", (event_id, username)).fetchone():
        db.execute(
            "UPDATE response SET status='bestätigt', profile_rate_snapshot = COALESCE(profile_rate_snapshot, %s) WHERE event_id=%s AND username=%s",
            (profile_rate_snapshot, event_id, username)
        )
    else:
        db.execute(
            "INSERT INTO response (event_id, username, status, remark, start_time, end_time, profile_rate_snapshot) VALUES (%s,%s,%s,%s,%s,%s,%s)",
            (event_id, username, "bestätigt", "", "", "", profile_rate_snapshot)
        )

    db.commit()

    mail_sent = False
    mail_error = ""
    try:
        employee_name = " ".join(filter(None, [
            (user_row.get("vorname") or "").strip(),
            (user_row.get("nachname") or "").strip()
        ])).strip() or username
        to_addr = (user_row.get("email") or "").strip()
        if to_addr:
            subject = f"✅ Auftrag zugewiesen: {event_row.get('title') or 'Einsatz'}"
            body = build_assignment_mail(
                employee_name=employee_name,
                event_title=event_row.get("title") or "",
                event_start_dt=event_row.get("start") or "",
                ort=event_row.get("ort") or "",
                dienstkleidung=event_row.get("dienstkleidung") or "",
                start_time="",
            )
            send_mail(to_addr, subject, body)
            mail_sent = True
        else:
            mail_error = "Keine E-Mail-Adresse beim Mitarbeiter hinterlegt."
    except Exception as e:
        mail_error = str(e)

    return jsonify({"status": "ok", "mail_sent": mail_sent, "mail_error": mail_error})


@app.route("/events/remove_user", methods=["POST"])
def remove_user_from_event():
    """Chef: Mitarbeiter komplett aus Einsatz entfernen."""
    if session.get("role") not in ["chef", "vorgesetzter", "vorgesetzter_cp"]:
        return jsonify({"error": "Nicht erlaubt"}), 403

    d = request.json or {}
    event_id = d.get("event_id")
    username = d.get("username")

    if not event_id or not username:
        return jsonify({"error": "event_id und username erforderlich"}), 400

    db = get_db()
        # Statt Löschen: auf "entfernt_chef" setzen, damit der Mitarbeiter den Einsatz nicht mehr sieht
    # und es nicht wieder als "offen" erscheint.
    cur = db.execute(
        "UPDATE response SET status=%s WHERE event_id=%s AND username=%s",
        ("entfernt_chef", event_id, username)
    )

    # Falls es noch keinen Response-Eintrag gab, legen wir einen entfernt_chefen an
    if cur.rowcount == 0:
        db.execute(
            "INSERT INTO response (event_id, username, status, remark, start_time, end_time) VALUES (%s,%s,%s,%s,%s,%s)",
            (event_id, username, "entfernt_chef", "", "", "")
        )
    db.commit()
    return jsonify({"status": "ok"})


@app.route("/events/<event_id>", methods=["DELETE"])
def delete_event(event_id):
    role_now = normalize_role(session.get("role") or "")
    amine_bs_delete = (role_now == "mitarbeiter" and is_amine_salah_user())
    if role_now not in ["chef", "vorgesetzter", "vorgesetzter_cp"] and not amine_bs_delete:
        return jsonify({"error": "Nicht erlaubt"}), 403
    db = get_db()
    if amine_bs_delete:
        ev = db.execute("SELECT category FROM event WHERE id=%s", (event_id,)).fetchone()
        if not ev or not is_private_amine_category(ev.get("category")):
            return jsonify({"error": "Amine darf nur eigene/private Aufträge löschen."}), 403
    else:
        blocked = deny_bs_for_non_amine(db, event_id)
        if blocked:
            return blocked
    db.execute("DELETE FROM event WHERE id=%s", (event_id,))
    db.commit()
    return jsonify({"status": "ok"})


@app.route("/events/release", methods=["POST"])
def release_event():
    if session.get("role") not in ["chef", "vorgesetzter", "vorgesetzter_cp"]:
        return jsonify({"error": "Nicht erlaubt"}), 403
    d = request.json or {}
    event_id = d.get("event_id")

    db = get_db()
    blocked = deny_bs_for_non_amine(db, event_id)
    if blocked:
        return blocked
    cur = db.execute("UPDATE event SET status='offen' WHERE id=%s", (event_id,))
    if cur.rowcount == 0:
        return jsonify({"error": "Event nicht gefunden"}), 404

    db.commit()
    return jsonify({"status": "ok"})


@app.route("/events/update", methods=["POST"])
def update_event():
    role_now = normalize_role(session.get("role") or "")
    amine_bs_update = (role_now == "mitarbeiter" and is_amine_salah_user())

    if role_now not in ["chef", "vorgesetzter", "vorgesetzter_cp"] and not amine_bs_update:
        return jsonify({"error": "Nicht erlaubt"}), 403

    d = request.json or {}
    event_id = d.get("event_id")
    if not event_id:
        return jsonify({"error": "event_id fehlt"}), 400

    title = d.get("title") or ""
    ort = d.get("ort") or ""
    dienstkleidung = d.get("dienstkleidung") or ""
    auftraggeber = d.get("auftraggeber") or ""
    start = d.get("start") or ""
    planned_end_time = (d.get("planned_end_time") or "").strip()
    frist = (d.get("frist") or "").strip()
    status = d.get("status") or "geplant"
    category = (d.get("category") or "CP").strip().upper()
    if amine_bs_update:
        category = normalize_private_category(d.get("category") or d.get("auftraggeber") or category)
    elif category not in ("CP", "CV"):
        return jsonify({"error": "Nur CP/CV-Einsätze dürfen hier bearbeitet werden."}), 403
    required_staff = to_int(d.get("required_staff", 0), 0)

    use_event_rate = to_int(d.get("use_event_rate", 1), 1)
    einsatzleitung_usernames = parse_einsatzleitung_usernames(d.get("einsatzleitung_usernames"), d.get("einsatzleitung_username"))
    if len(einsatzleitung_usernames) > 3:
        return jsonify({"error": "Maximal 3 Einsatzleiter erlaubt."}), 400
    einsatzleitung_username = einsatzleitung_usernames[0] if einsatzleitung_usernames else None
    einsatzleitung_usernames_json = dump_einsatzleitung_usernames(einsatzleitung_usernames)
    stundensatz = d.get("stundensatz")
    stundensatz = None if stundensatz in ("", None) else float(stundensatz)
    if use_event_rate == 0:
        stundensatz = None

    db = get_db()
    if not amine_bs_update:
        blocked = deny_bs_for_non_amine(db, event_id)
        if blocked:
            return blocked
    if amine_bs_update:
        ev = db.execute("SELECT category FROM event WHERE id=%s", (event_id,)).fetchone()
        if not ev or not is_private_amine_category(ev.get("category")):
            return jsonify({"error": "Amine darf nur eigene/private Aufträge bearbeiten."}), 403

        auftraggeber = (auftraggeber or category).strip()
        status = "offen"
        required_staff = 1
        use_event_rate = 1

        if stundensatz in (None, ""):
            return jsonify({"error": "Bitte Stundensatz eintragen."}), 400
    old_event_rate = db.execute(
        "SELECT use_event_rate, stundensatz FROM event WHERE id=%s",
        (event_id,),
    ).fetchone()
    if old_event_rate:
        old_use_event_rate = to_int(old_event_rate.get("use_event_rate", 1), 1)
        old_stundensatz = old_event_rate.get("stundensatz")
        if old_use_event_rate != use_event_rate or str(old_stundensatz or "") != str(stundensatz or ""):
            # Bereits bestätigte Mitarbeiter sichern, bevor der Einsatz-Stundensatz geändert wird.
            freeze_confirmed_event_snapshots(db, event_id)

    cur = db.execute(
        """UPDATE event SET
           title=%s, ort=%s, dienstkleidung=%s, auftraggeber=%s,
           start=%s, planned_end_time=%s, frist=%s, status=%s, category=%s, required_staff=%s,
           use_event_rate=%s, stundensatz=%s, einsatzleitung_username=%s, einsatzleitung_usernames=%s
           WHERE id=%s""",
        (
            title, ort, dienstkleidung, auftraggeber,
            start, planned_end_time, frist, status, category, required_staff,
            use_event_rate, stundensatz, einsatzleitung_username, einsatzleitung_usernames_json,
            event_id
        )
    )
    if cur.rowcount == 0:
        return jsonify({"error": "Event nicht gefunden"}), 404

    db.commit()
    return jsonify({"status": "ok"})


@app.route("/events/respond", methods=["POST"])
def respond_event():
    """
    Mitarbeiter: auf offenen Einsatz reagieren.
    - response: 'zugesagt' | 'abgelehnt' | '' (zurückziehen)
    - remark: optional (wird für Chef sichtbar gespeichert)
    Regel: Änderungen sind nur bis zur Frist möglich (falls gesetzt).
    """
    if session.get("role") != "mitarbeiter":
        return jsonify({"error": "Nicht erlaubt"}), 403

    # ✅ DSGVO: erst Einwilligung, dann Aktionen
    if employee_requires_consent():
        return jsonify({"error":"Bitte zuerst im Report in die Datenverarbeitung einwilligen."}), 403

    d = request.json or {}
    event_id = (d.get("event_id") or "").strip()
    response_val = (d.get("response") or "").strip()
    remark = (d.get("remark") or "").strip()

    if not event_id:
        return jsonify({"error": "event_id fehlt"}), 400

    if response_val not in ("zugesagt", "abgelehnt", ""):
        return jsonify({"error": "Ungültige Antwort"}), 400

    db = get_db()

    ev = db.execute("SELECT id, frist FROM event WHERE id=%s", (event_id,)).fetchone()
    if not ev:
        return jsonify({"error": "Event nicht gefunden"}), 404

    # Frist prüfen (falls gesetzt)
    frist_raw = (ev["frist"] or "").strip() if "frist" in ev.keys() else ""
    if frist_raw:
        try:
            frist_dt = datetime.fromisoformat(frist_raw)
            if datetime.now() > frist_dt:
                return jsonify({"error": "Die Frist ist abgelaufen. Änderungen sind nicht mehr möglich."}), 400
        except Exception:
            # Wenn das Datum in der DB kaputt ist, sperren wir lieber nicht
            pass

    me = db.execute("SELECT username FROM users WHERE username=%s", (session["username"],)).fetchone()
    if not me:
        return jsonify({"error": "Nicht eingeloggt"}), 403

    # Bestehenden Eintrag prüfen
    existing = db.execute(
        "SELECT status, end_time FROM response WHERE event_id=%s AND username=%s",
        (event_id, me["username"])
    ).fetchone()

    # Wenn bereits bestätigt oder Endzeit gesetzt -> nicht über /respond ändern
    if existing:
        if (existing["status"] or "") == "bestätigt" or (existing["end_time"] or "").strip():
            return jsonify({"error": "Dieser Einsatz ist bereits bestätigt/abgerechnet und kann hier nicht mehr geändert werden."}), 400

    # Zurückziehen: Status/Bemerkung wirklich entfernen (NULL), damit im Chef-Dashboard
    # keine "leere Karte" mit Rahmen stehen bleibt.
    if response_val == "":
        if existing:
            db.execute(
                "UPDATE response SET status=NULL, remark=NULL WHERE event_id=%s AND username=%s",
                (event_id, me["username"])
            )
        else:
            # Wenn es noch keinen Eintrag gab, müssen wir nichts anlegen.
            pass
    else:
        if existing:
            db.execute(
                "UPDATE response SET status=%s, remark=%s WHERE event_id=%s AND username=%s",
                (response_val, remark, event_id, me["username"])
            )
        else:
            db.execute(
                "INSERT INTO response (event_id, username, status, remark) VALUES (%s,%s,%s,%s)",
                (event_id, me["username"], response_val, remark)
            )

    db.commit()
    return jsonify({"status": "ok"})


@app.route("/events/confirm", methods=["POST"])
def confirm_event():
    """Chef: Zusage bestätigen oder ablehnen.
    - decision: 'bestätigt' | 'abgelehnt'
    Hinweis: Chef-Ablehnung wird als 'abgelehnt_chef' gespeichert, damit das UI die Fälle unterscheiden kann.
    """
    if session.get("role") not in ["chef", "vorgesetzter", "vorgesetzter_cp"]:
        return jsonify({"error": "Nicht erlaubt"}), 403

    d = request.json or {}
    event_id = (d.get("event_id") or "").strip()
    username = (d.get("username") or "").strip()
    decision = (d.get("decision") or "").strip()

    if not event_id or not username:
        return jsonify({"error": "event_id und username erforderlich"}), 400

    if decision == "bestätigt":
        decision_db = "bestätigt"
    elif decision == "abgelehnt":
        decision_db = "abgelehnt_chef"
    else:
        return jsonify({"error": "Ungültige Entscheidung"}), 400

    db = get_db()
    user_row = db.execute("SELECT vorname, nachname, email, stundensatz FROM users WHERE username=%s", (username,)).fetchone()
    if not user_row:
        return jsonify({"error": "User nicht gefunden"}), 404
    profile_rate_snapshot = freeze_effective_rate_snapshot(db, event_id, username)

    existing = db.execute(
        "SELECT status, start_time FROM response WHERE event_id=%s AND username=%s",
        (event_id, username)
    ).fetchone()

    if existing:
        if decision_db == "bestätigt":
            db.execute(
                "UPDATE response SET status=%s, profile_rate_snapshot = COALESCE(profile_rate_snapshot, %s) WHERE event_id=%s AND username=%s",
                (decision_db, profile_rate_snapshot, event_id, username)
            )
        else:
            db.execute(
                "UPDATE response SET status=%s WHERE event_id=%s AND username=%s",
                (decision_db, event_id, username)
            )
    else:
        db.execute(
            "INSERT INTO response (event_id, username, status, remark, start_time, end_time, profile_rate_snapshot) VALUES (%s,%s,%s,%s,%s,%s,%s)",
            (event_id, username, decision_db, "", "", "", (profile_rate_snapshot if decision_db == "bestätigt" else None))
        )

    db.commit()

    mail_sent = False
    mail_error = ""
    try:
        event_row = db.execute(
            "SELECT title, start, ort, dienstkleidung FROM event WHERE id=%s",
            (event_id,)
        ).fetchone()
        employee_name = " ".join(filter(None, [
            (user_row.get("vorname") or "").strip(),
            (user_row.get("nachname") or "").strip()
        ])).strip() or username
        to_addr = (user_row.get("email") or "").strip()
        if to_addr and event_row:
            event_title = event_row.get("title") or "Einsatz"
            if decision_db == "bestätigt":
                subject = f"✅ Auftrag bestätigt✅: {event_title}"
                start_override = (existing.get("start_time") if existing else "") if existing else ""
                body = build_confirmation_mail(
                    employee_name=employee_name,
                    event_title=event_title,
                    event_start_dt=event_row.get("start") or "",
                    ort=event_row.get("ort") or "",
                    dienstkleidung=event_row.get("dienstkleidung") or "",
                    start_time=start_override or "",
                )
            else:
                subject = f"❌ Auftrag abgewiesen❌: {event_title}"
                body = build_rejection_mail(
                    employee_name=employee_name,
                    event_title=event_title,
                    event_start_dt=event_row.get("start") or "",
                    ort=event_row.get("ort") or "",
                    dienstkleidung=event_row.get("dienstkleidung") or "",
                )
            send_mail(to_addr, subject, body)
            mail_sent = True
        elif not to_addr:
            mail_error = "Keine E-Mail-Adresse beim Mitarbeiter hinterlegt."
    except Exception as e:
        mail_error = str(e)

    return jsonify({"status": "ok", "mail_sent": mail_sent, "mail_error": mail_error})


@app.route("/events/endtime", methods=["POST"])
def set_endtime():
    """Mitarbeiter: Endzeit EINMALIG speichern."""
    if session.get("role") != "mitarbeiter":
        return jsonify({"error": "Nicht erlaubt"}), 403

    # ✅ DSGVO: erst Einwilligung, dann Aktionen
    if employee_requires_consent():
        return jsonify({"error":"Bitte zuerst im Report in die Datenverarbeitung einwilligen."}), 403

    # ✅ DSGVO: Endzeit erst nach Einwilligung
    info = get_user_consent(get_db(), session.get("username"))
    if not bool(info.get("given")):
        return jsonify({"error": "Einwilligung zur Datenverarbeitung ist erforderlich."}), 403


    d = request.json or {}
    event_id = d.get("event_id")
    end_time = (d.get("end_time") or "").strip()

    if not event_id or not end_time:
        return jsonify({"error": "event_id und end_time erforderlich"}), 400

    db = get_db()

    r = db.execute(
        "SELECT end_time FROM response WHERE event_id=%s AND username=%s",
        (event_id, session["username"])
    ).fetchone()

    if r and (r.get("end_time") or "").strip():
        return jsonify({"error": "Endzeit bereits gespeichert"}), 400

    if r:
        db.execute(
            "UPDATE response SET end_time=%s WHERE event_id=%s AND username=%s",
            (end_time, event_id, session["username"])
        )
    else:
        db.execute(
            "INSERT INTO response (event_id, username, end_time) VALUES (%s,%s,%s)",
            (event_id, session["username"], end_time)
        )

    db.commit()
    return jsonify({"success": True})


@app.route("/events/extra_costs", methods=["POST"])
def save_extra_costs():
    """Zusatzkosten nach gespeicherter Endzeit erfassen/bearbeiten."""
    if "username" not in session:
        return jsonify({"error": "Nicht eingeloggt"}), 403
    role_now = normalize_role(session.get("role") or "")
    d = request.json or {}
    event_id = (d.get("event_id") or "").strip()
    username = (d.get("username") or session.get("username") or "").strip()
    costs = parse_extra_costs_payload(d.get("extra_costs") or d.get("costs") or [])
    if not event_id:
        return jsonify({"error": "event_id erforderlich"}), 400
    if role_now == "mitarbeiter":
        username = session.get("username") or username
    elif role_now not in ["chef", "vorgesetzter", "vorgesetzter_cp"]:
        return jsonify({"error": "Nicht erlaubt"}), 403

    db = get_db()
    resp = db.execute("SELECT end_time FROM response WHERE event_id=%s AND username=%s", (event_id, username)).fetchone()
    if not resp or not (resp.get("end_time") or "").strip():
        return jsonify({"error": "Zusatzkosten können erst nach gespeicherter Endzeit eingetragen werden."}), 400

    replace_response_extra_costs(db, event_id, username, costs)
    db.commit()
    return jsonify({"success": True, "extra_costs": get_response_extra_costs(db, event_id, username)})


@app.route("/events/edit_entry", methods=["POST"])
def edit_entry():
    """
    Chef: Zeiten/Bemerkung/Stundensatz-Override pro Mitarbeiter setzen.
    WICHTIG: Wenn Chef start_time oder remark ändert -> Email an den Mitarbeiter.
    """
    role_now = normalize_role(session.get("role") or "")
    amine_bs_edit = (role_now == "mitarbeiter" and is_amine_salah_user())
    if role_now not in ["chef", "vorgesetzter", "vorgesetzter_cp"] and not amine_bs_edit:
        return jsonify({"error": "Nicht erlaubt"}), 403

    d = request.json or {}
    event_id = (d.get("event_id") or "").strip()
    username = (d.get("username") or "").strip()
    start_time = (d.get("start_time") or "").strip()
    end_time = (d.get("end_time") or "").strip()
    remark = (d.get("remark") or "").strip()

    rate_override = d.get("rate_override", None)
    if rate_override in ("", None):
        rate_override = None
    else:
        try:
            rate_override = float(rate_override)
        except Exception:
            return jsonify({"error": "rate_override ungültig"}), 400

    if not event_id:
        return jsonify({"error": "event_id erforderlich"}), 400

    db = get_db()

    if amine_bs_edit:
        ev = db.execute("SELECT category FROM event WHERE id=%s", (event_id,)).fetchone()
        if not ev or not is_private_amine_category(ev.get("category")):
            return jsonify({"error": "Amine darf nur eigene/private Aufträge bearbeiten."}), 403
        username = session.get("username") or username

    old_start = ""
    old_remark = ""

    if username:
        old_row = db.execute(
            "SELECT start_time, remark, profile_rate_snapshot FROM response WHERE event_id=%s AND username=%s",
            (event_id, username)
        ).fetchone()
        old_start = (old_row.get("start_time") if old_row else "") or ""
        old_remark = (old_row.get("remark") if old_row else "") or ""

        profile_rate_snapshot = freeze_effective_rate_snapshot(db, event_id, username)

        exists = db.execute(
            "SELECT 1 FROM response WHERE event_id=%s AND username=%s",
            (event_id, username)
        ).fetchone()

        if exists:
            db.execute(
                """
                UPDATE response SET
                  start_time    = COALESCE(NULLIF(%s,''), start_time),
                  end_time      = COALESCE(NULLIF(%s,''), end_time),
                  remark        = %s,
                  rate_override = %s,
                  profile_rate_snapshot = COALESCE(profile_rate_snapshot, %s)
                WHERE event_id=%s AND username=%s
                """,
                (start_time, end_time, remark, rate_override, profile_rate_snapshot, event_id, username)
            )
        else:
            db.execute(
                """
                INSERT INTO response (event_id, username, status, remark, start_time, end_time, rate_override, profile_rate_snapshot)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (event_id, username, "bestätigt", remark, start_time or "", end_time or "", rate_override, profile_rate_snapshot)
            )
    else:
        db.execute(
            """
            UPDATE response SET
              end_time      = COALESCE(NULLIF(%s,''), end_time),
              remark        = %s,
              rate_override = %s
            WHERE event_id=%s
            """,
            (end_time, remark, rate_override, event_id)
        )

    if username and isinstance(d.get("extra_costs"), list):
        # Vorgesetzte können Zusatzkosten im Report bearbeiten. Voraussetzung bleibt: Endzeit vorhanden.
        rr = db.execute("SELECT end_time FROM response WHERE event_id=%s AND username=%s", (event_id, username)).fetchone()
        if rr and (rr.get("end_time") or "").strip():
            replace_response_extra_costs(db, event_id, username, d.get("extra_costs") or [])

    db.commit()

    changed_start = bool(start_time) and (start_time != old_start)
    changed_remark = (remark != old_remark)

    if username and (changed_start or changed_remark):
        u = db.execute(
            "SELECT vorname, nachname, email FROM users WHERE username=%s",
            (username,)
        ).fetchone()
        e = db.execute(
            "SELECT title, start, ort, dienstkleidung FROM event WHERE id=%s",
            (event_id,)
        ).fetchone()

        if u and e and (u.get("email") or "").strip():
            employee_name = (f"{(u.get('vorname') or '').strip()} {(u.get('nachname') or '').strip()}").strip() or username
            event_start_dt = ((e.get("start") or "").strip().replace("T", " ")) or "-"
            subject = f"❗ Änderung zu deinem Auftrag: {(e.get('title') or 'Einsatz')}❗"
            body = build_change_mail(
                employee_name=employee_name,
                event_title=(e.get("title") or "Einsatz"),
                event_start_dt=event_start_dt,
                ort=(e.get("ort") or ""),
                dienstkleidung=(e.get("dienstkleidung") or ""),
                new_start_time=(start_time or old_start),
                new_remark=(remark if changed_remark else ""),
            )
            try:
                send_mail((u.get("email") or "").strip(), subject, body)
            except Exception:
                pass

    return jsonify({"status": "ok"})





@app.route("/events/duplicate", methods=["POST"])
def duplicate_event():
    """Chef/Vorgesetzter: Einsatz duplizieren (stabil & fehlertolerant)."""
    role_now = normalize_role(session.get("role") or "")
    amine_bs_duplicate = (role_now == "mitarbeiter" and is_amine_salah_user())
    if role_now not in ["chef", "vorgesetzter", "vorgesetzter_cp"] and not amine_bs_duplicate:
        return jsonify({"error": "Nicht erlaubt"}), 403

    try:
        d = request.json or {}
        source_id = (d.get("event_id") or "").strip()
        if not source_id:
            return jsonify({"error": "event_id fehlt"}), 400

        dates = d.get("dates") or []
        single_start = (d.get("start") or "").strip()

        db = get_db()
        src = db.execute("SELECT * FROM event WHERE id=%s", (source_id,)).fetchone()
        if not src:
            return jsonify({"error": "Event nicht gefunden"}), 404
        if is_private_amine_category(src.get("category")) and not amine_bs_duplicate:
            return jsonify({"error": "Private Einsätze dürfen nur von Amine dupliziert werden."}), 403

        # --- Kategorie sauber normalisieren ---
        src_cat = normalize_private_category(src.get("category") or "PRIVAT") if amine_bs_duplicate else (src.get("category") or "CP").strip().upper()
        if not amine_bs_duplicate and src_cat not in ("CP", "CV"):
            src_cat = "CP"
        if amine_bs_duplicate and not is_private_amine_category(src_cat):
            return jsonify({"error": "Amine darf nur eigene/private Aufträge duplizieren."}), 403

        # --- Uhrzeit aus Quelle holen ---
        src_start = (src.get("start") or "").strip()
        src_time = "09:00"
        m = re.match(r"^\d{4}-\d{2}-\d{2}T(\d{2}:\d{2})", src_start)
        if m:
            src_time = m.group(1)

        def insert_new(start_val: str) -> str:
            new_id = str(uuid.uuid4())
            db.execute(
                """
                INSERT INTO event
                  (id,title,ort,dienstkleidung,auftraggeber,start,
                   planned_end_time,frist,status,category,
                   required_staff,use_event_rate,stundensatz,einsatzleitung_username,einsatzleitung_usernames)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    new_id,
                    src.get("title") or "",
                    src.get("ort") or "",
                    src.get("dienstkleidung") or "",
                    src.get("auftraggeber") or "",
                    start_val,
                    src.get("planned_end_time") or "",
                    src.get("frist") or "",
                    src.get("status") or "geplant",
                    src_cat,
                    int(src.get("required_staff") or 0),
                    int(src.get("use_event_rate") if src.get("use_event_rate") is not None else 1),
                    src.get("stundensatz"),
                    (parse_einsatzleitung_usernames(src.get("einsatzleitung_usernames"), src.get("einsatzleitung_username")) or [None])[0],
                    dump_einsatzleitung_usernames(parse_einsatzleitung_usernames(src.get("einsatzleitung_usernames"), src.get("einsatzleitung_username"))),
                ),
            )
            if amine_bs_duplicate:
                profile_rate_snapshot = freeze_effective_rate_snapshot(db, new_id, session.get("username"))
                db.execute(
                    "INSERT INTO response (event_id, username, status, remark, start_time, end_time, profile_rate_snapshot) VALUES (%s,%s,%s,%s,%s,%s,%s)",
                    (new_id, session.get("username"), "bestätigt", "", "", "", profile_rate_snapshot)
                )
            return new_id

        created_ids = []

        # --- Mehrere Daten ---
        if isinstance(dates, list) and dates:
            for ds in dates:
                ds = (ds or "").strip()
                if not re.match(r"^\d{4}-\d{2}-\d{2}$", ds):
                    continue
                created_ids.append(insert_new(f"{ds}T{src_time}"))

            if not created_ids:
                db.rollback()
                return jsonify({"error": "Keine gültigen Datumswerte"}), 400

            db.commit()
            return jsonify({"status": "ok", "new_event_ids": created_ids}), 200

        # --- Einzeltermin ---
        start_val = single_start or src_start
        if not start_val:
            return jsonify({"error": "start fehlt"}), 400

        new_id = insert_new(start_val)
        db.commit()
        return jsonify({"status": "ok", "new_event_id": new_id}), 200

    except Exception as e:
        try:
            db.rollback()
        except Exception:
            pass
        print("DUPLICATE ERROR:", repr(e))
        return jsonify({"error": "Duplizieren fehlgeschlagen", "detail": str(e)}), 500



@app.route("/events/send_mail_all", methods=["POST"])
def send_mail_all():
    """Chef/Vorgesetzter: Sammel-Mail an alle Mitarbeiter senden.
    Text ist fest vorgegeben (wie in der Anforderung).
    Rückgabe: {"status":"ok","sent":<anzahl>}
    """
    if session.get("role") not in ["chef", "vorgesetzter", "vorgesetzter_cp"]:
        return jsonify({"error": "Nicht erlaubt"}), 403

    db = get_db()
    # "alle Mitarbeiter" = Rolle mitarbeiter (und nur mit gültiger E-Mail)
    cur = db.execute("SELECT vorname, nachname, email FROM users WHERE role=%s", ("mitarbeiter",))
    rows = cur.fetchall() or []

    subject = "Neue Einsätze zum Einbuchen"
    body = (
        "Hallo,\n\n"
        "es wurden neue Einsätze zum Einbuchen im Online-Portal eingestellt.\n\n"
        "Bitte die Rückmeldefrist beachten.\n\n"
        "Viele Grüße\n"
        "CV - Planung\n"
    )

    sent = 0
    for u in rows:
        to_addr = (u.get("email") or "").strip()
        if not to_addr:
            continue
        try:
            send_mail(to_addr, subject, body)
            sent += 1
        except Exception:
            # Mail-Fehler sollen die API nicht kaputt machen
            pass

    return jsonify({"status": "ok", "sent": sent})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "5000")), debug=True)






