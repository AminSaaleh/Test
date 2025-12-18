from flask import Flask, render_template, request, redirect, url_for, session, jsonify, g
import os, uuid, re

import psycopg2
import psycopg2.extras
from psycopg2 import IntegrityError

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "geheimes_passwort")

# Supabase/PostgreSQL connection string (example: postgresql://user:pass@host:5432/dbname)
DATABASE_URL = os.environ.get("DATABASE_URL")

# ---------------- DB helpers (PostgreSQL / Supabase) ----------------
class DBWrapper:
    def __init__(self, conn):
        self.conn = conn

    def execute(self, sql, params=None):
        cur = self.conn.cursor()
        cur.execute(sql, params or ())
        return cur

    def executescript(self, sql):
        # PostgreSQL has no executescript; emulate with a single execute (multiple statements allowed)
        cur = self.conn.cursor()
        cur.execute(sql)
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
            raise RuntimeError("DATABASE_URL ist nicht gesetzt (Supabase-Verbindung fehlt).")

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


def col_exists(db, table, col):
    cur = db.execute(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_name=%s AND column_name=%s
        """,
        (table, col),
    )
    return cur.fetchone() is not None


def row_to_dict(row):
    # RealDictCursor liefert dict-ähnliche Rows
    return dict(row)


def to_int(v, default=0):
    try:
        return int(v)
    except Exception:
        try:
            return int(float(v))
        except Exception:
            return default


def init_db():
    db = get_db()

    # NOTE: In Postgres ist "user" ein reserviertes Wort -> wir nutzen "users".
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            username TEXT PRIMARY KEY,
            password TEXT NOT NULL,
            role TEXT DEFAULT 'mitarbeiter',
            vorname TEXT,
            nachname TEXT,
            s34a TEXT,
            s34a_art TEXT,
            pschein TEXT,
            bewach_id TEXT,
            steuernummer TEXT,
            bsw TEXT,
            sanitaeter TEXT,
            stundensatz DOUBLE PRECISION
        );
        """
    )

    db.execute(
        """
        CREATE TABLE IF NOT EXISTS event (
            id TEXT PRIMARY KEY,
            title TEXT,
            ort TEXT,
            dienstkleidung TEXT,
            auftraggeber TEXT,
            start TEXT,
            planned_end_time TEXT,     -- 'HH:MM'
            status TEXT,               -- 'geplant' | 'offen'
            required_staff INTEGER DEFAULT 0,
            use_event_rate INTEGER DEFAULT 1, -- 1=Einsatz-Stundensatz, 0=User-Profil
            stundensatz DOUBLE PRECISION
        );
        """
    )

    db.execute(
        """
        CREATE TABLE IF NOT EXISTS response (
            id SERIAL PRIMARY KEY,
            event_id TEXT NOT NULL REFERENCES event(id) ON DELETE CASCADE,
            username TEXT NOT NULL REFERENCES users(username) ON DELETE CASCADE,
            status TEXT,       -- 'zugesagt' | 'bestätigt' | 'abgelehnt'
            remark TEXT,
            start_time TEXT,   -- 'HH:MM' (Chef kann pro Mitarbeiter setzen)
            end_time TEXT,     -- 'HH:MM' (Mitarbeiter / Chef)
            UNIQUE(event_id, username)
        );
        """
    )

    # Indizes
    db.execute("CREATE INDEX IF NOT EXISTS idx_response_event ON response(event_id);")
    db.execute("CREATE INDEX IF NOT EXISTS idx_response_user  ON response(username);")

    # ---- Migrationen (falls Tabellen schon existieren, aber Spalten fehlen) ----
    # users
    for c, ddl in [
        ("bewach_id", "ALTER TABLE users ADD COLUMN bewach_id TEXT"),
        ("steuernummer", "ALTER TABLE users ADD COLUMN steuernummer TEXT"),
        ("bsw", "ALTER TABLE users ADD COLUMN bsw TEXT"),
        ("sanitaeter", "ALTER TABLE users ADD COLUMN sanitaeter TEXT"),
        ("stundensatz", "ALTER TABLE users ADD COLUMN stundensatz DOUBLE PRECISION"),
        ("s34a", "ALTER TABLE users ADD COLUMN s34a TEXT"),
        ("s34a_art", "ALTER TABLE users ADD COLUMN s34a_art TEXT"),
        ("pschein", "ALTER TABLE users ADD COLUMN pschein TEXT"),
        ("vorname", "ALTER TABLE users ADD COLUMN vorname TEXT"),
        ("nachname", "ALTER TABLE users ADD COLUMN nachname TEXT"),
        ("role", "ALTER TABLE users ADD COLUMN role TEXT DEFAULT 'mitarbeiter'"),
        ("password", "ALTER TABLE users ADD COLUMN password TEXT"),
    ]:
        if not col_exists(db, "users", c):
            db.execute(ddl)

    # event
    for c, ddl in [
        ("planned_end_time", "ALTER TABLE event ADD COLUMN planned_end_time TEXT"),
        ("status", "ALTER TABLE event ADD COLUMN status TEXT"),
        ("required_staff", "ALTER TABLE event ADD COLUMN required_staff INTEGER DEFAULT 0"),
        ("use_event_rate", "ALTER TABLE event ADD COLUMN use_event_rate INTEGER DEFAULT 1"),
        ("stundensatz", "ALTER TABLE event ADD COLUMN stundensatz DOUBLE PRECISION"),
    ]:
        if not col_exists(db, "event", c):
            db.execute(ddl)

    # response
    for c, ddl in [
        ("status", "ALTER TABLE response ADD COLUMN status TEXT"),
        ("remark", "ALTER TABLE response ADD COLUMN remark TEXT"),
        ("start_time", "ALTER TABLE response ADD COLUMN start_time TEXT"),
        ("end_time", "ALTER TABLE response ADD COLUMN end_time TEXT"),
    ]:
        if not col_exists(db, "response", c):
            db.execute(ddl)

    db.commit()

    # ---- AdminTest (wie bisher) ----
    exists = db.execute("SELECT 1 FROM users WHERE username=%s", ("AdminTest",)).fetchone()
    if not exists:
        db.execute(
            """
            INSERT INTO users
               (username,password,role,vorname,nachname,s34a,s34a_art,pschein,bewach_id,steuernummer,bsw,sanitaeter,stundensatz)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                "AdminTest", "Test1234", "vorgesetzter",
                "Admin", "Test",
                "ja", "sachkunde", "ja",
                "A-000", "ST-000",
                "nein", "nein",
                0.0,
            ),
        )
        db.commit()


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

        if u and (u.get("password") == password):
            session["username"] = username
            session["role"] = (u.get("role") or "mitarbeiter")
            return redirect(url_for("dashboard"))

        return render_template("login.html", error="Login fehlgeschlagen")
    return render_template("login.html")


@app.route("/dashboard")
def dashboard():
    if "username" not in session:
        return redirect(url_for("login"))

    role = session.get("role") or "mitarbeiter"

    # Chef-Dashboard auch für Planer (UI beschränkt Planer auf den Planung-Reiter)
    if role in ["chef", "vorgesetzter", "planer"]:
        return render_template("dashboard_chef.html", user=session["username"], role=role)

    return render_template("dashboard_mitarbeiter.html", user=session["username"], role=role)


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


# ---------------- Users API ----------------
@app.route("/users", methods=["GET"])
def get_users():
    if session.get("role") not in ["chef", "vorgesetzter"]:
        return jsonify({"error": "Nicht erlaubt"}), 403

    cur = get_db().execute(
        "SELECT * FROM users WHERE username NOT IN (%s,%s) ORDER BY nachname, vorname",
        ("AdminTest", "TestAdmin"),
    )
    users = [row_to_dict(r) for r in cur.fetchall()]
    for u in users:
        if u.get("stundensatz") is None:
            u["stundensatz"] = ""
    return jsonify(users)


@app.route("/users_public", methods=["GET"])
def users_public():
    """
    Minimaler User-Export (nur Name) für Planung.
    Erlaubt für eingeloggte Rollen inkl. Planer – ohne sensible Felder/Passwörter.
    """
    if "username" not in session:
        return jsonify({"error": "Nicht eingeloggt"}), 403

    if session.get("role") not in ["chef", "vorgesetzter", "planer"]:
        return jsonify({"error": "Nicht erlaubt"}), 403

    cur = get_db().execute(
        "SELECT username, vorname, nachname FROM users WHERE username NOT IN (%s,%s) ORDER BY nachname, vorname",
        ("AdminTest", "TestAdmin"),
    )
    users = [row_to_dict(r) for r in cur.fetchall()]
    return jsonify(users)


@app.route("/users", methods=["POST"])
def add_user():
    if session.get("role") not in ["chef", "vorgesetzter"]:
        return jsonify({"error": "Nicht erlaubt"}), 403

    d = request.json or {}
    username = (d.get("username") or "").strip()
    if not username:
        return jsonify({"error": "username ist erforderlich"}), 400

    db = get_db()
    if db.execute("SELECT 1 FROM users WHERE username=%s", (username,)).fetchone():
        return jsonify({"error": "Benutzername existiert schon"}), 400

    stundensatz = d.get("stundensatz")
    stundensatz = None if stundensatz in (None, "") else float(stundensatz)

    db.execute(
        """
        INSERT INTO users
           (username,password,role,vorname,nachname,s34a,s34a_art,pschein,bewach_id,steuernummer,bsw,sanitaeter,stundensatz)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        (
            username,
            d.get("password") or "",
            d.get("role") or "mitarbeiter",
            d.get("vorname") or "",
            d.get("nachname") or "",
            d.get("s34a") or "nein",
            d.get("s34a_art") or "",
            d.get("pschein") or "nein",
            d.get("bewach_id") or "",
            d.get("steuernummer") or "",
            d.get("bsw") or "nein",
            d.get("sanitaeter") or "nein",
            stundensatz,
        ),
    )
    db.commit()
    return jsonify({"status": "ok"})


@app.route("/users/rename", methods=["POST"])
def rename_user():
    if session.get("role") not in ["chef", "vorgesetzter"]:
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

        # FK-safe: neuen User anlegen, response umhängen, alten User löschen
        db.execute(
            """
            INSERT INTO users
               (username,password,role,vorname,nachname,s34a,s34a_art,pschein,bewach_id,steuernummer,bsw,sanitaeter,stundensatz)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                new_username,
                old.get("password") or "",
                old.get("role") or "mitarbeiter",
                old.get("vorname") or "",
                old.get("nachname") or "",
                old.get("s34a") or "nein",
                old.get("s34a_art") or "",
                old.get("pschein") or "nein",
                old.get("bewach_id") or "",
                old.get("steuernummer") or "",
                old.get("bsw") or "nein",
                old.get("sanitaeter") or "nein",
                old.get("stundensatz"),
            ),
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
    if session.get("role") not in ["chef", "vorgesetzter"]:
        return jsonify({"error": "Nicht erlaubt"}), 403

    d = request.json or {}
    db = get_db()

    u = db.execute("SELECT * FROM users WHERE username=%s", (username,)).fetchone()
    if not u:
        return jsonify({"error": "Benutzer nicht gefunden"}), 404

    updates = dict(u)
    for k in ["vorname", "nachname", "role", "s34a", "s34a_art", "pschein",
              "bewach_id", "steuernummer", "bsw", "sanitaeter"]:
        if k in d:
            updates[k] = d[k]

    if "password" in d and d["password"] is not None:
        updates["password"] = d["password"]

    if "stundensatz" in d:
        updates["stundensatz"] = None if d["stundensatz"] in ("", None) else float(d["stundensatz"])

    db.execute(
        """
        UPDATE users SET
           password=%s, role=%s, vorname=%s, nachname=%s, s34a=%s, s34a_art=%s, pschein=%s,
           bewach_id=%s, steuernummer=%s, bsw=%s, sanitaeter=%s, stundensatz=%s
        WHERE username=%s
        """,
        (
            updates.get("password") or "",
            updates.get("role") or "mitarbeiter",
            updates.get("vorname") or "",
            updates.get("nachname") or "",
            updates.get("s34a") or "nein",
            updates.get("s34a_art") or "",
            updates.get("pschein") or "nein",
            updates.get("bewach_id") or "",
            updates.get("steuernummer") or "",
            updates.get("bsw") or "nein",
            updates.get("sanitaeter") or "nein",
            updates.get("stundensatz"),
            username,
        ),
    )
    db.commit()
    return jsonify({"status": "ok"})


@app.route("/users/<username>", methods=["DELETE"])
def delete_user(username):
    if session.get("role") not in ["chef", "vorgesetzter"]:
        return jsonify({"error": "Nicht erlaubt"}), 403
    db = get_db()
    db.execute("DELETE FROM users WHERE username=%s", (username,))
    db.commit()
    return jsonify({"status": "ok"})


# ---------------- Events API ----------------
@app.route("/events", methods=["GET"])
def events_list():
    # ✅ Login erforderlich (damit Planer/Mitarbeiter nicht anonym zugreifen)
    if "username" not in session:
        return jsonify({"error": "Nicht eingeloggt"}), 403

    db = get_db()
    role = session.get("role") or "mitarbeiter"

    ecur = db.execute("SELECT * FROM event")
    events = [row_to_dict(e) for e in ecur.fetchall()]

    # Mitarbeiter: Profil-Stundensatz holen (für my_rate)
    my_profile_rate = 0.0
    if role not in ["chef", "vorgesetzter", "planer"]:
        me = db.execute("SELECT * FROM users WHERE username=%s", (session.get("username"),)).fetchone()
        if me:
            my_profile_rate = float(me.get("stundensatz") or 0.0)

    result = []
    for e in events:
        rcur = db.execute(
            "SELECT username,status,remark,start_time,end_time FROM response WHERE event_id=%s",
            (e["id"],),
        )
        rmap = {
            r["username"]: {
                "status": (r.get("status") or ""),
                "remark": (r.get("remark") or ""),
                "start_time": (r.get("start_time") or ""),
                "end_time": (r.get("end_time") or ""),
            } for r in rcur.fetchall()
        }
        e["responses"] = rmap

        # ✅ BUGFIX: 0 darf NICHT zu 1 werden
        raw_u = e.get("use_event_rate")
        use_event_rate = 1 if raw_u is None else int(raw_u)

        # Chef/Vorgesetzter/Planer: keine eigenen Raten berechnen
        if role in ["chef", "vorgesetzter", "planer"]:
            e["my_rate"] = 0
        else:
            if use_event_rate == 1:
                e["my_rate"] = float(e.get("stundensatz") or 0.0)
            else:
                e["my_rate"] = my_profile_rate

        result.append(e)

    return jsonify(result)


@app.route("/events", methods=["POST"])
def add_event():
    if session.get("role") not in ["chef", "vorgesetzter"]:
        return jsonify({"error": "Nicht erlaubt"}), 403

    d = request.json or {}
    ev_id = str(uuid.uuid4())

    start = d.get("start") or ""
    planned_end_time = (d.get("planned_end_time") or "").strip()

    status = d.get("status", "geplant")
    required_staff = to_int(d.get("required_staff", 0), 0)

    use_event_rate = to_int(d.get("use_event_rate", 1), 1)
    stundensatz = d.get("stundensatz")
    stundensatz = None if stundensatz in ("", None) else float(stundensatz)
    if use_event_rate == 0:
        stundensatz = None

    db = get_db()
    db.execute(
        """
        INSERT INTO event
           (id,title,ort,dienstkleidung,auftraggeber,start,planned_end_time,status,required_staff,use_event_rate,stundensatz)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        (
            ev_id,
            d.get("title") or "",
            d.get("ort") or "",
            d.get("dienstkleidung") or "",
            d.get("auftraggeber") or "",
            start,
            planned_end_time,
            status,
            required_staff,
            use_event_rate,
            stundensatz,
        ),
    )
    db.commit()
    return jsonify({"status": "ok"})


@app.route("/events/assign_user", methods=["POST"])
def assign_user():
    """Chef: Mitarbeiter als bestätigt zuweisen."""
    if session.get("role") not in ["chef", "vorgesetzter"]:
        return jsonify({"error": "Nicht erlaubt"}), 403

    d = request.json or {}
    event_id = d.get("event_id")
    username = d.get("username")

    if not event_id or not username:
        return jsonify({"error": "event_id und username erforderlich"}), 400

    db = get_db()
    if not db.execute("SELECT 1 FROM event WHERE id=%s", (event_id,)).fetchone():
        return jsonify({"error": "Event nicht gefunden"}), 404

    if not db.execute("SELECT 1 FROM users WHERE username=%s", (username,)).fetchone():
        return jsonify({"error": "User nicht gefunden"}), 404

    if db.execute("SELECT 1 FROM response WHERE event_id=%s AND username=%s", (event_id, username)).fetchone():
        db.execute(
            "UPDATE response SET status='bestätigt' WHERE event_id=%s AND username=%s",
            (event_id, username),
        )
    else:
        db.execute(
            "INSERT INTO response (event_id, username, status, remark, start_time, end_time) VALUES (%s,%s,%s,%s,%s,%s)",
            (event_id, username, "bestätigt", "", "", ""),
        )

    db.commit()
    return jsonify({"status": "ok"})


@app.route("/events/remove_user", methods=["POST"])
def remove_user_from_event():
    """Chef: Mitarbeiter komplett aus Einsatz entfernen."""
    if session.get("role") not in ["chef", "vorgesetzter"]:
        return jsonify({"error": "Nicht erlaubt"}), 403

    d = request.json or {}
    event_id = d.get("event_id")
    username = d.get("username")

    if not event_id or not username:
        return jsonify({"error": "event_id und username erforderlich"}), 400

    db = get_db()
    # Statt Löschen: auf "abgelehnt" setzen, damit der Mitarbeiter den Einsatz nicht mehr sieht
    cur = db.execute(
        "UPDATE response SET status=%s WHERE event_id=%s AND username=%s",
        ("abgelehnt", event_id, username),
    )

    # Falls es noch keinen Response-Eintrag gab, legen wir einen abgelehnten an
    if cur.rowcount == 0:
        db.execute(
            "INSERT INTO response (event_id, username, status, remark, start_time, end_time) VALUES (%s,%s,%s,%s,%s,%s)",
            (event_id, username, "abgelehnt", "", "", ""),
        )
    db.commit()
    return jsonify({"status": "ok"})


@app.route("/events/<event_id>", methods=["DELETE"])
def delete_event(event_id):
    if session.get("role") not in ["chef", "vorgesetzter"]:
        return jsonify({"error": "Nicht erlaubt"}), 403
    db = get_db()
    db.execute("DELETE FROM event WHERE id=%s", (event_id,))
    db.commit()
    return jsonify({"status": "ok"})


@app.route("/events/release", methods=["POST"])
def release_event():
    if session.get("role") not in ["chef", "vorgesetzter"]:
        return jsonify({"error": "Nicht erlaubt"}), 403
    d = request.json or {}
    event_id = d.get("event_id")

    db = get_db()
    cur = db.execute("UPDATE event SET status='offen' WHERE id=%s", (event_id,))
    if cur.rowcount == 0:
        return jsonify({"error": "Event nicht gefunden"}), 404

    db.commit()
    return jsonify({"status": "ok"})


@app.route("/events/update", methods=["POST"])
def update_event():
    if session.get("role") not in ["chef", "vorgesetzter"]:
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
    status = d.get("status") or "geplant"
    required_staff = to_int(d.get("required_staff", 0), 0)

    use_event_rate = to_int(d.get("use_event_rate", 1), 1)
    stundensatz = d.get("stundensatz")
    stundensatz = None if stundensatz in ("", None) else float(stundensatz)
    if use_event_rate == 0:
        stundensatz = None

    db = get_db()
    cur = db.execute(
        """
        UPDATE event SET
           title=%s, ort=%s, dienstkleidung=%s, auftraggeber=%s,
           start=%s, planned_end_time=%s, status=%s, required_staff=%s,
           use_event_rate=%s, stundensatz=%s
        WHERE id=%s
        """,
        (
            title, ort, dienstkleidung, auftraggeber,
            start, planned_end_time, status, required_staff,
            use_event_rate, stundensatz,
            event_id,
        ),
    )
    if cur.rowcount == 0:
        return jsonify({"error": "Event nicht gefunden"}), 404

    db.commit()
    return jsonify({"status": "ok"})


@app.route("/events/respond", methods=["POST"])
def respond_event():
    if session.get("role") != "mitarbeiter":
        return jsonify({"error": "Nicht erlaubt"}), 403

    d = request.json or {}
    event_id = d.get("event_id")
    response_val = d.get("response")
    remark = ""  # Bemerkungen deaktiviert

    db = get_db()
    ev = db.execute("SELECT 1 FROM event WHERE id=%s", (event_id,)).fetchone()
    if not ev:
        return jsonify({"error": "Event nicht gefunden"}), 404

    me = db.execute("SELECT * FROM users WHERE username=%s", (session["username"],)).fetchone()
    if not me:
        return jsonify({"error": "Nicht eingeloggt"}), 403

    if db.execute("SELECT 1 FROM response WHERE event_id=%s AND username=%s", (event_id, me["username"])).fetchone():
        db.execute(
            "UPDATE response SET status=%s, remark=%s WHERE event_id=%s AND username=%s",
            (response_val, remark, event_id, me["username"]),
        )
    else:
        db.execute(
            "INSERT INTO response (event_id, username, status, remark) VALUES (%s,%s,%s,%s)",
            (event_id, me["username"], response_val, remark),
        )
    db.commit()
    return jsonify({"status": "ok"})


@app.route("/events/confirm", methods=["POST"])
def confirm_event():
    if session.get("role") not in ["chef", "vorgesetzter"]:
        return jsonify({"error": "Nicht erlaubt"}), 403

    d = request.json or {}
    event_id, username, decision = d.get("event_id"), d.get("username"), d.get("decision")

    db = get_db()
    if db.execute("SELECT 1 FROM response WHERE event_id=%s AND username=%s", (event_id, username)).fetchone():
        db.execute("UPDATE response SET status=%s WHERE event_id=%s AND username=%s", (decision, event_id, username))
    else:
        # wenn Chef direkt bestätigt, legen wir Response an
        db.execute(
            "INSERT INTO response (event_id, username, status, remark, start_time, end_time) VALUES (%s,%s,%s,%s,%s,%s)",
            (event_id, username, decision, "", "", ""),
        )
    db.commit()
    return jsonify({"status": "ok"})


@app.route("/events/endtime", methods=["POST"])
def set_endtime():
    """Mitarbeiter: Endzeit EINMALIG speichern."""
    if session.get("role") != "mitarbeiter":
        return jsonify({"error": "Nicht erlaubt"}), 403

    d = request.json or {}
    event_id = d.get("event_id")
    end_time = (d.get("end_time") or "").strip()

    if not event_id or not end_time:
        return jsonify({"error": "event_id und end_time erforderlich"}), 400

    db = get_db()

    r = db.execute(
        "SELECT end_time FROM response WHERE event_id=%s AND username=%s",
        (event_id, session["username"]),
    ).fetchone()

    if r and (r.get("end_time") or "").strip():
        return jsonify({"error": "Endzeit bereits gespeichert"}), 400

    if r:
        db.execute(
            "UPDATE response SET end_time=%s WHERE event_id=%s AND username=%s",
            (end_time, event_id, session["username"]),
        )
    else:
        db.execute(
            "INSERT INTO response (event_id, username, end_time) VALUES (%s,%s,%s)",
            (event_id, session["username"], end_time),
        )

    db.commit()
    return jsonify({"success": True})


@app.route("/events/edit_entry", methods=["POST"])
def edit_entry():
    """
    Chef: Zeiten/Bemerkung pro Mitarbeiter setzen.
    - start_time: Chef-Startzeit (HH:MM)
    - end_time: optional (Chef kann auch Endzeit setzen/ändern)
    - remark: optional (deaktiviert)
    """
    if session.get("role") not in ["chef", "vorgesetzter"]:
        return jsonify({"error": "Nicht erlaubt"}), 403

    d = request.json or {}
    event_id = d.get("event_id")
    username = d.get("username")
    start_time = (d.get("start_time") or "").strip()
    end_time = (d.get("end_time") or "").strip()
    remark = ""  # Bemerkungen deaktiviert

    if not event_id or not username:
        return jsonify({"error": "event_id und username erforderlich"}), 400

    db = get_db()

    exists = db.execute(
        "SELECT 1 FROM response WHERE event_id=%s AND username=%s",
        (event_id, username),
    ).fetchone()

    if exists:
        # Nur überschreiben wenn Feld NICHT leer gesendet wird (sonst alten Wert behalten)
        db.execute(
            """
            UPDATE response SET
              start_time = COALESCE(NULLIF(%s,''), start_time),
              end_time   = COALESCE(NULLIF(%s,''), end_time),
              remark     = ''
            WHERE event_id=%s AND username=%s
            """,
            (start_time, end_time, event_id, username),
        )
    else:
        # Wenn noch keine response existiert: Chef setzt => status "bestätigt"
        db.execute(
            """
            INSERT INTO response (event_id, username, status, remark, start_time, end_time)
            VALUES (%s,%s,%s,%s,%s,%s)
            """,
            (event_id, username, "bestätigt", "", start_time or "", end_time or ""),
        )

    db.commit()
    return jsonify({"status": "ok"})


@app.route("/events/duplicate", methods=["POST"])
def duplicate_event():
    """Chef/Vorgesetzter: Einsatz duplizieren – optional auf mehrere Daten.
    Payload:
      - event_id: Quelleinsatz-ID (Pflicht)
      - dates: Liste von 'YYYY-MM-DD' (optional)
      - start: einzelner ISO 'YYYY-MM-DDTHH:MM' (optional; fallback)
    Verhalten:
      - Wenn dates gesetzt ist: pro Datum wird ein neuer Einsatz erstellt.
        Uhrzeit wird aus Quelle (start) übernommen.
      - Wenn start gesetzt ist: genau ein neuer Einsatz mit dieser Startzeit.
    """
    if session.get("role") not in ["chef", "vorgesetzter"]:
        return jsonify({"error": "Nicht erlaubt"}), 403

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

    # Quelle-Uhrzeit ermitteln (HH:MM)
    src_start = (src.get("start") or "").strip()
    src_time = "09:00"
    m = re.match(r"^\d{4}-\d{2}-\d{2}T(\d{2}:\d{2})", src_start)
    if m:
        src_time = m.group(1)
    else:
        # Fallback: wenn nur Uhrzeit gespeichert wäre
        m2 = re.match(r"^(\d{1,2}:\d{2})$", src_start)
        if m2:
            hhmm = m2.group(1).split(":")
            src_time = f"{int(hhmm[0]):02d}:{hhmm[1]}"

    def insert_new(start_val: str):
        new_id = str(uuid.uuid4())
        db.execute(
            """
            INSERT INTO event
               (id,title,ort,dienstkleidung,auftraggeber,start,planned_end_time,status,required_staff,use_event_rate,stundensatz)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                new_id,
                src.get("title") or "",
                src.get("ort") or "",
                src.get("dienstkleidung") or "",
                src.get("auftraggeber") or "",
                start_val,
                src.get("planned_end_time") or "",
                src.get("status") or "geplant",
                int(src.get("required_staff") or 0),
                int(src.get("use_event_rate") if src.get("use_event_rate") is not None else 1),
                src.get("stundensatz"),
            ),
        )
        return new_id

    created_ids = []

    # Mehrere Daten
    if isinstance(dates, list) and len(dates) > 0:
        for ds in dates:
            ds = (ds or "").strip()
            if not re.match(r"^\d{4}-\d{2}-\d{2}$", ds):
                continue
            start_val = f"{ds}T{src_time}"
            created_ids.append(insert_new(start_val))

        if not created_ids:
            return jsonify({"error": "Keine gültigen Datumswerte übergeben"}), 400

        db.commit()
        return jsonify({"status": "ok", "new_event_ids": created_ids})

    # Einzelstart
    start_val = single_start or src_start
    if not start_val:
        return jsonify({"error": "start fehlt"}), 400

    new_id = insert_new(start_val)
    db.commit()
    return jsonify({"status": "ok", "new_event_id": new_id})


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


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "5000")), debug=True)
