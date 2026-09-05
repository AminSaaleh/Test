import os
import sys
import unittest
from unittest.mock import patch
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.pop("SUPER_ADMIN_USERNAMES", None)

import app as app_module


class FakeCursor:
    def __init__(self, row=None, rows=None):
        self.row = row
        self.rows = rows or []

    def fetchone(self):
        return self.row

    def fetchall(self):
        return self.rows


class FakeDB:
    def __init__(self):
        self.statements = []
        self.commits = 0

    def execute(self, statement, params=None):
        self.statements.append((str(statement), params or ()))
        if "SELECT 1 FROM organizations" in str(statement):
            return FakeCursor({"exists": 1})
        return FakeCursor()

    def commit(self):
        self.commits += 1

    def rollback(self):
        pass


class OrganizationFoundationTests(unittest.TestCase):
    def setUp(self):
        app_module.app.config.update(TESTING=True, SECRET_KEY="organization-test")
        self.client = app_module.app.test_client()

    def test_anonymous_superadmin_redirects_to_login(self):
        response = self.client.get("/superadmin")
        self.assertEqual(response.status_code, 302)

    def test_existing_employee_does_not_gain_superadmin_access(self):
        with self.client.session_transaction() as session:
            session["username"] = "existing-user"
            session["role"] = "mitarbeiter"
        response = self.client.get("/superadmin")
        self.assertEqual(response.status_code, 403)

    def test_superadmin_role_is_recognized(self):
        with app_module.app.test_request_context("/"):
            app_module.session["username"] = "platform-admin"
            app_module.session["role"] = "superadmin"
            self.assertTrue(app_module.is_super_admin())

    def test_new_organization_requires_name_and_key_before_db_access(self):
        with self.client.session_transaction() as session:
            session["username"] = "platform-admin"
            session["role"] = "superadmin"
        response = self.client.post("/api/superadmin/organizations", json={})
        self.assertEqual(response.status_code, 400)

    def test_feature_catalog_contains_core_and_optional_modules(self):
        self.assertIn("dashboard", app_module.ORGANIZATION_FEATURES)
        self.assertIn("invoices", app_module.ORGANIZATION_FEATURES)
        self.assertIn("id_card", app_module.ORGANIZATION_FEATURES)
        self.assertIn("email_notifications", app_module.ORGANIZATION_FEATURES)

    def test_creating_company_builds_separate_test_and_production_areas(self):
        fake_db = FakeDB()
        with self.client.session_transaction() as session:
            session["username"] = "platform-admin"
            session["role"] = "superadmin"
        with patch.object(app_module, "get_db", return_value=fake_db):
            response = self.client.post("/api/superadmin/organizations", json={
                "name": "Beispiel Sicherheitsdienst", "organization_key": "beispiel",
                "primary_color": "#2f7d57",
            })
        self.assertEqual(response.status_code, 201)
        organization_inserts = [params for sql, params in fake_db.statements if "INSERT INTO organizations" in sql]
        self.assertEqual(len(organization_inserts), 2)
        self.assertEqual({params[3] for params in organization_inserts}, {"test", "production"})
        self.assertGreaterEqual(fake_db.commits, 1)

    def test_feature_change_is_audited(self):
        fake_db = FakeDB()
        with self.client.session_transaction() as session:
            session["username"] = "platform-admin"
            session["role"] = "superadmin"
        with patch.object(app_module, "get_db", return_value=fake_db):
            response = self.client.put(
                "/api/superadmin/organizations/org-as-test/features/invoices",
                json={"enabled": True},
            )
        self.assertEqual(response.status_code, 200)
        self.assertTrue(any("INSERT INTO superadmin_audit_log" in sql for sql, _ in fake_db.statements))


if __name__ == "__main__":
    unittest.main()
