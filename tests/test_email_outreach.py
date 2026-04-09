import unittest
from datetime import datetime, timedelta, timezone

from app.services.email_outreach import (
    create_email_campaign,
    get_email_warmup_status,
    process_post_scrape_automations,
    send_email_campaign,
    upsert_email_warmup_settings,
)


class FakeEmailSender:
    def __init__(self):
        self.sent = []

    def send_email(self, **payload):
        self.sent.append(payload)
        return f"resend-{len(self.sent)}"


class FakeStorage:
    def __init__(self):
        self.jobs = {}
        self.templates = {}
        self.campaigns = {}
        self.deliveries = {}
        self.automation_rules = {}
        self.warmup_settings = None
        self._campaign_counter = 0
        self._delivery_counter = 0
        self._rule_counter = 0

    def list_jobs_for_email(self, source=None, run_id=None, job_ids=None, only_not_emailed=False, require_email=True):
        rows = list(self.jobs.values())
        if source:
            rows = [row for row in rows if row.get("source") == source]
        if run_id:
            rows = [row for row in rows if row.get("last_run_id") == run_id]
        if job_ids:
            rows = [row for row in rows if row.get("id") in set(job_ids)]
        if only_not_emailed:
            rows = [row for row in rows if row.get("email_status") != "sent"]
        if require_email:
            rows = [row for row in rows if row.get("employer_email")]
        return rows

    def get_job(self, job_id):
        return self.jobs.get(job_id)

    def mark_jobs_email_queued(self, job_ids):
        for job_id in job_ids:
            if job_id in self.jobs:
                self.jobs[job_id]["email_status"] = "queued"
                self.jobs[job_id]["email_last_error"] = None

    def mark_job_email_sent(self, job_id, *, sent_at):
        job = self.jobs[job_id]
        job["email_status"] = "sent"
        job["email_last_sent_at"] = sent_at
        job["email_last_error"] = None
        job["email_send_count"] = job.get("email_send_count", 0) + 1

    def mark_job_email_failed(self, job_id, *, error):
        job = self.jobs[job_id]
        job["email_status"] = "failed"
        job["email_last_error"] = error

    def get_email_template(self, template_id):
        return self.templates.get(template_id)

    def create_email_campaign(self, campaign):
        self._campaign_counter += 1
        row = {
            "id": f"campaign-{self._campaign_counter}",
            "created_at": datetime.now(timezone.utc).isoformat(),
            **campaign,
        }
        self.campaigns[row["id"]] = row
        return row

    def get_email_campaign(self, campaign_id):
        return self.campaigns.get(campaign_id)

    def list_email_campaigns(self):
        return list(self.campaigns.values())

    def list_queued_email_campaigns(self):
        return [campaign for campaign in self.campaigns.values() if campaign.get("status") == "queued"]

    def update_email_campaign(self, campaign_id, payload):
        self.campaigns[campaign_id].update(payload)

    def insert_email_deliveries(self, deliveries):
        for delivery in deliveries:
            self._delivery_counter += 1
            row = {
                "id": f"delivery-{self._delivery_counter}",
                "created_at": datetime.now(timezone.utc).isoformat(),
                **delivery,
            }
            self.deliveries[row["id"]] = row
        return len(deliveries)

    def list_email_deliveries(self, campaign_id):
        return [delivery for delivery in self.deliveries.values() if delivery.get("campaign_id") == campaign_id]

    def update_email_delivery(self, delivery_id, payload):
        self.deliveries[delivery_id].update(payload)

    def upsert_email_automation_rule(self, rule):
        self._rule_counter += 1
        row = {
            "id": f"rule-{self._rule_counter}",
            "created_at": datetime.now(timezone.utc).isoformat(),
            **rule,
        }
        self.automation_rules[row["id"]] = row
        return row

    def list_email_automation_rules(self, enabled_only=False):
        rows = list(self.automation_rules.values())
        if enabled_only:
            rows = [row for row in rows if row.get("enabled")]
        return rows

    def upsert_email_warmup_settings(self, settings):
        self.warmup_settings = {"id": "warmup-1", **settings}
        return self.warmup_settings

    def get_email_warmup_settings(self):
        return self.warmup_settings

    def count_sent_email_deliveries_between(self, start_iso, end_iso):
        start = datetime.fromisoformat(start_iso)
        end = datetime.fromisoformat(end_iso)
        total = 0
        for delivery in self.deliveries.values():
            if delivery.get("status") != "sent" or not delivery.get("sent_at"):
                continue
            sent_at = datetime.fromisoformat(delivery["sent_at"])
            if start <= sent_at < end:
                total += 1
        return total


class EmailOutreachTests(unittest.TestCase):
    def setUp(self):
        self.storage = FakeStorage()
        self.storage.jobs = {
            "job-1": {
                "id": "job-1",
                "title": "Sales",
                "company": "Alpha",
                "location": "Zagreb",
                "source": "hzz",
                "detail_url": "https://example.com/jobs/1",
                "published_at": "2026-04-09",
                "employer_email": "contact@alpha.test",
                "email_status": "not_sent",
                "email_send_count": 0,
                "last_run_id": "run-1",
            },
            "job-2": {
                "id": "job-2",
                "title": "Support",
                "company": "Alpha Branch",
                "location": "Split",
                "source": "hzz",
                "detail_url": "https://example.com/jobs/2",
                "published_at": "2026-04-09",
                "employer_email": "contact@alpha.test",
                "email_status": "not_sent",
                "email_send_count": 0,
                "last_run_id": "run-1",
            },
            "job-3": {
                "id": "job-3",
                "title": "Engineer",
                "company": "Beta",
                "location": "Rijeka",
                "source": "hzz",
                "detail_url": "https://example.com/jobs/3",
                "published_at": "2026-04-09",
                "employer_email": "hello@beta.test",
                "email_status": "not_sent",
                "email_send_count": 0,
                "last_run_id": "run-1",
            },
        }

    def test_create_email_campaign_deduplicates_recipients(self):
        result = create_email_campaign(
            name="April Outreach",
            job_ids=["job-1", "job-2"],
            subject="Hi {{company}}",
            html_content="<p>Hello {{company}}</p>",
            storage=self.storage,
        )

        self.assertEqual(result["status"], "draft")
        self.assertEqual(result["total_recipients"], 1)
        self.assertEqual(result["queued_count"], 1)
        self.assertEqual(self.storage.jobs["job-1"]["email_status"], "queued")
        self.assertEqual(len(self.storage.list_email_deliveries(result["campaign_id"])), 1)

    def test_send_email_campaign_respects_daily_warmup_limit(self):
        sender = FakeEmailSender()
        now = datetime(2026, 4, 9, 10, 0, tzinfo=timezone.utc)
        upsert_email_warmup_settings(
            enabled=True,
            initial_daily_limit=1,
            daily_increment=0,
            increment_interval_days=1,
            max_daily_limit=1,
            started_at=now - timedelta(days=2),
            storage=self.storage,
        )

        created = create_email_campaign(
            name="Warmup Campaign",
            job_ids=["job-1", "job-3"],
            subject="Hi {{company}}",
            html_content="<p>Hello {{company}}</p>",
            sender_email="sales@example.com",
            queue_immediately=True,
            storage=self.storage,
        )

        result = send_email_campaign(
            campaign_id=created["campaign_id"],
            storage=self.storage,
            email_sender=sender,
            now=now,
        )

        self.assertEqual(len(sender.sent), 1)
        self.assertEqual(result["sent_count"], 1)
        self.assertEqual(result["queued_count"], 1)
        self.assertEqual(result["status"], "queued")
        self.assertEqual(result["warmup_remaining_today"], 0)

    def test_process_post_scrape_automations_creates_campaign_for_matching_run(self):
        self.storage.upsert_email_automation_rule(
            {
                "name": "After HZZ Scrape",
                "enabled": True,
                "source": "hzz",
                "template_id": None,
                "subject": "Hi {{company}}",
                "html_content": "<p>Hello {{company}}</p>",
                "text_content": None,
                "sender_email": "sales@example.com",
                "reply_to_email": None,
                "created_by": "tester",
                "auto_send": False,
                "delay_minutes": 30,
                "only_not_emailed": True,
                "require_email": True,
            }
        )

        result = process_post_scrape_automations(
            run_id="run-1",
            source="hzz",
            storage=self.storage,
            now=datetime(2026, 4, 9, 10, 0, tzinfo=timezone.utc),
        )

        self.assertEqual(len(result["campaign_ids"]), 1)
        campaign = self.storage.get_email_campaign(result["campaign_ids"][0])
        self.assertEqual(campaign["mode"], "automation_after_scrape")
        self.assertEqual(campaign["status"], "queued")
        self.assertEqual(campaign["last_scrape_run_id"], "run-1")

    def test_warmup_status_increases_until_max_limit(self):
        now = datetime(2026, 4, 9, 10, 0, tzinfo=timezone.utc)
        upsert_email_warmup_settings(
            enabled=True,
            initial_daily_limit=10,
            daily_increment=5,
            increment_interval_days=1,
            max_daily_limit=20,
            started_at=now - timedelta(days=3),
            storage=self.storage,
        )

        status = get_email_warmup_status(storage=self.storage, now=now)

        self.assertEqual(status["effective_daily_limit"], 20)
        self.assertEqual(status["sent_today"], 0)
        self.assertEqual(status["remaining_today"], 20)


if __name__ == "__main__":
    unittest.main()
