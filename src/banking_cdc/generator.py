"""Synthetic Banco Atlântico activity.

The simulator is pure: it returns batches of SQL operations (applied to the cards database by
`banking_cdc.seed`) and partner CSV files (dropped on the file share). Fraud patterns are planted
on purpose and recorded in `ground_truth`, which never reaches a source system, so the pipeline's
detection can be scored with precision/recall instead of "it works".
"""

from __future__ import annotations

import csv
import io
import random
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal

from faker import Faker

from banking_cdc.validation import iban_is_valid  # noqa: F401  (re-exported for tests)

MERCHANTS = [
    ("Continente", "5411"),
    ("Pingo Doce", "5411"),
    ("Galp", "5541"),
    ("CP Comboios", "4112"),
    ("Worten", "5732"),
    ("Zara", "5651"),
    ("Uber", "4121"),
    ("Booking.com", "4722"),
    ("Restaurante O Tasco", "5812"),
    ("Farmácia Central", "5912"),
]
FOREIGN = ["ES", "FR", "GB", "BR", "US", "NG", "SG"]
STRUCTURING_LOW = Decimal("9000.00")
STRUCTURING_HIGH = Decimal("9999.99")
FILE_COLUMNS = [
    "transfer_id",
    "value_date",
    "booking_ts",
    "debtor_iban",
    "creditor_iban",
    "amount",
    "currency",
    "reference",
]


def _pt_iban(nib19: str) -> str:
    """Portuguese IBAN from bank(4) + branch(4) + account(11).

    The NIB carries its own mod-97 check digits, which is why every valid PT IBAN starts PT50.
    """
    nib = f"{nib19}{98 - (int(nib19) * 100) % 97:02d}"
    digits = "".join(str(int(ch, 36)) for ch in nib + "PT00")
    return f"PT{98 - int(digits) % 97:02d}{nib}"


@dataclass
class Batch:
    sql_ops: list[tuple[str, str, dict]] = field(default_factory=list)  # (table, op, row)
    files: list[tuple[str, str]] = field(default_factory=list)  # (file name, csv text)
    ground_truth: list[dict] = field(default_factory=list)


class BankSimulator:
    def __init__(
        self,
        seed: int,
        start: datetime,
        n_customers: int = 200,
        tx_per_round: int = 150,
        transfers_per_file: int = 25,
    ):
        self.rng = random.Random(seed)
        self.fake = Faker("pt_PT")
        self.fake.seed_instance(seed)
        self.clock = start
        self.n_customers = n_customers
        self.tx_per_round = tx_per_round
        self.transfers_per_file = transfers_per_file
        self.round_no = 0
        self.accounts: list[dict] = []
        self.balances: dict[int, Decimal] = {}
        self.open_auths: dict[int, dict] = {}  # tx_id -> row still AUTHORISED
        self.deleted_tx_status: dict[int, str] = {}
        self.trips: dict[int, tuple[str, int]] = {}  # account_id -> (country, hours left)
        self._next_tx = 1
        self._next_transfer = 1

    # -- entities ---------------------------------------------------------------------------

    def initial_load(self) -> Batch:
        batch = Batch()
        account_id = 1
        for customer_id in range(1, self.n_customers + 1):
            batch.sql_ops.append(
                (
                    "customers",
                    "insert",
                    {
                        "customer_id": customer_id,
                        "full_name": self.fake.name(),
                        "nif": str(self.rng.randint(100_000_000, 299_999_999)),
                        "email": self.fake.email(),
                        "risk_segment": self.rng.choice(["LOW", "LOW", "LOW", "MEDIUM", "HIGH"]),
                        "created_at": self.clock,
                    },
                )
            )
            for _ in range(self.rng.choice([1, 1, 2])):
                nib = f"0033{self.rng.randint(0, 9999):04d}{account_id:011d}"
                balance = Decimal(self.rng.randint(200_00, 40_000_00)) / 100
                account = {
                    "account_id": account_id,
                    "customer_id": customer_id,
                    "iban": _pt_iban(nib),
                    "account_type": self.rng.choice(["CURRENT", "CURRENT", "SAVINGS"]),
                    "balance": balance,
                    "status": "ACTIVE",
                    "updated_at": self.clock,
                }
                self.accounts.append(account)
                self.balances[account_id] = balance
                batch.sql_ops.append(("accounts", "insert", dict(account)))
                account_id += 1
        return batch

    # -- rounds -----------------------------------------------------------------------------

    def next_round(self) -> Batch:
        """One simulated hour of activity."""
        self.round_no += 1
        batch = Batch()
        window_start = self.clock
        self._settle_and_expire(batch)
        self._update_trips()
        for _ in range(self.tx_per_round):
            self._card_tx(batch, self._pick_current(), self._ts_in_hour(window_start))
        if self.round_no == 1 or self.rng.random() < 0.6:
            self._inject_velocity(batch, window_start)
        if self.round_no == 2 or self.rng.random() < 0.6:
            self._inject_travel(batch, window_start)
        self._update_customer_emails(batch)
        self._partner_file(batch, window_start)
        self.clock = window_start + timedelta(hours=1)
        return batch

    def _update_trips(self) -> None:
        """Cardholders travel: for a few hours all their card use is in one foreign country.

        Modelling trips (instead of a random country per transaction) keeps legitimate
        country changes rare, as they are in real card data.
        """
        self.trips = {a: (c, n - 1) for a, (c, n) in self.trips.items() if n > 1}
        current = [a for a in self.accounts if a["account_type"] == "CURRENT"]
        for account in self.rng.sample(current, max(1, len(current) // 50)):
            self.trips.setdefault(
                account["account_id"], (self.rng.choice(FOREIGN), self.rng.randint(2, 6))
            )

    def _pick_current(self) -> dict:
        return self.rng.choice([a for a in self.accounts if a["account_type"] == "CURRENT"])

    def _ts_in_hour(self, window_start: datetime) -> datetime:
        return window_start + timedelta(seconds=self.rng.randint(0, 3599))

    def _card_tx(
        self,
        batch: Batch,
        account: dict,
        ts: datetime,
        country: str | None = None,
        amount: Decimal | None = None,
    ) -> dict:
        merchant, mcc = self.rng.choice(MERCHANTS)
        if country is None:
            country = self.trips.get(account["account_id"], ("PT", 0))[0]
        if amount is None:
            amount = Decimal(self.rng.randint(150, 25_000)) / 100
        row = {
            "tx_id": self._next_tx,
            "account_id": account["account_id"],
            "amount": amount,
            "currency": "EUR",
            "merchant": merchant,
            "mcc": mcc,
            "country": country,
            "status": "AUTHORISED",
            "tx_ts": ts,
        }
        self._next_tx += 1
        self.open_auths[row["tx_id"]] = row
        batch.sql_ops.append(("card_transactions", "insert", dict(row)))
        return row

    def _settle_and_expire(self, batch: Batch) -> None:
        """Previous-round authorisations settle, a few reverse, a few expire (deleted)."""
        for tx_id, row in list(self.open_auths.items()):
            roll = self.rng.random()
            if roll < 0.85:
                self._set_status(batch, row, "SETTLED")
                self._debit(batch, row["account_id"], row["amount"])
            elif roll < 0.92:
                self._set_status(batch, row, "REVERSED")
            elif roll < 0.97:
                # Expired authorisation: the cards platform hard-deletes it.
                batch.sql_ops.append(("card_transactions", "delete", {"tx_id": tx_id}))
                self.deleted_tx_status[tx_id] = row["status"]
            else:
                continue  # still pending
            del self.open_auths[tx_id]

    def _set_status(self, batch: Batch, row: dict, status: str) -> None:
        batch.sql_ops.append(
            ("card_transactions", "update", {"tx_id": row["tx_id"], "status": status})
        )

    def _debit(self, batch: Batch, account_id: int, amount: Decimal) -> None:
        self.balances[account_id] -= amount
        batch.sql_ops.append(
            (
                "accounts",
                "update",
                {
                    "account_id": account_id,
                    "balance": self.balances[account_id],
                    "updated_at": self.clock,
                },
            )
        )

    def _update_customer_emails(self, batch: Batch) -> None:
        for _ in range(3):
            batch.sql_ops.append(
                (
                    "customers",
                    "update",
                    {
                        "customer_id": self.rng.randint(1, self.n_customers),
                        "email": self.fake.email(),
                    },
                )
            )

    # -- planted fraud ----------------------------------------------------------------------

    def _inject_velocity(self, batch: Batch, window_start: datetime) -> None:
        account = self._pick_current()
        first = window_start + timedelta(minutes=self.rng.randint(0, 45))
        rows = [
            self._card_tx(
                batch,
                account,
                first + timedelta(seconds=self.rng.randint(0, 540)),
                country="PT",
                amount=Decimal(self.rng.randint(5_000, 20_000)) / 100,
            )
            for _ in range(self.rng.randint(5, 8))
        ]
        batch.ground_truth.append(
            {
                "rule": "card_velocity",
                "account_id": account["account_id"],
                "tx_ids": [r["tx_id"] for r in rows],
            }
        )

    def _inject_travel(self, batch: Batch, window_start: datetime) -> None:
        account = self._pick_current()
        first_ts = window_start + timedelta(minutes=self.rng.randint(0, 20))
        home = self._card_tx(batch, account, first_ts, country="PT")
        away = self._card_tx(
            batch,
            account,
            first_ts + timedelta(minutes=self.rng.randint(5, 40)),
            country=self.rng.choice(FOREIGN),
        )
        batch.ground_truth.append(
            {
                "rule": "impossible_travel",
                "account_id": account["account_id"],
                "tx_ids": [home["tx_id"], away["tx_id"]],
            }
        )

    def _partner_file(self, batch: Batch, window_start: datetime) -> None:
        rows = []
        for _ in range(self.transfers_per_file):
            ours = self.rng.choice(self.accounts)["iban"]
            other = _pt_iban(f"0035{self.rng.randint(0, 10**15 - 1):015d}")
            debtor, creditor = (ours, other) if self.rng.random() < 0.5 else (other, ours)
            rows.append(
                self._transfer(
                    window_start,
                    debtor,
                    creditor,
                    Decimal(self.rng.randint(10_00, 2_500_00)) / 100,
                )
            )
        if self.round_no == 3 or self.rng.random() < 0.4:
            debtor = self.rng.choice(self.accounts)
            planted = [
                self._transfer(
                    window_start,
                    debtor["iban"],
                    _pt_iban(f"0018{self.rng.randint(0, 10**15 - 1):015d}"),
                    Decimal(
                        self.rng.randint(int(STRUCTURING_LOW * 100), int(STRUCTURING_HIGH * 100))
                    )
                    / 100,
                )
                for _ in range(self.rng.randint(3, 5))
            ]
            rows.extend(planted)
            batch.ground_truth.append(
                {
                    "rule": "structuring",
                    "account_id": debtor["account_id"],
                    "transfer_ids": [r["transfer_id"] for r in planted],
                }
            )
        if self.rng.random() < 0.3:
            # Partner sent a European-formatted amount: must land in the DLQ, not be dropped.
            bad = self._transfer(
                window_start,
                self.rng.choice(self.accounts)["iban"],
                rows[0]["creditor_iban"],
                Decimal("1"),
            )
            bad["amount"] = "1.250,00"
            rows.append(bad)
        self.rng.shuffle(rows)
        out = io.StringIO()
        writer = csv.DictWriter(out, fieldnames=FILE_COLUMNS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
        name = f"sepa_partner_{window_start:%Y%m%d_%H%M}.csv"
        batch.files.append((name, out.getvalue()))

    def _transfer(
        self, window_start: datetime, debtor: str, creditor: str, amount: Decimal
    ) -> dict:
        ts = self._ts_in_hour(window_start)
        row = {
            "transfer_id": f"TRF{self._next_transfer:08d}",
            "value_date": ts.date().isoformat(),
            "booking_ts": ts.isoformat(),
            "debtor_iban": debtor,
            "creditor_iban": creditor,
            "amount": f"{amount:.2f}",
            "currency": "EUR",
            "reference": self.fake.bothify("INV-####-??").upper(),
        }
        self._next_transfer += 1
        return row
